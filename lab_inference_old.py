#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
lab_inference.py — deploy the trained ACT policy on the REVO Scout AGV.

This is the NEW inference architecture (AI-only UDP port, frame bus reader,
dedicated 50 Hz tx thread, action TTL, action_bins path resolution,
--unscale-ang) built against the OLDER lerobot import layout so it drops
into the older robot's environment without a lerobot upgrade.

Lives at the repo root, alongside teleop_start.sh and the LAB/ package.

    aadi_scout_hw/
        lab_inference.py     ← this file
        LAB/
            teleop.py
            motion.py
            cameras.py
            utils/frame_bus.py
            ...

────────────────────────────────────────────────────────────────────────────
HOW THIS FITS INTO THE ROBOT
────────────────────────────────────────────────────────────────────────────

This process does NOT drive the robot directly. It is one of three command
sources that teleop arbitrates between, and it gets its OWN port so the
routing is structural rather than a string field teleop has to trust:

    local dongle  (LAB/local_gamepad.py)  → in-process, human
    browser/pilot (bridge or WS)          → udp :55999,  human only
    THIS FILE                             → udp :55998,  AI only

teleop's MotionController owns the arbitration and enforces HARD HUMAN
PRIORITY. Our packets are only honoured when all of the following hold:

    1. the operator has enabled AI
         · gamepad A button      → local ai_mode envelope
         · gamepad lift-trigger chord → ai_request="enable"
         · browser AI button     → WS {"AI": 1}
    2. the human handback window has closed (no meaningful stick input
       for motion's human_handback_sec, default 2 s)
    3. the robot is UNLOCKED and the human is not braking
    4. our packets are arriving (motion's AI watchdog, see below)

Any human brake hard-latches AI back off. We never send robot_lock and we
never send brake — those belong to the human exclusively.

Because the policy runs at ~15 Hz but motion's watchdog fires at
cfg.motion_watchdog_sec (0.5 s), the UDP sender runs on its OWN thread at
--send-hz (default 50 Hz) and repeats the most recent action between policy
ticks. That keeps the drivetrain fed even if a single inference step is slow,
and gives teleop a clean "the AI stream is alive" signal. If the policy stops
producing actions for --action-ttl seconds we send zeros instead of repeating
a stale command forever, and teleop's own AI watchdog then hands control back
to the human.

────────────────────────────────────────────────────────────────────────────
CAMERA
────────────────────────────────────────────────────────────────────────────

teleop is the sole owner of /dev/videoN. It republishes every frame into a
/dev/shm ring via LAB/utils/frame_bus.py, so we attach as a reader and never
touch V4L2 while teleop is up.

    primary   FrameBusReader(cfg.record_camera_name)      ← teleop running
    fallback  MultiCameraCapture.from_configs([cam_cfg])  ← teleop NOT running

The fallback exists only for standalone bench testing. It opens the V4L2
device directly and will fail if teleop already holds it — that failure is
expected and reported clearly rather than silently producing black frames.

────────────────────────────────────────────────────────────────────────────
GPS
────────────────────────────────────────────────────────────────────────────

teleop's GpsReader already binds udp 127.0.0.1:57002. UDP unicast will not
share a port (SO_REUSEADDR does not help here), so we listen on a SECOND
fan-out port published by LAB/utils/gps_mux.py:

    GPS_UDP_PORTS="57002,57003"   in the gps_mux environment

    57002 → teleop
    57003 → this file             (--gps-udp-port, default 57003)

If nothing ever arrives we log loudly rather than silently feeding the policy
lat=lon=orientation=0.0, which is a distribution it never saw in training.

────────────────────────────────────────────────────────────────────────────
ang_z CONVENTION
────────────────────────────────────────────────────────────────────────────

MotionController multiplies the ang_z it receives by cfg.ang_z_scale (0.20),
then adds cfg.ang_z_drift_correction, before publishing to the chassis:

    motion.command(ang_z_raw)  →  /cmd_vel gets ang_z_raw * 0.20 + drift

The drift term is a constant yaw bias cancelling the drivetrain's veer. It
lives only on the wire — it is never recorded and never fed back as an
observation — so nothing in this file has to account for it.

WHICH SPACE IS YOUR DATASET IN? CHECK BEFORE YOU DRIVE.
teleop.py wires SessionRecorder to motion.published_state_raw(), so recordings
made by the current build store RAW ang_z: no 0.20 multiply, no drift offset.
Recordings from the interim build wired to motion.published_state() stored
SCALED ang_z. Older recordings wired to motion.state() stored RAW.

Tell them apart by magnitude — the gamepad's yaw limit is 2.0-3.5 rad/s:

    |ang_z| peaks near 2-3.5   → RAW    → run without --unscale-ang
    |ang_z| peaks near 0.4-0.7 → SCALED → run WITH --unscale-ang

Get this wrong and the robot turns 5x too weakly (or 5x too hard).

    without --unscale-ang:  policy out = raw    → passed straight through
    with    --unscale-ang:  policy out = scaled → divided by 0.20 here, so
                            MotionController's multiply restores it

The observation echo always feeds back the value in the POLICY's own space,
not the wire value, so the loop stays inside the training distribution either
way. This mirrors what the dataset recorded: observation.state ang_z and the
action ang_z both come from the same source, so they share one space.

────────────────────────────────────────────────────────────────────────────
USAGE
────────────────────────────────────────────────────────────────────────────

Dry run — print predictions, robot never moves:

    python3 lab_inference.py \
        --policy-path ~/policies/act_scout_dataset_03/checkpoints/080000/pretrained_model \
        --dataset-repo-id Aadi/scout_dataset_03

Download the policy from HuggingFace instead:

    python3 lab_inference.py \
        --policy-id Aadi/act_scout_dataset_03 \
        --dataset-repo-id Aadi/scout_dataset_03

Live — teleop must already be running; the operator still has to press AI:

    python3 lab_inference.py \
        --policy-path ~/policies/.../pretrained_model \
        --dataset-repo-id Aadi/scout_dataset_03 \
        --send \
        --temporal-ensemble-coeff 0.01 \
        --ang-deadband 0.15

Stale absolute action_bins.pt path baked into the checkpoint config:

    ... --action-bins-path /path/to/agvdata-noyolo_bins_uniform_a01.pt
"""

from __future__ import annotations

import argparse
import json
import signal
import socket
import sys
import threading
import time
from dataclasses import replace as dataclass_replace
from pathlib import Path
from typing import Optional

import cv2
import numpy as np

# ── repo root on sys.path so `import LAB...` works from anywhere ────────────
_REPO = Path(__file__).resolve().parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from LAB.common import log                      # noqa: E402
from LAB.config import LabConfig                # noqa: E402
from LAB.sensors import GpsReader               # noqa: E402

# ── LeRobot (OLD import layout — matches the older robot's environment) ─────
from lerobot.configs.policies           import PreTrainedConfig           # noqa: E402
from lerobot.datasets.lerobot_dataset   import LeRobotDatasetMetadata     # noqa: E402
from lerobot.datasets.utils             import build_dataset_frame        # noqa: E402
from lerobot.policies.factory           import make_policy, make_pre_post_processors  # noqa: E402
from lerobot.policies.utils             import make_robot_action          # noqa: E402
from lerobot.processor                  import make_default_processors    # noqa: E402
from lerobot.processor.rename_processor import rename_stats               # noqa: E402
from lerobot.utils.constants            import ACTION, OBS_STR            # noqa: E402
from lerobot.utils.control_utils        import predict_action             # noqa: E402
from lerobot.utils.utils                import get_safe_torch_device, init_logging  # noqa: E402


# ── Constants ───────────────────────────────────────────────────────────────

# Must match data_convert_agv.py's CAMERA_KEY.
CAMERA_KEY = "front"

BLANK_FRAME_THRESHOLD = 5.0    # mean pixel below this → treat as blank
BLANK_FRAME_MAX_CONSEC = 30    # halt after this many consecutive bad frames

DEFAULT_GPS_UDP_PORT = 57003   # second gps_mux fan-out target
DEFAULT_AI_UDP_PORT = 55998    # teleop's AI-only motion listener
DEFAULT_SEND_HZ = 50.0         # UDP tx rate, independent of policy rate
DEFAULT_ACTION_TTL = 0.5       # stop repeating an action older than this
GPS_WARN_AFTER_SEC = 10.0      # complain if no GPS by then


# ════════════════════════════════════════════════════════════════════════════
#  AI motion sender — 50 Hz UDP to teleop, decoupled from policy latency
# ════════════════════════════════════════════════════════════════════════════

class AiMotionSender:
    """Streams the latest policy action to teleop's motion listener.

    Runs at a fixed rate on its own thread so a slow inference step can never
    starve teleop's drivetrain watchdog. The policy loop calls set_action();
    this thread repeats whatever it last saw.

    Fail-safe: if set_action() has not been called for `action_ttl` seconds
    the repeated command decays to zero. teleop's AI watchdog then observes
    a zero-velocity-but-alive stream; if we stop entirely it auto-disables AI
    and hands control back to the human.

    We deliberately send neither robot_lock nor a nonzero brake — both are
    human-authoritative in MotionController and an AI packet must never be
    able to unlock the robot or latch a brake.
    """

    def __init__(
        self,
        host: str,
        port: int,
        send_hz: float = DEFAULT_SEND_HZ,
        action_ttl: float = DEFAULT_ACTION_TTL,
        enabled: bool = True,
    ) -> None:
        self._host = host
        self._port = port
        self._period = 1.0 / max(1.0, float(send_hz))
        self._action_ttl = float(action_ttl)
        self._enabled = bool(enabled)

        self._lock = threading.Lock()
        self._lin = 0.0
        self._ang = 0.0
        self._action_t = 0.0          # monotonic, 0 = nothing set yet
        self._seq = 0
        self._stale_logged = False

        self._sock: Optional[socket.socket] = None
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ── lifecycle ───────────────────────────────────────────────────────────

    def start(self) -> None:
        if not self._enabled:
            log("inference", "dry run — no UDP will be sent (omit --send to keep this)")
            return
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        except Exception as exc:
            log("inference", f"UDP socket create failed: {exc} — AI output disabled")
            self._enabled = False
            return
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="ai-motion-tx"
        )
        self._thread.start()
        log("inference",
            f"AI motion → udp://{self._host}:{self._port} @ "
            f"{1.0 / self._period:.0f} Hz (origin=ai, ttl={self._action_ttl:.2f}s)")

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)
            self._thread = None
        # Explicit zero burst so the chassis is not left holding our last
        # command while teleop's watchdog counts down.
        if self._sock is not None:
            for _ in range(5):
                self._emit(0.0, 0.0)
                time.sleep(0.02)
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None

    # ── public API ──────────────────────────────────────────────────────────

    def set_action(self, lin_x: float, ang_z: float) -> None:
        with self._lock:
            self._lin = float(lin_x)
            self._ang = float(ang_z)
            self._action_t = time.monotonic()
            self._stale_logged = False

    @property
    def enabled(self) -> bool:
        return self._enabled

    # ── internals ───────────────────────────────────────────────────────────

    def _current_command(self) -> tuple[float, float]:
        now = time.monotonic()
        with self._lock:
            if self._action_t <= 0.0:
                return 0.0, 0.0
            age = now - self._action_t
            if age > self._action_ttl:
                if not self._stale_logged:
                    log("inference",
                        f"action stale ({age:.2f}s > ttl) — sending zeros")
                    self._stale_logged = True
                return 0.0, 0.0
            return self._lin, self._ang

    def _emit(self, lin: float, ang: float) -> None:
        if self._sock is None:
            return
        self._seq += 1
        payload = {
            "seq": self._seq,
            "t": time.time(),
            "origin": "ai",
            "lin_x": round(float(lin), 4),
            "ang_z": round(float(ang), 4),
            "brake": 0.0,
        }
        try:
            self._sock.sendto(
                json.dumps(payload, separators=(",", ":")).encode("utf-8"),
                (self._host, self._port),
            )
        except Exception as exc:
            log("inference", f"motion sendto error: {exc}")

    def _run(self) -> None:
        next_t = time.monotonic()
        while not self._stop.is_set():
            lin, ang = self._current_command()
            self._emit(lin, ang)
            next_t += self._period
            delay = next_t - time.monotonic()
            if delay < -self._period:
                next_t = time.monotonic()
                delay = 0.0
            if delay > 0:
                self._stop.wait(timeout=delay)


# ════════════════════════════════════════════════════════════════════════════
#  Frame source — shared-memory bus first, direct V4L2 only as a fallback
# ════════════════════════════════════════════════════════════════════════════

class FrameSource:
    """Reads BGR frames from teleop's frame bus, or straight from V4L2.

    teleop owns /dev/videoN exclusively, so the bus is the only correct path
    while it is running. The direct path exists for bench work with teleop
    stopped; if teleop holds the device it will fail to open and we say so
    instead of quietly serving nothing.

    The direct-V4L2 fallback uses the OLD MultiCameraCapture API so this file
    stays compatible with the older LAB.cameras module.
    """

    def __init__(
        self,
        cam_cfg,
        expected_hw: tuple,
        prefer_bus: bool = True,
        bus_timeout_sec: float = 3.0,
    ) -> None:
        self._cam_cfg = cam_cfg
        self._expected_hw = expected_hw
        self._bus = None
        self._cameras = None
        self._camera_name = cam_cfg.name
        self.label = "none"

        if prefer_bus:
            self._bus = self._attach_bus(cam_cfg.name, bus_timeout_sec)
            if self._bus is not None:
                self.label = f"frame bus (/dev/shm/lab_{cam_cfg.name})"
                return

        self._cameras = self._open_direct(cam_cfg)
        if self._cameras is not None:
            self.label = f"direct V4L2 ({cam_cfg.source})"
            return

        raise RuntimeError(
            f"no frame source for camera {cam_cfg.name!r}. The frame bus is "
            f"empty (is teleop running, and is publish_frames=True for this "
            f"camera?) and {cam_cfg.source} could not be opened directly "
            f"(teleop is probably holding it)."
        )

    # ── construction helpers ────────────────────────────────────────────────

    def _attach_bus(self, camera_name: str, timeout_sec: float):
        try:
            from LAB.utils.frame_bus import FrameBusReader
        except Exception as exc:
            log("inference", f"frame_bus import failed: {exc}")
            return None

        log("inference", f"attaching to frame bus {camera_name!r}…")
        rdr = FrameBusReader(camera_name)

        deadline = time.monotonic() + timeout_sec
        frame = None
        while time.monotonic() < deadline:
            _ts, frame = rdr.read_latest()
            if frame is not None:
                break
            time.sleep(0.1)

        if frame is None:
            log("inference",
                f"frame bus: nothing published within {timeout_sec:.0f}s "
                f"— is teleop running?")
            rdr.close()
            return None

        exp_h, exp_w = self._expected_hw
        if frame.shape[:2] != (exp_h, exp_w):
            log("inference",
                f"frame bus shape {frame.shape} != expected "
                f"({exp_h}, {exp_w}, 3) — check record_width/record_height "
                f"against the camera config")
            rdr.close()
            return None

        log("inference",
            f"frame bus OK: shape={frame.shape} mean_px={float(frame.mean()):.0f}")
        return rdr

    def _open_direct(self, cam_cfg):
        try:
            from LAB.cameras import MultiCameraCapture
        except Exception as exc:
            log("inference", f"cameras import failed: {exc}")
            return None

        # Never publish from here — teleop owns the bus region for this name
        # and two publishers would fight over /dev/shm/lab_<name>.
        try:
            solo_cfg = dataclass_replace(cam_cfg, publish_frames=False)
        except Exception:
            # If CameraConfig isn't a dataclass in this build, fall through
            # with the original cfg; teleop being down means nothing else is
            # publishing anyway.
            solo_cfg = cam_cfg

        log("inference", f"falling back to direct V4L2 on {solo_cfg.source}")
        try:
            cameras = MultiCameraCapture.from_configs([solo_cfg])
        except Exception as exc:
            log("inference", f"MultiCameraCapture.from_configs failed: {exc}")
            return None

        if not cameras.has(solo_cfg.name):
            log("inference",
                f"cannot open camera {solo_cfg.name!r} at {solo_cfg.source} "
                f"— is teleop already holding the V4L2 device?")
            try:
                cameras.stop_all()
            except Exception:
                pass
            return None

        # Give the capture thread a moment to land its first frame.
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            _ts, frame = cameras.read(solo_cfg.name)
            if frame is not None:
                return cameras
            time.sleep(0.1)

        log("inference", "direct V4L2 opened but produced no frames")
        try:
            cameras.stop_all()
        except Exception:
            pass
        return None

    # ── public API ──────────────────────────────────────────────────────────

    def read(self) -> Optional[np.ndarray]:
        if self._bus is not None:
            _ts, frame = self._bus.read_latest()
            return frame
        if self._cameras is not None:
            _ts, frame = self._cameras.read(self._camera_name)
            return frame
        return None

    def close(self) -> None:
        if self._bus is not None:
            try:
                self._bus.close()
            except Exception:
                pass
            self._bus = None
        if self._cameras is not None:
            try:
                self._cameras.stop_all()
            except Exception:
                pass
            self._cameras = None


# ════════════════════════════════════════════════════════════════════════════
#  Frame validation
# ════════════════════════════════════════════════════════════════════════════

class FrameValidator:
    """Rejects None / wrong-shape / blank frames, halts on a sustained run.

    A blank frame usually means the USB camera wedged or the bus writer died.
    Feeding those to the policy produces confident nonsense, so a long streak
    is a hard stop rather than a warning.
    """

    def __init__(
        self,
        expected_h: int,
        expected_w: int,
        blank_thresh: float = BLANK_FRAME_THRESHOLD,
        max_consec: int = BLANK_FRAME_MAX_CONSEC,
    ) -> None:
        self.expected_shape = (expected_h, expected_w, 3)
        self.blank_thresh = blank_thresh
        self.max_consec = max_consec
        self.n_total = 0
        self.n_none = 0
        self.n_wrong_shape = 0
        self.n_blank = 0
        self.n_ok = 0
        self._consec_bad = 0

    def validate(self, frame: Optional[np.ndarray]) -> tuple:
        self.n_total += 1

        if frame is None:
            self.n_none += 1
            self._consec_bad += 1
            self._check_halt()
            return False, "frame is None"

        if frame.shape != self.expected_shape:
            self.n_wrong_shape += 1
            self._consec_bad += 1
            self._check_halt()
            return False, f"wrong shape {frame.shape}, expected {self.expected_shape}"

        mean_px = float(frame.mean())
        if mean_px < self.blank_thresh:
            self.n_blank += 1
            self._consec_bad += 1
            self._check_halt()
            return False, f"blank frame mean_px={mean_px:.1f} < {self.blank_thresh}"

        self.n_ok += 1
        self._consec_bad = 0
        return True, ""

    def summary(self) -> str:
        return (
            f"total={self.n_total} ok={self.n_ok} none={self.n_none} "
            f"wrong_shape={self.n_wrong_shape} blank={self.n_blank}"
        )

    def _check_halt(self) -> None:
        if self._consec_bad >= self.max_consec:
            raise RuntimeError(
                f"{self._consec_bad} consecutive bad frames — halting. "
                f"{self.summary()}"
            )


# ════════════════════════════════════════════════════════════════════════════
#  Policy pipeline
# ════════════════════════════════════════════════════════════════════════════

def _resolve_action_bins_path(
    policy_cfg,
    policy_path: str,
    override: Optional[str] = None,
) -> None:
    """Repair a stale absolute action_bins_path baked in at training time.

    Resolution order:
      1. --action-bins-path override
      2. <policy_path>/<basename of the baked path>
      3. <policy_path>/action_bins.pt
      4. the baked path itself, if it happens to exist here
    """
    if not hasattr(policy_cfg, "action_bins_path"):
        return  # policy does not use discretization

    baked = getattr(policy_cfg, "action_bins_path", None)
    resolved: Optional[Path] = None

    if override:
        resolved = Path(override).expanduser()
    else:
        candidates = []
        if baked:
            candidates.append(Path(policy_path) / Path(str(baked)).name)
        candidates.append(Path(policy_path) / "action_bins.pt")
        if baked:
            candidates.append(Path(str(baked)))
        for c in candidates:
            try:
                if c.exists():
                    resolved = c
                    break
            except OSError:
                continue

    if resolved is None or not resolved.exists():
        raise FileNotFoundError(
            f"action_bins_path in the policy config points at {baked!r}, which "
            f"does not exist on this machine, and nothing usable was found in "
            f"{policy_path}. Pass --action-bins-path pointing at the .pt file "
            f"(normally shipped alongside the checkpoint)."
        )

    log("inference", f"action_bins_path: {baked}  →  {resolved}")
    policy_cfg.action_bins_path = str(resolved)


def build_policy_pipeline(
    policy_path: str,
    dataset_repo_id: str,
    device: str = "cuda",
    rename_map: Optional[dict] = None,
    action_bins_path: Optional[str] = None,
):
    """Build policy + pre/post processors aligned with data_convert_agv.py.

    Uses the OLDER lerobot factory signature: make_policy(policy_cfg, ds_meta)
    without a rename_map kwarg. rename_map is still applied through the
    preprocessor overrides and rename_stats(), so an empty map is a no-op and
    a populated one still takes effect at inference time.
    """
    if rename_map is None:
        rename_map = {}

    ds_meta = LeRobotDatasetMetadata(dataset_repo_id)
    _, robot_action_processor, robot_observation_processor = make_default_processors()

    policy_cfg = PreTrainedConfig.from_pretrained(policy_path)
    policy_cfg.device = device
    # Older PreTrainedConfig expects a plain string here.
    policy_cfg.pretrained_path = policy_path

    _resolve_action_bins_path(policy_cfg, policy_path, override=action_bins_path)

    policy = make_policy(policy_cfg, ds_meta=ds_meta)

    preprocessor, postprocessor = make_pre_post_processors(
        policy_cfg=policy_cfg,
        pretrained_path=policy_path,
        dataset_stats=rename_stats(ds_meta.stats, rename_map),
        preprocessor_overrides={
            # Older API: pass the requested device string directly rather
            # than reading it back off policy_cfg.
            "device_processor": {"device": device},
            "rename_observations_processor": {"rename_map": rename_map},
        },
    )

    image_key = f"{OBS_STR}.images.{CAMERA_KEY}"
    if image_key not in ds_meta.features:
        available = [k for k in ds_meta.features if "image" in k.lower()]
        raise RuntimeError(
            f"Camera key {image_key!r} not found in dataset features. "
            f"Available image keys: {available}. Update CAMERA_KEY at the top "
            f"of this file."
        )

    return (
        policy,
        preprocessor,
        postprocessor,
        robot_action_processor,
        robot_observation_processor,
        ds_meta.features,
    )


def build_raw_observation(
    frame_bgr: np.ndarray,
    lin_x: float,
    ang_z: float,
    gps_data: dict,
) -> dict:
    """Raw observation dict — same structure as data_convert_agv.py.

    ang_z must be in the same space the dataset stored (RAW by default; see
    the ang_z convention note at the top of this file). GPS fields fall back
    to 0.0 exactly as the offline converter does when there is no fix.
    """
    frame_rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
    lat = float(gps_data.get("gps_latitude", 0.0) or 0.0)
    lon = float(gps_data.get("gps_longitude", 0.0) or 0.0)
    ori = float(
        gps_data.get("heading_deg_true", gps_data.get("orientation", 0.0)) or 0.0
    )

    return {
        "lin_x": lin_x,
        "ang_z": ang_z,
        "lat": lat,
        "long": lon,
        "orientation": ori,
        CAMERA_KEY: frame_rgb,
    }


# ════════════════════════════════════════════════════════════════════════════
#  LabInference
# ════════════════════════════════════════════════════════════════════════════

class LabInference:

    def __init__(
        self,
        policy_path: str,
        dataset_repo_id: str,
        device: str,
        cfg: LabConfig,
        *,
        send: bool = False,
        fps: Optional[float] = None,
        send_hz: float = DEFAULT_SEND_HZ,
        action_ttl: float = DEFAULT_ACTION_TTL,
        udp_host: str = "127.0.0.1",
        udp_port: Optional[int] = None,
        gps_udp_port: int = DEFAULT_GPS_UDP_PORT,
        ang_deadband: float = 0.0,
        unscale_ang: bool = False,
        temporal_ensemble_coeff: Optional[float] = None,
        duration_s: Optional[float] = None,
        action_bins_path: Optional[str] = None,
        no_gps: bool = False,
    ) -> None:
        self._cfg = cfg
        self._send = send
        self._ang_deadband = ang_deadband
        self._unscale_ang = unscale_ang
        self._ang_z_scale = float(getattr(cfg, "ang_z_scale", 0.20) or 0.20)
        self._duration_s = duration_s
        self._fps = float(fps or getattr(cfg, "record_fps", 15) or 15)
        self._stop = threading.Event()

        # ── 1. Policy ───────────────────────────────────────────────────────
        log("inference", f"loading policy: {policy_path}")
        (
            self._policy,
            self._preprocessor,
            self._postprocessor,
            _robot_action_processor,   # unused — we have no Robot object
            self._robot_obs_processor,
            self._features,
        ) = build_policy_pipeline(
            policy_path=policy_path,
            dataset_repo_id=dataset_repo_id,
            device=device,
            action_bins_path=action_bins_path,
        )

        if temporal_ensemble_coeff is not None:
            self._enable_temporal_ensembling(temporal_ensemble_coeff)

        # ── 2. Frame validation ─────────────────────────────────────────────
        self._validator = FrameValidator(
            expected_h=cfg.record_height,
            expected_w=cfg.record_width,
        )

        # ── 3. Camera ───────────────────────────────────────────────────────
        cam_cfg = next(
            (c for c in cfg.cameras if c.name == cfg.record_camera_name), None
        )
        if cam_cfg is None:
            names = [c.name for c in cfg.cameras]
            raise RuntimeError(
                f"camera {cfg.record_camera_name!r} not in cfg.cameras "
                f"(have: {names})"
            )
        self._frames = FrameSource(
            cam_cfg,
            expected_hw=(cfg.record_height, cfg.record_width),
        )
        log("inference", f"camera source: {self._frames.label}")

        # ── 4. GPS ──────────────────────────────────────────────────────────
        self._gps: Optional[GpsReader] = None
        self._gps_warned = False
        self._gps_start_t = 0.0
        if no_gps:
            log("inference", "GPS disabled (--no-gps) — lat/lon/orientation = 0.0")
        else:
            teleop_gps_port = getattr(cfg, "gps_udp_port", 57002)
            if gps_udp_port == teleop_gps_port:
                log("inference",
                    f"WARNING: --gps-udp-port {gps_udp_port} is the same port "
                    f"teleop binds. The bind will fail while teleop runs. Add a "
                    f"second fan-out target to gps_mux "
                    f"(GPS_UDP_PORTS=\"{teleop_gps_port},{DEFAULT_GPS_UDP_PORT}\") "
                    f"and use --gps-udp-port {DEFAULT_GPS_UDP_PORT}.")
            self._gps = GpsReader(
                udp_host=cfg.gps_udp_host,
                udp_port=gps_udp_port,
            )
            self._gps.start()
            self._gps_start_t = time.monotonic()

        # ── 5. AI motion output ─────────────────────────────────────────────
        self._tx = AiMotionSender(
            host=udp_host,
            port=int(udp_port
                     or getattr(cfg, "udp_ai_motion_port", DEFAULT_AI_UDP_PORT)),
            send_hz=send_hz,
            action_ttl=action_ttl,
            enabled=send,
        )
        self._tx.start()

        # Self-consistent observation echo. During teleoperation the dataset
        # recorded the value the operator had just commanded; with no gamepad
        # present we feed back what WE last commanded, which keeps the loop in
        # the same distribution:
        #     tick N   : command(pred_ang)
        #     tick N+1 : obs ang_z = pred_ang
        self._last_cmd_lin = 0.0
        self._last_cmd_ang = 0.0

        log("inference", "init complete")

    # ── public ──────────────────────────────────────────────────────────────

    def run(self) -> None:
        mode = "SENDING (origin=ai)" if self._send else "DRY RUN — no UDP"
        log("inference",
            f"starting — {mode}  fps={self._fps:.0f}  "
            f"deadband={self._ang_deadband}  unscale_ang={self._unscale_ang}")

        self._policy.reset()
        self._preprocessor.reset()
        self._postprocessor.reset()

        interval = 1.0 / max(1.0, self._fps)
        next_tick = time.monotonic()
        t_start_mono = time.monotonic()
        frame_i = 0

        print()
        print(f"{'frame':>6}  {'wall_ts':>13}  {'mean_px':>8}  "
              f"{'obs_lin':>9}  {'obs_ang':>10}  "
              f"{'pred_lin':>9}  {'pred_ang':>11}  {'ang_cmd':>10}  "
              f"{'gps':>4}  {'tx':>4}")
        print("─" * 104)

        try:
            while not self._stop.is_set():
                if (self._duration_s is not None
                        and (time.monotonic() - t_start_mono) >= self._duration_s):
                    log("inference", f"duration {self._duration_s}s reached — stopping")
                    break

                sleep_for = next_tick - time.monotonic()
                if sleep_for > 0:
                    time.sleep(sleep_for)
                next_tick += interval
                # Never chase a backlog after a slow inference step.
                if next_tick < time.monotonic() - interval:
                    next_tick = time.monotonic() + interval

                wall_ts = time.time()

                # ── A. frame ────────────────────────────────────────────────
                frame_bgr = self._frames.read()
                ok, reason = self._validator.validate(frame_bgr)
                if not ok:
                    # A bad frame must not leave a stale action being repeated.
                    self._tx.set_action(0.0, 0.0)
                    self._last_cmd_lin = 0.0
                    self._last_cmd_ang = 0.0
                    log("inference", f"SKIP f={frame_i}: {reason}")
                    frame_i += 1
                    continue

                mean_px = float(frame_bgr.mean())

                # ── B. observation state ────────────────────────────────────
                lin_x_obs = self._last_cmd_lin
                ang_z_obs = self._last_cmd_ang

                gps_data = self._gps.get() if self._gps is not None else {}
                self._check_gps(gps_data)
                has_fix = bool(gps_data.get("gps_latitude") is not None)

                raw_obs = build_raw_observation(
                    frame_bgr=frame_bgr,
                    lin_x=lin_x_obs,
                    ang_z=ang_z_obs,
                    gps_data=gps_data,
                )
                obs_processed = self._robot_obs_processor(raw_obs)
                observation_frame = build_dataset_frame(
                    self._features, obs_processed, prefix=OBS_STR
                )

                # ── C. policy ───────────────────────────────────────────────
                action_values = predict_action(
                    observation=observation_frame,
                    policy=self._policy,
                    device=get_safe_torch_device(self._policy.config.device),
                    preprocessor=self._preprocessor,
                    postprocessor=self._postprocessor,
                    use_amp=self._policy.config.use_amp,
                    task=None,
                    robot_type="revobots_agv_follower",
                )
                act_pred = make_robot_action(action_values, self._features)

                pred_lin = float(act_pred.get("lin_x", 0.0))
                pred_ang = float(act_pred.get("ang_z", 0.0))

                # ── D. deadband ─────────────────────────────────────────────
                ang_after_db = pred_ang
                if self._ang_deadband > 0.0 and abs(pred_ang) < self._ang_deadband:
                    ang_after_db = 0.0

                # ── E. convention ───────────────────────────────────────────
                # Default: dataset stored RAW ang_z, MotionController applies
                # ang_z_scale, so pass it straight through. With --unscale-ang
                # the dataset stored the already-scaled value, so undo the
                # scale here to keep /cmd_vel at the intended rate.
                ang_cmd = (ang_after_db / self._ang_z_scale
                           if self._unscale_ang else ang_after_db)

                # ── F. publish ──────────────────────────────────────────────
                # The wire gets ang_cmd; the observation echo gets the value
                # in the POLICY's own space. Those differ under --unscale-ang,
                # and feeding the wire value back would hand the policy an
                # observation 1/ang_z_scale larger than anything in its
                # training set.
                self._tx.set_action(pred_lin, ang_cmd)
                self._last_cmd_lin = pred_lin
                self._last_cmd_ang = ang_after_db

                db_mark = "*" if ang_after_db != pred_ang else " "
                print(
                    f"{frame_i:>6d}  "
                    f"{wall_ts:>13.3f}  "
                    f"{mean_px:>8.1f}  "
                    f"{lin_x_obs:>+9.4f}  "
                    f"{ang_z_obs:>+10.5f}  "
                    f"{pred_lin:>+9.4f}  "
                    f"{pred_ang:>+10.5f}{db_mark}  "
                    f"{ang_cmd:>+10.5f}  "
                    f"{('fix' if has_fix else '---'):>4}  "
                    f"{('SEND' if self._send else '----'):>4}"
                )

                frame_i += 1

        except RuntimeError as exc:
            print()
            log("inference", f"HALT — {exc}")
        except KeyboardInterrupt:
            print()
        except Exception as exc:
            print()
            log("inference", f"loop error: {exc}")
            import traceback
            traceback.print_exc()
        finally:
            print("─" * 104)
            log("inference", f"frame stats: {self._validator.summary()}")
            self._shutdown()

    def stop(self) -> None:
        self._stop.set()

    # ── internals ───────────────────────────────────────────────────────────

    def _check_gps(self, gps_data: dict) -> None:
        if self._gps is None or self._gps_warned or not self._gps_start_t:
            return
        if gps_data.get("gps_latitude") is not None:
            self._gps_warned = True   # got a fix, stop checking
            return
        if (time.monotonic() - self._gps_start_t) < GPS_WARN_AFTER_SEC:
            return
        self._gps_warned = True
        log("inference",
            f"WARNING: no GPS after {GPS_WARN_AFTER_SEC:.0f}s — the policy is "
            f"seeing lat=lon=orientation=0.0, which it never saw in training. "
            f"Check that gps_mux is fanning out to this port.")

    def _enable_temporal_ensembling(self, coeff: float) -> None:
        cfg_p = self._policy.config
        log("inference",
            f"temporal ensembling: coeff={coeff} n_action_steps=1 "
            f"chunk={cfg_p.chunk_size}")
        cfg_p.temporal_ensemble_coeff = coeff
        cfg_p.n_action_steps = 1
        try:
            from lerobot.policies.act.modeling_act import ACTTemporalEnsembler
            self._policy.temporal_ensembler = ACTTemporalEnsembler(
                coeff, cfg_p.chunk_size
            )
        except ImportError:
            log("inference", "WARNING: cannot import ACTTemporalEnsembler")
        self._policy.reset()

    def _shutdown(self) -> None:
        log("inference", "stopping…")
        # Zero the command before tearing the socket down.
        self._tx.set_action(0.0, 0.0)
        try:
            self._tx.stop()
        except Exception as exc:
            log("inference", f"tx stop error: {exc}")
        try:
            self._frames.close()
        except Exception as exc:
            log("inference", f"camera close error: {exc}")
        if self._gps is not None:
            try:
                self._gps.stop()
            except Exception:
                pass
        log("inference", "shutdown complete")


# ════════════════════════════════════════════════════════════════════════════
#  CLI
# ════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Deploy the trained ACT policy on the REVO Scout AGV.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--policy-path", default=None,
                     help="Local pretrained checkpoint directory.")
    src.add_argument("--policy-id", default=None,
                     help="HuggingFace repo id to download the policy from "
                          "(e.g. 'Aadi/act_scout_dataset_03').")
    ap.add_argument("--policy-revision", default=None,
                    help="Optional git revision/branch/tag for --policy-id.")
    ap.add_argument("--dataset-repo-id", required=True,
                    help="HF dataset repo-id (features + normalization stats).")
    ap.add_argument("--device", default="cuda")

    ap.add_argument("--send", action="store_true",
                    help="Stream commands to teleop as origin=ai. The operator "
                         "still has to enable AI on the gamepad or browser "
                         "before the robot will act on them.")
    ap.add_argument("--fps", type=float, default=None,
                    help="Policy tick rate (default: cfg.record_fps).")
    ap.add_argument("--send-hz", type=float, default=DEFAULT_SEND_HZ,
                    help=f"UDP transmit rate, independent of the policy rate "
                         f"(default: {DEFAULT_SEND_HZ:.0f}).")
    ap.add_argument("--action-ttl", type=float, default=DEFAULT_ACTION_TTL,
                    help=f"Stop repeating an action older than this and send "
                         f"zeros instead (default: {DEFAULT_ACTION_TTL}s).")
    ap.add_argument("--udp-host", default="127.0.0.1",
                    help="teleop motion listener host (default: 127.0.0.1).")
    ap.add_argument("--udp-port", type=int, default=None,
                    help=f"teleop's AI-only motion listener port (default: "
                         f"cfg.udp_ai_motion_port, else {DEFAULT_AI_UDP_PORT}). "
                         f"This is NOT the human port — do not point it at 55999.")

    ap.add_argument("--gps-udp-port", type=int, default=DEFAULT_GPS_UDP_PORT,
                    help=f"Second gps_mux fan-out port (default: "
                         f"{DEFAULT_GPS_UDP_PORT}). Must NOT be the port teleop "
                         f"binds.")
    ap.add_argument("--no-gps", action="store_true",
                    help="Skip GPS entirely and feed 0.0 for lat/lon/orientation.")

    ap.add_argument("--ang-deadband", type=float, default=0.0,
                    help="Zero ang_z below this magnitude, in the policy's own "
                         "output units. Recommended: 0.10–0.15.")
    ap.add_argument("--unscale-ang", action="store_true",
                    help="Divide the policy's ang_z by cfg.ang_z_scale before "
                         "sending. Use when the dataset stored ang_z from "
                         "motion.published_state() (already scaled) — that is "
                         "the interim build only; current recordings are RAW "
                         "and need no flag.")
    ap.add_argument("--temporal-ensemble-coeff", type=float, default=None,
                    help="Enable temporal ensembling (e.g. 0.01). Reduces "
                         "single-frame false turns.")
    ap.add_argument("--duration", type=float, default=None,
                    help="Stop after this many seconds (default: until Ctrl+C).")
    ap.add_argument("--action-bins-path", default=None,
                    help="Override for the policy config's action_bins .pt file.")
    return ap.parse_args()


def resolve_policy_path(args: argparse.Namespace) -> str:
    """Return a local checkpoint dir, downloading from HF if needed."""
    if args.policy_path:
        return args.policy_path
    from huggingface_hub import snapshot_download
    log("inference",
        f"downloading policy from HF: {args.policy_id}"
        + (f" @ {args.policy_revision}" if args.policy_revision else ""))
    local_dir = snapshot_download(
        repo_id=args.policy_id,
        revision=args.policy_revision,
    )
    log("inference", f"policy downloaded to: {local_dir}")
    return local_dir


def main() -> int:
    args = parse_args()
    init_logging()

    cfg = LabConfig.load_secrets()
    policy_path = resolve_policy_path(args)

    udp_port = args.udp_port or getattr(cfg, "udp_ai_motion_port",
                                        DEFAULT_AI_UDP_PORT)
    fps = args.fps or getattr(cfg, "record_fps", 15)

    if int(udp_port) == int(getattr(cfg, "udp_motion_port", 55999)):
        print(f"\n  REFUSING TO START: --udp-port {udp_port} is the HUMAN motion "
              f"port.\n  Sending AI commands there makes them indistinguishable "
              f"from operator\n  input and bypasses the AI enable gate entirely. "
              f"Use {DEFAULT_AI_UDP_PORT}.\n")
        return 2

    print()
    print("═" * 66)
    print("  LAB INFERENCE")
    print("═" * 66)
    print(f"  policy source   : {args.policy_id or args.policy_path}")
    print(f"  policy path     : {policy_path}")
    print(f"  dataset         : {args.dataset_repo_id}")
    print(f"  device          : {args.device}")
    print(f"  --send          : {args.send}  ← "
          f"{'streaming origin=ai' if args.send else 'dry run, no UDP'}")
    print(f"  motion target   : udp://{args.udp_host}:{udp_port}  (AI-only port)")
    print(f"  policy rate     : {fps} Hz     tx rate: {args.send_hz:.0f} Hz")
    print(f"  action ttl      : {args.action_ttl}s")
    print(f"  ang_deadband    : {args.ang_deadband}")
    print(f"  unscale_ang     : {args.unscale_ang}  (ang_z_scale={cfg.ang_z_scale})")
    print(f"  temporal_coeff  : {args.temporal_ensemble_coeff}")
    print(f"  duration        : {args.duration if args.duration else 'unlimited (Ctrl+C)'}")
    print(f"  action_bins     : {args.action_bins_path or '(auto — from checkpoint dir)'}")
    print(f"  camera          : {cfg.record_camera_name} "
          f"({cfg.record_height}x{cfg.record_width})")
    print(f"  GPS             : "
          f"{'disabled' if args.no_gps else f'{cfg.gps_udp_host}:{args.gps_udp_port}'}")
    print("─" * 66)
    print("  The robot only moves when teleop is running AND the operator has")
    print("  enabled AI AND the human handback window has closed. Any brake or")
    print("  stick input takes control straight back.")
    print("═" * 66)
    print()

    inf = LabInference(
        policy_path=policy_path,
        dataset_repo_id=args.dataset_repo_id,
        device=args.device,
        cfg=cfg,
        send=args.send,
        fps=args.fps,
        send_hz=args.send_hz,
        action_ttl=args.action_ttl,
        udp_host=args.udp_host,
        udp_port=args.udp_port,
        gps_udp_port=args.gps_udp_port,
        ang_deadband=args.ang_deadband,
        unscale_ang=args.unscale_ang,
        temporal_ensemble_coeff=args.temporal_ensemble_coeff,
        duration_s=args.duration,
        action_bins_path=args.action_bins_path,
        no_gps=args.no_gps,
    )

    def _on_signal(_sig, _frm):
        print("\n[inference] interrupt — stopping")
        inf.stop()

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    inf.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())