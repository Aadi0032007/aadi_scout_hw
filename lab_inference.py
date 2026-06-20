#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
lab_inference.py — Deploy the trained ACT policy on the REVO Scout AGV.

Place at repo root alongside teleop.py / agv_offline_eval.py.

Usage
-----
    # Dry-run — print predictions, no robot movement:
    python3 lab_inference.py \
        --policy-path ~/policies/act_scout_dataset_03/checkpoints/080000/pretrained_model \
        --dataset-repo-id Aadi/scout_dataset_03 \
        --device cuda

    # Live — actually drive the robot:
    python3 lab_inference.py \
        --policy-path ~/policies/act_scout_dataset_03/checkpoints/080000/pretrained_model \
        --dataset-repo-id Aadi/scout_dataset_03 \
        --device cuda \
        --send \
        --temporal-ensemble-coeff 0.01 \
        --ang-deadband 0.15
"""

from __future__ import annotations

import argparse
import json
import signal
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Optional

import cv2
import numpy as np

# ── repo root on sys.path ──────────────────────────────────────────────────
_REPO = Path(__file__).resolve().parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

# ── LAB ───────────────────────────────────────────────────────────────────
from LAB.common  import log
from LAB.config  import LabConfig
# NOTE: We no longer instantiate MotionController here. lab_inference now
# sends UDP packets into teleop's motion listener with origin="ai", and
# teleop's MotionController handles human/AI arbitration centrally.
from LAB.sensors import GpsReader

# ── LeRobot — identical imports to lerobot_inference_api.py ───────────────
from lerobot.configs.policies           import PreTrainedConfig
from lerobot.datasets.lerobot_dataset   import LeRobotDatasetMetadata
from lerobot.datasets.utils             import build_dataset_frame
from lerobot.policies.factory           import make_policy, make_pre_post_processors
from lerobot.policies.utils             import make_robot_action
from lerobot.processor                  import make_default_processors
from lerobot.processor.rename_processor import rename_stats
from lerobot.utils.constants            import ACTION, OBS_STR
from lerobot.utils.control_utils        import predict_action
from lerobot.utils.utils                import get_safe_torch_device, init_logging


# ── Constants ──────────────────────────────────────────────────────────────
# Camera key must match data_convert_agv.py: CAMERA_KEY = "front"
CAMERA_KEY             = "front"
BLANK_FRAME_THRESHOLD  = 5.0    # mean pixel value below this → blank frame
BLANK_FRAME_MAX_CONSEC = 30     # halt after this many consecutive bad frames


# ══════════════════════════════════════════════════════════════════════════════
#
#  END-TO-END PIPELINE EXPLANATION
#  ─────────────────────────────────────────────────────────────────────────
#
#  1. DATA RECORDING (teleop.py + record.py)
#  ─────────────────────────────────────────
#  Operator drives with a gamepad. UDP packet arrives with raw ang_z
#  (large values, ±3.5 rad/s range).
#
#  motion.command(lin_x, raw_ang_z, locked, brake)
#      ↓
#  MotionController stores raw_ang_z as self._ang_z
#  MotionController publishes to /cmd_vel: lin_x, raw_ang_z * ang_z_scale(0.20)
#      ↓
#  motion.published_state() → returns (lin_x, raw_ang_z * 0.20)   ← SCALED
#  motion.state()           → returns (lin_x, raw_ang_z, ...)      ← RAW
#
#  SessionRecorder is wired to motion.published_state (see teleop.py line 514).
#  It writes to JSONL:
#      linear_velocity  = lin_x
#      angular_velocity = raw_ang_z * 0.20     ← SCALED value stored on disk
#
#  Camera frame (BGR) is written to MP4 simultaneously.
#
#
#  2. DATASET CREATION (split_session_agv.py → data_convert_agv.py)
#  ─────────────────────────────────────────────────────────────────
#  split_session_agv.py: cuts long recordings into 2-minute chunks.
#  Each chunk becomes one episode folder: session_N/session_N.mp4 + .jsonl
#
#  data_convert_agv.py: for each episode folder:
#    - reads frame from MP4 → converts BGR→RGB
#    - reads row from JSONL:
#        lin_x = row["linear_velocity"]      ← the SCALED value (×0.20 already)
#        ang_z = row["angular_velocity"]     ← the SCALED value (×0.20 already)
#        lat, lon, orientation from GPS fields
#    - builds raw_observation dict:
#        { lin_x, ang_z, lat, long, orientation, "front": frame_rgb }
#    - applies robot_observation_processor (make_default_processors — currently identity)
#    - calls build_dataset_frame → packs into LeRobot dataset format
#    - action stored = { lin_x, ang_z } — same SCALED values
#
#  Result: dataset stores SCALED ang_z (already ×0.20) in both
#  observation.state and action vectors.
#
#
#  3. TRAINING
#  ──────────────────────────────────────────────────────────────────────────
#  ACT policy trained on LeRobot dataset. Input: image + state vector
#  [lin_x, ang_z_scaled, lat, lon, orientation]. Output: action chunk
#  [lin_x, ang_z_scaled] × chunk_size steps.
#
#  The policy learns SCALED ang_z throughout — it never sees raw values.
#  ang_z in the dataset is small (mean ~0.007, range ±0.70 approx = ±3.5×0.20).
#
#  With discretization (Method 1): action head outputs logits over 31 bins
#  per action dimension. Bin centers are in normalized (MEAN_STD) space.
#  At inference: softmax → expected value → un-normalize → SCALED ang_z.
#
#
#  4. INFERENCE (this file)
#  ──────────────────────────────────────────────────────────────────────────
#  No gamepad. Policy IS the sole source of commands.
#
#  Observation state feedback:
#    During teleoperation: motion.state() returned raw_ang_z from the gamepad.
#    During inference: there is no gamepad. We must feed back SCALED ang_z
#    to match what the dataset stored as observation.state ang_z.
#    Source: motion.published_state() → (lin_x, ang_z_scaled) — correct.
#
#  Policy output:
#    pred_ang_z is SCALED (×0.20 already applied by the dataset convention).
#    The robot expects SCALED ang_z on /cmd_vel.
#    motion.command() takes RAW ang_z and multiplies by 0.20 internally.
#    So: ang_z_cmd = pred_ang_z / ang_z_scale   (undo the scale so
#        motion.command(ang_z_cmd) * 0.20 = pred_ang_z on /cmd_vel)
#
#  In numbers: policy outputs 0.14 rad/s (scaled).
#    We pass 0.14 / 0.20 = 0.70 to motion.command().
#    MotionController sends 0.70 * 0.20 = 0.14 to /cmd_vel. ✓
#
#  Next observation feedback:
#    motion.published_state() → 0.14 (scaled) → fed back as ang_z obs. ✓
#    This matches what the dataset stored, so the policy sees the right dist.
#
# ══════════════════════════════════════════════════════════════════════════════


# ── Policy pipeline ────────────────────────────────────────────────────────

def build_policy_pipeline(
    policy_path:     str,
    dataset_repo_id: str,
    device:          str = "cuda",
    rename_map:      Optional[dict] = None,
):
    """
    Identical to lerobot_inference_api.build_policy_pipeline.
    Uses ds_meta.features directly for normalization-aligned feature schema.
    Uses make_default_processors() for [SYMMETRY] with data_convert_agv.py.
    """
    if rename_map is None:
        rename_map = {}

    ds_meta = LeRobotDatasetMetadata(dataset_repo_id)
    _, robot_action_processor, robot_observation_processor = make_default_processors()

    policy_cfg = PreTrainedConfig.from_pretrained(policy_path)
    policy_cfg.device          = device
    policy_cfg.pretrained_path = policy_path

    policy = make_policy(policy_cfg, ds_meta=ds_meta)

    preprocessor, postprocessor = make_pre_post_processors(
        policy_cfg      = policy_cfg,
        pretrained_path = policy_path,
        dataset_stats   = rename_stats(ds_meta.stats, rename_map),
        preprocessor_overrides = {
            "device_processor":              {"device": device},
            "rename_observations_processor": {"rename_map": rename_map},
        },
    )

    # Verify camera key exists in the dataset features at startup
    image_key = f"{OBS_STR}.images.{CAMERA_KEY}"
    if image_key not in ds_meta.features:
        available = [k for k in ds_meta.features if "image" in k.lower()]
        raise RuntimeError(
            f"Camera key '{image_key}' not found in dataset features. "
            f"Available image keys: {available}. "
            f"Update CAMERA_KEY at the top of this file."
        )

    return (
        policy,
        preprocessor,
        postprocessor,
        robot_action_processor,
        robot_observation_processor,
        ds_meta.features,
    )


# ── Frame validator ────────────────────────────────────────────────────────

class FrameValidator:
    """
    Three checks per frame:
      1. Not None
      2. Shape == (expected_h, expected_w, 3)
      3. Mean pixel > blank_thresh  (not black/blank)
    Raises RuntimeError after max_consec consecutive bad frames.
    """

    def __init__(
        self,
        expected_h:   int,
        expected_w:   int,
        blank_thresh: float = BLANK_FRAME_THRESHOLD,
        max_consec:   int   = BLANK_FRAME_MAX_CONSEC,
    ) -> None:
        self.expected_shape = (expected_h, expected_w, 3)
        self.blank_thresh   = blank_thresh
        self.max_consec     = max_consec
        self.n_total        = 0
        self.n_none         = 0
        self.n_wrong_shape  = 0
        self.n_blank        = 0
        self.n_ok           = 0
        self._consec_bad    = 0

    def validate(self, frame: Optional[np.ndarray]) -> tuple[bool, str]:
        self.n_total += 1

        if frame is None:
            self.n_none      += 1
            self._consec_bad += 1
            self._check_halt()
            return False, "frame is None"

        if frame.shape != self.expected_shape:
            self.n_wrong_shape += 1
            self._consec_bad   += 1
            self._check_halt()
            return False, f"wrong shape {frame.shape}, expected {self.expected_shape}"

        mean_px = float(frame.mean())
        if mean_px < self.blank_thresh:
            self.n_blank     += 1
            self._consec_bad += 1
            self._check_halt()
            return False, f"blank frame mean_px={mean_px:.1f} < {self.blank_thresh}"

        self.n_ok        += 1
        self._consec_bad  = 0
        return True, ""

    def summary(self) -> str:
        return (
            f"total={self.n_total} ok={self.n_ok} "
            f"none={self.n_none} wrong_shape={self.n_wrong_shape} blank={self.n_blank}"
        )

    def _check_halt(self) -> None:
        if self._consec_bad >= self.max_consec:
            raise RuntimeError(
                f"{self._consec_bad} consecutive bad frames — halting. "
                f"{self.summary()}"
            )


# ── Raw observation builder ────────────────────────────────────────────────

def build_raw_observation(
    frame_bgr: np.ndarray,
    lin_x:     float,
    ang_z:     float,
    gps_data:  dict,
) -> dict:
    """
    Builds the raw observation dict — identical structure to
    data_convert_agv.py / row_to_raw_obs_and_action().

    lin_x and ang_z must be RAW (motion.state() values) to match
    what the old dataset stored as observation state.
    GPS fields default to 0.0 if None (no fix yet — same as converter).
    Image is BGR→RGB (same as converter's cv2.cvtColor call).
    """
    frame_rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
    lat = float(gps_data.get("gps_latitude",  0.0) or 0.0)
    lon = float(gps_data.get("gps_longitude", 0.0) or 0.0)
    ori = float(gps_data.get("orientation",   0.0) or 0.0)

    return {
        "lin_x":       lin_x,
        "ang_z":       ang_z,      # RAW — matches old dataset observation.state
        "lat":         lat,
        "long":        lon,
        "orientation": ori,
        CAMERA_KEY:    frame_rgb,  # "front"
    }


# ── Main inference class ───────────────────────────────────────────────────

class LabInference:

    def __init__(
        self,
        policy_path:             str,
        dataset_repo_id:         str,
        device:                  str,
        cfg:                     LabConfig,
        send:                    bool  = False,
        ang_deadband:            float = 0.0,
        temporal_ensemble_coeff: Optional[float] = None,
        duration_s:              Optional[float] = None,
    ) -> None:
        self._cfg         = cfg
        self._ang_z_scale = cfg.ang_z_scale   # 0.20
        self._send        = send
        self._ang_deadband= ang_deadband
        self._stop        = threading.Event()
        self._duration_s  = duration_s

        # ── 1. Policy pipeline ────────────────────────────────────────────
        log("inference", f"loading policy: {policy_path}")
        (
            self._policy,
            self._preprocessor,
            self._postprocessor,
            _robot_action_processor,    # unused — no robot object
            self._robot_obs_processor,  # make_default_processors [SYMMETRY]
            self._features,             # ds_meta.features — normalization aligned
        ) = build_policy_pipeline(
            policy_path     = policy_path,
            dataset_repo_id = dataset_repo_id,
            device          = device,
        )

        # ── 2. Temporal ensembling ────────────────────────────────────────
        if temporal_ensemble_coeff is not None:
            self._enable_temporal_ensembling(temporal_ensemble_coeff)

        # ── 3. Frame validator ────────────────────────────────────────────
        self._validator = FrameValidator(
            expected_h = cfg.record_height,
            expected_w = cfg.record_width,
        )

        # ── 4. Motion UDP sender (replaces MotionController) ──────────────
        # Previously we owned a MotionController and forwarded directly to
        # the Docker bridge. Now we send UDP packets to teleop's motion
        # listener (cfg.udp_motion_port, default 55999) tagged origin="ai".
        # teleop's MotionController arbitrates human vs AI per its
        # human-priority policy.
        #
        # No rclpy. No direct Docker UDP. teleop must be running.
        self._motion_udp_host = "127.0.0.1"
        self._motion_udp_port = getattr(cfg, "udp_motion_port", 55999)
        self._motion_sock: Optional[socket.socket] = None
        if send:
            self._motion_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            log("inference",
                f"motion UDP → {self._motion_udp_host}:{self._motion_udp_port} "
                f"(origin=ai)  — teleop must be running")
            # Initial inert packet so teleop has a fresh AI slot (still
            # ignored until the human chord-enables AI).
            self._send_motion_ai(0.0, 0.0, locked=False, braking=False)

        # Self-consistent obs echo (replaces motion.state() in the obs loop).
        # We feed our last commanded values back as the policy's obs state
        # so the loop is coherent when no gamepad is present. This mirrors
        # what motion.state() used to return — the last value we commanded.
        self._last_sent_lin: float = 0.0
        self._last_sent_ang: float = 0.0

        # ── 5. GPS ────────────────────────────────────────────────────────
        self._gps = GpsReader(
            udp_host = cfg.gps_udp_host,
            udp_port = cfg.gps_udp_port,
        )
        self._gps.start()

        # ── 6. Camera ────────────────────────────────────────────────────
        self._frame_bus_reader = None
        self._cameras          = None

        cam_cfg = next(
            (c for c in cfg.cameras if c.name == cfg.record_camera_name), None
        )
        if cam_cfg is None:
            raise RuntimeError(
                f"Camera {cfg.record_camera_name!r} not in cfg.cameras"
            )

        if cam_cfg.publish_frames:
            self._frame_bus_reader = self._try_attach_frame_bus(cam_cfg.name)

        if self._frame_bus_reader is None:
            log("inference",
                f"camera {cam_cfg.name!r}: direct V4L2 ({cam_cfg.source})")
            from LAB.cameras import MultiCameraCapture
            self._cameras = MultiCameraCapture.from_configs([cam_cfg])
            if not self._cameras.has(cam_cfg.name):
                raise RuntimeError(
                    f"Cannot open camera {cam_cfg.name!r} at {cam_cfg.source}. "
                    f"Is teleop.py already holding the V4L2 device?"
                )

        self._camera_name = cam_cfg.name
        log("inference", "init complete")

    # ── Public ─────────────────────────────────────────────────────────────

    def run(self) -> None:
        mode = "SENDING TO ROBOT" if self._send else "DRY RUN (print only)"
        log("inference", f"starting — {mode}  deadband={self._ang_deadband}  scale={self._ang_z_scale}")

        if self._send:
            self._send_motion_ai(0.0, 0.0, locked=False, braking=False)

        self._policy.reset()
        self._preprocessor.reset()
        self._postprocessor.reset()

        interval  = 1.0 / self._cfg.stream_fps   # 1/15 s
        next_tick = time.monotonic()
        frame_i   = 0
        t_start      = time.time()       # wall-clock for timestamps
        t_start_mono = time.monotonic()  # for duration limit

        print()
        print(f"{'frame':>6}  {'timestamp':>12}  {'frame_mean':>10}  "
              f"{'obs_lin':>8}  {'obs_ang':>10}  "
              f"{'lin_x':>8}  {'ang_z_pred':>12}  {'ang_z_cmd':>12}  "
              f"{'→robot':>8}")
        print("─" * 105)

        try:
            while not self._stop.is_set():
                # Duration limit (--duration)
                if self._duration_s is not None and (time.monotonic() - t_start_mono) >= self._duration_s:
                    log("inference", f"duration {self._duration_s}s reached — stopping")
                    break

                sleep_for = next_tick - time.monotonic()
                if sleep_for > 0:
                    time.sleep(sleep_for)
                next_tick += interval

                wall_ts = time.time()

                # ── A. Grab and validate frame ─────────────────────────────
                frame_bgr = self._read_frame()
                ok, reason = self._validator.validate(frame_bgr)
                if not ok:
                    log("inference", f"SKIP f={frame_i}: {reason}")
                    frame_i += 1
                    continue

                mean_px = float(frame_bgr.mean())

                # ── B. Observation state — SCALED ang_z from published_state
                #
                # Dataset stores ang_z = raw_ang_z * 0.20 (published_state).
                # Policy was trained on that scaled value as obs state.
                # During inference there is no gamepad, so published_state()
                # reflects what we sent last tick — already in scaled space.
                # This is exactly what the policy expects as its state input.
                #
                # OLD dataset: ang_z was recorded from motion.state() = raw pre-scale.
                # Policy predicts raw ang_z. Feed raw back as obs to match training.
                # motion.state() returns whatever raw value we last passed to
                # motion.command() — so the loop is self-consistent:
                #   tick N:   motion.command(pred_ang_raw)
                #   tick N+1: motion.state() -> pred_ang_raw -> obs ang_z ✓
                if self._send:
                    lin_x_obs    = self._last_sent_lin
                    ang_z_raw_obs = self._last_sent_ang
                    ang_z_obs    = ang_z_raw_obs   # raw — matches old dataset
                else:
                    lin_x_obs, ang_z_obs = 0.0, 0.0

                gps_data = self._gps.get()

                # ── C. Build observation (identical to data_convert_agv.py) ─
                raw_obs = build_raw_observation(
                    frame_bgr = frame_bgr,
                    lin_x     = lin_x_obs,
                    ang_z     = ang_z_obs,   # RAW — matches old dataset
                    gps_data  = gps_data,
                )
                obs_processed     = self._robot_obs_processor(raw_obs)
                observation_frame = build_dataset_frame(
                    self._features, obs_processed, prefix=OBS_STR
                )

                # ── D. Policy inference ────────────────────────────────────
                action_values = predict_action(
                    observation   = observation_frame,
                    policy        = self._policy,
                    device        = get_safe_torch_device(self._policy.config.device),
                    preprocessor  = self._preprocessor,
                    postprocessor = self._postprocessor,
                    use_amp       = self._policy.config.use_amp,
                    task          = None,
                    robot_type    = "revobots_agv_follower",
                )
                act_pred = make_robot_action(action_values, self._features)

                # OLD dataset: policy output is RAW ang_z (not scaled).
                pred_lin = float(act_pred.get("lin_x", 0.0))
                pred_ang = float(act_pred.get("ang_z", 0.0))   # RAW

                # ── E. Deadband ────────────────────────────────────────────
                pred_ang_after_db = pred_ang
                if self._ang_deadband > 0.0 and abs(pred_ang) < self._ang_deadband:
                    pred_ang_after_db = 0.0

                # ── F. Compute command value for motion.command() ──────────
                #
                # Policy outputs SCALED ang_z (e.g. 0.14 rad/s).
                # motion.command() takes RAW ang_z and multiplies by 0.20.
                # We must un-scale so the robot receives the intended value:
                #
                # OLD dataset: pred_ang is already RAW.
                # motion.command(raw) * 0.20 → /cmd_vel receives raw*0.20 ✓
                # No division needed — pass raw directly.
                ang_z_cmd = pred_ang_after_db

                # ── G. Print what is being sent ────────────────────────────
                sent_marker = "SEND" if self._send else "----"
                db_marker   = " DB" if pred_ang_after_db != pred_ang else "   "
                print(
                    f"{frame_i:>6d}  "
                    f"{wall_ts:>12.3f}  "
                    f"{mean_px:>10.1f}  "
                    f"{lin_x_obs:>+8.4f}  "
                    f"{ang_z_obs:>+10.5f}  "
                    f"{pred_lin:>+8.4f}  "
                    f"{pred_ang:>+12.5f}{db_marker}  "
                    f"{ang_z_cmd:>+12.5f}  "
                    f"{sent_marker:>8}"
                )

                # ── H. Send to robot ───────────────────────────────────────
                if self._send:
                    self._send_motion_ai(
                        lin_x   = pred_lin,
                        ang_z   = ang_z_cmd,   # raw pred_ang; teleop's MotionController applies *0.20
                        locked  = False,
                        braking = False,
                    )

                frame_i += 1

        except RuntimeError as exc:
            print()
            log("inference", f"HALT — {exc}")
        except Exception as exc:
            print()
            log("inference", f"loop error: {exc}")
            import traceback
            traceback.print_exc()
        finally:
            print("─" * 105)
            log("inference", f"frame stats: {self._validator.summary()}")
            self._safe_stop()

    def stop(self) -> None:
        self._stop.set()

    # ── Internal ───────────────────────────────────────────────────────────

    def _enable_temporal_ensembling(self, coeff: float) -> None:
        cfg_p = self._policy.config
        log("inference",
            f"temporal ensembling: coeff={coeff} n_action_steps=1 chunk={cfg_p.chunk_size}")
        cfg_p.temporal_ensemble_coeff = coeff
        cfg_p.n_action_steps = 1
        try:
            from lerobot.policies.act.modeling_act import ACTTemporalEnsembler
            self._policy.temporal_ensembler = ACTTemporalEnsembler(coeff, cfg_p.chunk_size)
        except ImportError:
            log("inference", "WARNING: cannot import ACTTemporalEnsembler")
        self._policy.reset()

    def _try_attach_frame_bus(self, camera_name: str) -> Optional[object]:
        """Attach to SHM frame bus. Returns reader if first frame is valid, else None."""
        try:
            from LAB.utils.frame_bus import FrameBusReader
            log("inference", f"trying frame bus for {camera_name!r}...")
            rdr = FrameBusReader(camera_name)

            deadline = time.monotonic() + 2.0
            frame = None
            while time.monotonic() < deadline:
                _, frame = rdr.read_latest()
                if frame is not None:
                    break
                time.sleep(0.1)

            if frame is None:
                log("inference",
                    f"frame bus: no frame after 2s — is teleop.py running? "
                    f"Falling back to direct V4L2.")
                rdr.close()
                return None

            # Validate the first frame before committing
            tmp = FrameValidator(
                expected_h   = self._cfg.record_height,
                expected_w   = self._cfg.record_width,
                blank_thresh = BLANK_FRAME_THRESHOLD,
                max_consec   = 1,
            )
            ok, reason = tmp.validate(frame)
            if not ok:
                log("inference",
                    f"frame bus first frame invalid ({reason}) — "
                    f"falling back to direct V4L2.")
                rdr.close()
                return None

            log("inference",
                f"frame bus OK: shape={frame.shape}  mean_px={frame.mean():.0f}")
            return rdr

        except Exception as exc:
            log("inference", f"frame bus attach failed ({exc}) — using direct V4L2")
            return None

    def _read_frame(self) -> Optional[np.ndarray]:
        if self._frame_bus_reader is not None:
            _, frame = self._frame_bus_reader.read_latest()
            return frame
        if self._cameras is not None:
            _, frame = self._cameras.read(self._camera_name)
            return frame
        return None

    def _send_motion_ai(self, lin_x: float, ang_z: float,
                        locked: bool = False, braking: bool = False) -> None:
        """Send one AI motion packet to teleop's UDP motion listener.

        Tagged origin="ai" so teleop's MotionController routes it through
        the human/AI arbiter (which may ignore it if AI isn't enabled or
        if the human is currently in control).

        Also stores the values locally so the next obs read can echo them
        — keeps the inference loop self-consistent without needing to
        round-trip through motion.state().
        """
        if self._motion_sock is None:
            # Dry-run: keep self-consistent obs only.
            self._last_sent_lin = float(lin_x)
            self._last_sent_ang = float(ang_z)
            return
        try:
            payload = json.dumps({
                "lin_x":      float(lin_x),
                "ang_z":      float(ang_z),
                "robot_lock": bool(locked),
                "brake":      1.0 if braking else 0.0,
                "origin":     "ai",
            }).encode("utf-8")
            self._motion_sock.sendto(
                payload, (self._motion_udp_host, self._motion_udp_port)
            )
            self._last_sent_lin = float(lin_x)
            self._last_sent_ang = float(ang_z)
        except Exception as exc:
            log("inference", f"motion sendto error: {exc}")

    def _safe_stop(self) -> None:
        log("inference", "stopping — zeroing and closing UDP socket")
        if self._send:
            for _ in range(3):
                self._send_motion_ai(0.0, 0.0, locked=False, braking=False)
                time.sleep(0.05)
        if self._motion_sock is not None:
            try: self._motion_sock.close()
            except Exception: pass
            self._motion_sock = None

        if self._cameras is not None:
            try: self._cameras.stop_all()
            except Exception: pass

        if self._frame_bus_reader is not None:
            try: self._frame_bus_reader.close()
            except Exception: pass

        try: self._gps.stop()
        except Exception: pass

        log("inference", "shutdown complete")


# ── CLI ────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--policy-path",     required=True,
                    help="Pretrained checkpoint directory.")
    ap.add_argument("--dataset-repo-id", required=True,
                    help="HF dataset repo-id (features + normalization stats).")
    ap.add_argument("--device",          default="cuda")
    ap.add_argument("--send",            action="store_true",
                    help="Send commands to the robot. Without this flag the "
                         "script prints predictions only — no robot movement.")
    ap.add_argument("--ang-deadband",    type=float, default=0.0,
                    help="Zero ang_z below this magnitude (in scaled space, "
                         "same units as policy output). Recommended: 0.10–0.15.")
    ap.add_argument("--duration", type=float, default=None,
                    help="Stop after this many seconds. Default: run until Ctrl+C.")
    ap.add_argument("--temporal-ensemble-coeff", type=float, default=None,
                    help="Enable temporal ensembling (e.g. 0.01). "
                         "Reduces single-frame false turns.")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    init_logging()

    cfg = LabConfig.load_secrets()

    print()
    print("═" * 60)
    print("  LAB INFERENCE")
    print("═" * 60)
    print(f"  policy          : {args.policy_path}")
    print(f"  dataset         : {args.dataset_repo_id}")
    print(f"  device          : {args.device}")
    print(f"  --send          : {args.send}  ← {'ROBOT WILL MOVE' if args.send else 'dry run, no movement'}")
    print(f"  ang_deadband    : {args.ang_deadband}")
    print(f"  temporal_coeff  : {args.temporal_ensemble_coeff}")
    print(f"  duration        : {args.duration if args.duration else 'unlimited (Ctrl+C to stop)'}")
    print(f"  ang_z_scale     : {cfg.ang_z_scale}  (internal to MotionController)")
    print(f"  camera          : {cfg.record_camera_name}")
    print(f"  frame shape     : ({cfg.record_height}, {cfg.record_width}, 3)")
    print(f"  GPS UDP         : {cfg.gps_udp_host}:{cfg.gps_udp_port}")
    print()
    print("  Column guide:")
    print("  frame  timestamp  frame_mean  obs_lin  obs_ang  lin_x  ang_z_pred(DB)  ang_z_cmd  →robot")
    print("  ─────────────────────────────────────────────────────────────────────────────")
    print("  ang_z_pred = policy output (RAW — old dataset)")
    print("  DB         = deadbanded to zero")
    print("  ang_z_cmd  = ang_z_pred (passed directly to motion.command())")
    print("             motion.command() × 0.20 → /cmd_vel receives ang_z_pred × 0.20 ✓")
    print("═" * 60)
    print()

    inf = LabInference(
        policy_path             = args.policy_path,
        dataset_repo_id         = args.dataset_repo_id,
        device                  = args.device,
        cfg                     = cfg,
        send                    = args.send,
        ang_deadband            = args.ang_deadband,
        temporal_ensemble_coeff = args.temporal_ensemble_coeff,
        duration_s              = args.duration,
    )

    def _on_signal(sig, frame):
        print("\n[inference] interrupt — stopping")
        inf.stop()

    signal.signal(signal.SIGINT,  _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    inf.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())