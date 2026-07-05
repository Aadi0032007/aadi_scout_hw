# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations

"""
teleop.py — REDESIGN.

Major structural changes vs previous version:

    - stream.py deleted. No Daily WebRTC, no video compositor.
    - Streaming is now RTSP-out from cameras.py's in-process GstRtspServer.
    - SessionAndStreamManager collapsed to SessionManager (recorder only).
    - Port 57000 UDP + port 57001 UDP replaced with a single TCP server
      on port 57000. Length-prefixed JSON envelopes, ack per message.
    - Motion UDP payload trimmed (no camera/button/a/b/cruise/fwd/accel/
      priority/record). Fields still consumed:
          robot_lock, lin_x, ang_z, brake, head, speed, ai_request
    - camera field is gone entirely — no more switch_source.
    - PTZ home capture/return via TCP event: {"type":"ptz","data":{"action":"capture_home"|"goto_home"}}
    - Telemetry publisher pushes state to Azure over UDP.

Subsystem status (this build):
    - motion, cameras (RTSP-out + record), local gamepad: always on
    - lights, PTZ, audio: constructed with try/except; disabled on init failure
      (missing hardware just logs "… init failed — disabled" and keeps going)
    - sensors (GPS, TempHum, Battery) + lidar: constructed from sensors.py,
      gated on their config flags. GPS feeds the recorder + telemetry; lidar
      feeds the motion forward-brake gate when cfg.lidar_safety_brake is set.

Note on local_gamepad: its in-process event packets need to be built in
the new envelope shape ({type, data} instead of {event, ...}) — see
patch notes at end of local_gamepad.py update.
"""

import json
import os
import signal
import socket
import struct
import threading
import time
from pathlib import Path
from typing import Any, Callable, Optional

from .common import first_float, log, now_mono, truthy
from .config import LabConfig


# ══════════════════════════════════════════════════════════════════════════════
#  UDP listener (motion channel)
# ══════════════════════════════════════════════════════════════════════════════

class UdpListener:
    def __init__(
        self,
        bind_ip:  str,
        port:     int,
        label:    str,
        on_pkt:   Callable[[dict, tuple, int], None],
    ) -> None:
        self._bind = bind_ip
        self._port = port
        self._label = label
        self._on_pkt = on_pkt
        self._sock: Optional[socket.socket] = None
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self._bind, self._port))
        self._sock.settimeout(0.5)
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name=f"udp-{self._label}")
        self._thread.start()
        log("teleop", f"UDP listener {self._label} on {self._bind}:{self._port}")

    def stop(self) -> None:
        self._stop.set()
        if self._sock is not None:
            try: self._sock.close()
            except Exception: pass
        if self._thread is not None:
            self._thread.join(timeout=1.0)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                data, addr = self._sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                pkt = json.loads(data.decode("utf-8"))
            except Exception as exc:
                log("teleop", f"{self._label} bad packet from {addr}: {exc}")
                continue
            try:
                self._on_pkt(pkt, addr, self._port)
            except Exception as exc:
                log("teleop", f"{self._label} dispatch error: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
#  TCP event server (unified events channel)
# ══════════════════════════════════════════════════════════════════════════════

LENGTH_PREFIX_FORMAT = ">I"
LENGTH_PREFIX_SIZE = struct.calcsize(LENGTH_PREFIX_FORMAT)


class TcpEventServer:
    """Accepts one persistent TCP connection from the pilot gamepad.

    Reads {seq, t, type, data} envelopes with 4-byte length prefix framing,
    dispatches by type, and sends back {ack_of, status, t} on the same
    socket. If the connection drops, the server keeps listening for the
    next client.
    """

    def __init__(
        self,
        bind_ip:  str,
        port:     int,
        on_event: Callable[[dict], tuple[str, Optional[str]]],   # → (status, error_or_none)
    ) -> None:
        self._bind = bind_ip
        self._port = port
        self._on_event = on_event
        self._server: Optional[socket.socket] = None
        self._stop = threading.Event()
        self._accept_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind((self._bind, self._port))
        self._server.listen(4)
        self._server.settimeout(0.5)
        self._accept_thread = threading.Thread(
            target=self._accept_loop, daemon=True, name="tcp-events-accept"
        )
        self._accept_thread.start()
        log("teleop", f"TCP event server on {self._bind}:{self._port}")

    def stop(self) -> None:
        self._stop.set()
        if self._server is not None:
            try: self._server.close()
            except Exception: pass
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=1.0)

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            try:
                conn, addr = self._server.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            log("teleop", f"TCP event client connected from {addr}")
            threading.Thread(
                target=self._client_loop, args=(conn, addr),
                daemon=True, name=f"tcp-events-{addr[0]}"
            ).start()

    def _client_loop(self, conn: socket.socket, addr: tuple) -> None:
        try:
            while not self._stop.is_set():
                header = self._read_exactly(conn, LENGTH_PREFIX_SIZE)
                if header is None:
                    break
                (msg_len,) = struct.unpack(LENGTH_PREFIX_FORMAT, header)
                if msg_len <= 0 or msg_len > 1_000_000:
                    log("teleop", f"TCP suspicious msg_len {msg_len} from {addr}")
                    break
                body = self._read_exactly(conn, msg_len)
                if body is None:
                    break
                try:
                    envelope = json.loads(body.decode("utf-8"))
                except Exception as exc:
                    log("teleop", f"TCP bad JSON from {addr}: {exc}")
                    continue

                seq = envelope.get("seq")
                try:
                    status, err = self._on_event(envelope)
                except Exception as exc:
                    status, err = "error", f"handler_exception: {exc}"

                ack: dict[str, Any] = {"ack_of": seq, "status": status, "t": time.time()}
                if err:
                    ack["error"] = err
                self._send_framed(conn, ack)
        except Exception as exc:
            log("teleop", f"TCP client {addr} loop error: {exc}")
        finally:
            try: conn.close()
            except Exception: pass
            log("teleop", f"TCP event client disconnected: {addr}")

    @staticmethod
    def _read_exactly(conn: socket.socket, n: int) -> Optional[bytes]:
        buf = b""
        while len(buf) < n:
            try:
                chunk = conn.recv(n - len(buf))
            except OSError:
                return None
            if not chunk:
                return None
            buf += chunk
        return buf

    @staticmethod
    def _send_framed(conn: socket.socket, obj: dict) -> None:
        body = json.dumps(obj).encode("utf-8")
        header = struct.pack(LENGTH_PREFIX_FORMAT, len(body))
        try:
            conn.sendall(header + body)
        except OSError:
            pass


# ══════════════════════════════════════════════════════════════════════════════
#  Source arbiter (unchanged — local wins over remote by priority)
# ══════════════════════════════════════════════════════════════════════════════

class SourceArbiter:
    def __init__(self, priorities: dict, timeout_sec: float) -> None:
        self._priorities = dict(priorities)
        self._timeout = timeout_sec
        self._last_seen: dict = {k: 0.0 for k in priorities}
        self._lock = threading.Lock()
        self._active: Optional[str] = None

    def report(self, source: str) -> None:
        with self._lock:
            self._last_seen[source] = now_mono()
            self._update_active_locked()

    def is_active(self, source: str) -> bool:
        with self._lock:
            self._update_active_locked()
            return self._active == source

    def active(self) -> Optional[str]:
        with self._lock:
            self._update_active_locked()
            return self._active

    def _update_active_locked(self) -> None:
        now = now_mono()
        live = [
            (self._priorities[s], s)
            for s, ts in self._last_seen.items()
            if (now - ts) <= self._timeout
        ]
        if not live:
            self._active = None
            return
        live.sort()
        self._active = live[0][1]


def parse_lock_state(pkt: dict, last_known_locked: bool) -> tuple[bool, bool]:
    if "robot_lock" in pkt:
        return truthy(pkt["robot_lock"]), True
    if "lock" in pkt:
        return truthy(pkt["lock"]), True
    return last_known_locked, False


# ══════════════════════════════════════════════════════════════════════════════
#  Session manager — recorder only (stream half removed)
# ══════════════════════════════════════════════════════════════════════════════

class SessionManager(threading.Thread):
    """Coordinates recorder lifecycle around lock/unlock edges.

    Previous version also coordinated a Daily stream; that's gone entirely.
    """

    def __init__(self, recorder, debounce_sec: float = 0.75) -> None:
        super().__init__(daemon=True, name="session-manager")
        self._recorder = recorder
        self._debounce_sec = debounce_sec

        self._lock = threading.Lock()
        self._last_edge_t = 0.0
        self._requested_locked: Optional[bool] = None  # last value handed to set_robot_lock
        self._pending_locked: Optional[bool] = None
        self._current_locked = True
        self._stop = threading.Event()
        self._wake = threading.Event()

    def set_robot_lock(self, locked: bool) -> None:
        locked = bool(locked)
        with self._lock:
            # The motion channel re-asserts robot_lock on every packet (~50 Hz).
            # Only treat a *change* as an edge — otherwise the steady stream of
            # same-value calls keeps resetting the debounce timer and the
            # unlock→start / lock→stop transition never fires.
            if locked == self._requested_locked:
                return
            self._requested_locked = locked
            self._pending_locked = locked
            self._last_edge_t = now_mono()
        self._wake.set()

    def run(self) -> None:
        while not self._stop.is_set():
            self._wake.wait(timeout=0.1)
            self._wake.clear()
            with self._lock:
                pending = self._pending_locked
                last_t = self._last_edge_t
            if pending is None:
                continue
            if now_mono() - last_t < self._debounce_sec:
                self._wake.set()
                time.sleep(0.05)
                continue
            with self._lock:
                self._pending_locked = None
                target = pending
            if target == self._current_locked:
                continue
            self._current_locked = target
            try:
                # Recorder is start/stop driven, not flag driven. Unlock →
                # start(). Lock → stop() and finalize the MP4 + JSONL.
                # set_robot_lock() is still called so the recorder tick loop
                # can gate frame writes internally if it wants to.
                self._recorder.set_robot_lock(target)
                if target:
                    log("session", "robot LOCKED — stopping recorder")
                    self._recorder.stop()
                else:
                    log("session", "robot UNLOCKED — starting recorder")
                    self._recorder.start()
            except Exception as exc:
                log("session", f"recorder transition error: {exc}")

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        try:
            self.join(timeout=2.0)
        except Exception:
            pass
        # Finalize any in-progress recording. Without this the MP4 never gets
        # its moov atom (mp4mux needs EOS), the JSONL isn't flushed/closed, and
        # session.json is never written — i.e. a Ctrl-C mid-recording leaves a
        # corrupt session. Safe to call when not recording.
        try:
            self._recorder.set_robot_lock(True)
            self._recorder.stop()
        except Exception as exc:
            log("session", f"final recorder stop error: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
#  Telemetry snapshot helper (dashboard schema, real values)
# ══════════════════════════════════════════════════════════════════════════════

# Turns commanded lin_x into a 0–100% dashboard bar. Set this to your
# gamepad's max |lin_x| as seen by motion.published_state(). If lin_x is
# already normalized to [-1, 1], leave it at 1.0.  << VERIFY on hardware >>
MAX_LIN_X = 0.75

_SPEED_MODE_CUTS = (34.0, 67.0)   # <34 slow, <67 medium, else fast


def _speed_mode_from_pct(pct: float) -> str:
    if pct < _SPEED_MODE_CUTS[0]:
        return "slow"
    if pct < _SPEED_MODE_CUTS[1]:
        return "medium"
    return "fast"


def _read_cpu_temp_f() -> Optional[float]:
    """Jetson CPU temperature in °F from the kernel thermal zones.

    Prefers a zone whose `type` mentions CPU; falls back to the hottest zone.
    """
    import glob
    chosen_c: Optional[float] = None
    all_c: list = []
    for zone in glob.glob("/sys/class/thermal/thermal_zone*"):
        try:
            with open(f"{zone}/temp") as f:
                milli = int(f.read().strip())
        except (OSError, ValueError):
            continue
        c = milli / 1000.0
        all_c.append(c)
        try:
            with open(f"{zone}/type") as f:
                ztype = f.read().strip().lower()
        except OSError:
            ztype = ""
        if chosen_c is None and "cpu" in ztype:
            chosen_c = c
    if chosen_c is None and all_c:
        chosen_c = max(all_c)
    if chosen_c is None:
        return None
    return chosen_c * 9.0 / 5.0 + 32.0


def _make_dashboard_snapshot_fn(motion, temphum, gps, battery, speed_label_fn):
    """Callable AzureTelemetryPublisher invokes each tick, returning REAL values
    in the dashboard schema. The publisher adds robot_id, ts, up_time, fake.

    Field sources:
        speed_pct / speed_mode ← motion.published_state() lin_x + gamepad speed label
        robot_battery_pct      ← battery.get()["bat_soc"]
        box_temp_F / humidity  ← temphum.get()["temp_f" / "humidity_pct"]
        cpu_temp_F             ← Jetson thermal zone
        gps_lat/lng/alt/orient/fix ← gps.get()
    Missing values are simply omitted.
    """
    def snapshot() -> dict:
        out: dict = {}

        # Speed
        try:
            lin, _ang = motion.published_state()
            pct = (min(100.0, abs(float(lin)) / MAX_LIN_X * 100.0) if MAX_LIN_X else 0.0)
            out["speed_pct"] = round(pct, 1)
        except Exception:
            pass
        label = None
        try:
            label = speed_label_fn()
        except Exception:
            pass
        out["speed_mode"] = (str(label) if label
                             else _speed_mode_from_pct(out.get("speed_pct", 0.0)))

        # Battery state of charge
        try:
            if battery is not None:
                soc = battery.get().get("bat_soc")
                if soc is not None:
                    out["robot_battery_pct"] = round(float(soc), 1)
        except Exception:
            pass

        # Enclosure temp + humidity (TEMPerHUM)
        try:
            if temphum is not None:
                th = temphum.get()
                if th.get("temp_f") is not None:
                    out["box_temp_F"] = round(float(th["temp_f"]), 1)
                if th.get("humidity_pct") is not None:
                    out["humidity_pct"] = round(float(th["humidity_pct"]), 1)
        except Exception:
            pass

        # CPU temp (Jetson)
        cpu_f = _read_cpu_temp_f()
        if cpu_f is not None:
            out["cpu_temp_F"] = round(cpu_f, 1)

        # GPS
        try:
            if gps is not None:
                g = gps.get()
                if g.get("gps_latitude")  is not None: out["gps_lat"]  = round(float(g["gps_latitude"]), 7)
                if g.get("gps_longitude") is not None: out["gps_lng"]  = round(float(g["gps_longitude"]), 7)
                if g.get("gps_altitude")  is not None: out["gps_alt"]  = round(float(g["gps_altitude"]), 1)
                heading = g.get("heading_deg_true", g.get("orientation"))
                if heading is not None:
                    out["gps_orient"] = round(float(heading), 1)
                if g.get("gps_fix") is not None:
                    out["gps_fix"] = g["gps_fix"]
        except Exception:
            pass

        return out
    return snapshot


# ══════════════════════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    cfg = LabConfig.load_secrets()

    # ── Startup banner ──────────────────────────────────────────────────────
    log("teleop", "=" * 60)
    log("teleop", f"cache_dir  = {cfg.cache_dir}")
    log("teleop", f"record_fps = {cfg.record_fps}")
    log("teleop", f"ports      = motion:{cfg.udp_motion_port}(UDP) events:{cfg.tcp_events_port}(TCP)")
    log("teleop", f"rtsp       = {cfg.gst_rtsp_bind}:{cfg.gst_rtsp_port}  usb_mount=/{cfg.usb_stream_mount}")
    log("teleop", f"telemetry  = udp://{cfg.udp_telemetry_host}:{cfg.udp_telemetry_port} @ {cfg.udp_telemetry_hz}Hz")
    log("teleop", "local_gamepad = always enabled")
    log("teleop", "=" * 60)

    # ── Subsystem imports ───────────────────────────────────────────────────
    from .cameras       import CamerasManager
    from .lights        import LightsController
    from .motion        import MotionController
    from .ptz           import PtzController
    from .audio         import AudioController
    from .record         import SessionRecorder
    from .azure_telemetry import AzureTelemetryPublisher
    from .local_gamepad   import LocalGamepad
    from .sensors       import GpsReader, TempHumReader, BatteryReader, LidarReader

    # ── Cameras (RTSP-out + local USB for record/AI) ────────────────────────
    cameras = CamerasManager(cfg)
    cameras.start()

    # ── Lidar (built before motion so its block_fn can gate the drivetrain) ──
    lidar: Optional[LidarReader] = None
    if cfg.lidar_enabled:
        try:
            lidar = LidarReader(
                port=cfg.lidar_port,
                symlink=cfg.lidar_symlink,
                usb_serial=cfg.lidar_usb_serial,
                baud=cfg.lidar_baud,
                poll_hz=cfg.lidar_poll_hz,
                scan_timeout_sec=cfg.lidar_scan_timeout_sec,
                range_min=cfg.lidar_range_min_m,
                range_max=cfg.lidar_range_max_m,
                min_quality=cfg.lidar_min_quality,
                front_min_deg=cfg.lidar_front_min_deg,
                front_max_deg=cfg.lidar_front_max_deg,
                left_min_deg=cfg.lidar_left_min_deg,
                left_max_deg=cfg.lidar_left_max_deg,
                right_min_deg=cfg.lidar_right_min_deg,
                right_max_deg=cfg.lidar_right_max_deg,
                bubble_front_m=cfg.lidar_bubble_front_m,
                bubble_left_m=cfg.lidar_bubble_left_m,
                bubble_right_m=cfg.lidar_bubble_right_m,
                stale_after_sec=cfg.lidar_stale_after_sec,
            )
            lidar.start()
        except Exception as exc:
            log("teleop", f"lidar init failed: {exc} — disabled")
            lidar = None

    # Forward-brake gate: only wired when lidar is up AND safety brake is on.
    lidar_block_fn = (
        lidar.is_blocked_forward
        if (lidar is not None and cfg.lidar_safety_brake)
        else None
    )
    if lidar_block_fn is not None:
        log("teleop", "lidar forward-brake gate ENABLED")

    # ── Motion ──────────────────────────────────────────────────────────────
    motion = MotionController(
        docker_host=cfg.docker_motion_host,
        docker_port=cfg.docker_motion_port,
        publish_hz=cfg.motion_publish_hz,
        watchdog_sec=cfg.motion_watchdog_sec,
        ang_z_scale=cfg.ang_z_scale,
        lidar_block_fn=lidar_block_fn,
    )
    motion.start()

    # ── Optional actuators — construct with try/except, disable on failure ──
    lights: Optional[LightsController] = None
    try:
        lights = LightsController(
            blink_period_sec=cfg.blink_period_sec,
            signal_timeout_sec=cfg.signal_timeout_sec,
            talk_default_duration=cfg.talk_default_duration,
            all_lights_cooldown_sec=cfg.all_lights_cooldown_sec,
            all_lights_blink_sec=cfg.all_lights_blink_sec,
        )
        lights.start()
    except Exception as exc:
        log("teleop", f"lights init failed: {exc} — disabled")
        lights = None

    ptz: Optional[PtzController] = None
    if getattr(cfg, "ptz_enabled", True):
        try:
            ptz = PtzController(
                ip=cfg.ptz_ip, port=cfg.ptz_port,
                user=cfg.ptz_user, password=(cfg.ptz_password or cfg.camera_password),
                pan_speed=cfg.ptz_pan_speed, tilt_speed=cfg.ptz_tilt_speed,
                loop_hz=cfg.ptz_loop_hz,
                deadband_sec=cfg.ptz_deadband_sec,
                stop_after_sec=cfg.ptz_stop_after_sec,
            )
            ptz.start()
            # PTZ has its own lock independent of the drivetrain; unlock now so
            # the operator can look around, and capture the startup pose as home.
            ptz.set_ptz_unlock_state(True)
        except Exception as exc:
            log("teleop", f"ptz init failed: {exc} — disabled")
            ptz = None
    else:
        log("teleop", "ptz disabled by config (ptz_enabled=False)")

    audio: Optional[AudioController] = None
    try:
        audio = AudioController(
            piper_model=cfg.piper_model,
            music_dir=cfg.music_dir,
            music_tracks=cfg.music_tracks,
            startup_volume_pct=cfg.startup_volume_pct,
            preferred_sink_patterns=cfg.preferred_sink_patterns,
            preferred_source_patterns=cfg.preferred_source_patterns,
            piper_speaker_id=cfg.piper_speaker_id,
        )
        audio.start()
    except Exception as exc:
        log("teleop", f"audio init failed: {exc} — disabled")
        audio = None

    # ── Sensors (GPS / TempHum / Battery) ───────────────────────────────────
    # GPS is always constructed — it feeds both the recorder and telemetry.
    # Orientation comes from the GPS/RTK receiver, not a separate IMU.
    gps: Optional[GpsReader] = None
    try:
        gps = GpsReader(udp_host=cfg.gps_udp_host, udp_port=cfg.gps_udp_port)
        gps.start()
    except Exception as exc:
        log("teleop", f"gps init failed: {exc} — disabled")
        gps = None

    temphum: Optional[TempHumReader] = None
    if cfg.temphum_enabled:
        try:
            temphum = TempHumReader(
                vid=cfg.temphum_vid,
                pid=cfg.temphum_pid,
                poll_sec=cfg.temphum_poll_sec,
            )
            temphum.start()
        except Exception as exc:
            log("teleop", f"temphum init failed: {exc} — disabled")
            temphum = None

    battery: Optional[BatteryReader] = None
    if cfg.battery_enabled:
        try:
            battery = BatteryReader(
                container=cfg.battery_container,
                topic=cfg.battery_topic,
                ros_setup=cfg.battery_ros_setup,
                ws_setup=cfg.battery_ws_setup,
                poll_sec=cfg.battery_poll_sec,
                cmd_timeout=cfg.battery_cmd_timeout_sec,
            )
            battery.start()
        except Exception as exc:
            log("teleop", f"battery init failed: {exc} — disabled")
            battery = None

    # ── Recorder ────────────────────────────────────────────────────────────
    # motion.published_state() is what actually went to the wheels (post-gate),
    # which is the right thing to log for training.
    recorder = SessionRecorder(
        base_dir=cfg.cache_dir,
        camera_name=cfg.record_camera_name,
        cameras=cameras,
        width=cfg.record_width,
        height=cfg.record_height,
        fps=cfg.record_fps,
        video_bitrate=cfg.record_video_bitrate,
        encoder_preference=cfg.record_encoder_preference,
        motion_state_fn=motion.published_state,
        gps_get_fn=(gps.get if gps is not None else None),
    )
    recorder.set_robot_lock(True)   # start not recording

    session_mgr = SessionManager(recorder)
    session_mgr.start()

    # ── Shared mutable state visible to dispatchers ─────────────────────────
    shared: dict = {
        "speed_label": None,   # last seen speed field from motion pkt
    }

    # Telemetry — dashboard WebSocket (1 Hz) + Azure IoT Hub via DPS (30 s),
    # real robot values in the dashboard schema. IoT Hub secrets are read from
    # /etc/revobots/revo.env (AZURE_DEVICE_ID / AZURE_DPS_ID_SCOPE /
    # AZURE_DPS_PRIMARY_KEY); if absent, only the dashboard WS runs.
    robot_id = os.environ.get("AZURE_DEVICE_ID", "iwu-scout-001")
    dashboard_snap = _make_dashboard_snapshot_fn(
        motion, temphum, gps, battery, lambda: shared.get("speed_label")
    )

    azure_tel = AzureTelemetryPublisher(
        robot_id=robot_id,
        snapshot_fn=dashboard_snap,
        env_file=str(Path(__file__).parent / ".env"),   # was "/etc/revobots/revo.env"
        dashboard_interval_s=1.0,
        iot_interval_s=30.0,
    )
    azure_tel.start()

    arbiter = SourceArbiter(
        priorities={
            "local":  cfg.local_dongle_priority,
            "remote": cfg.remote_gamepad_priority,
        },
        timeout_sec=cfg.source_activity_timeout_sec,
    )
    lock_state = {"locked": True}
    prev_state = {"speed_label": None}

    # ── Motion dispatcher (trimmed schema) ──────────────────────────────────

    def on_motion_packet(pkt: dict, addr, port: int) -> None:
        source = "local" if pkt.get("_local") else "remote"
        arbiter.report(source)
        if not arbiter.is_active(source):
            return

        locked, lock_present = parse_lock_state(pkt, lock_state["locked"])
        if lock_present and locked != lock_state["locked"]:
            log("teleop",
                f"LOCK EDGE source={source} addr={addr}: "
                f"{lock_state['locked']} -> {locked}")
        lock_state["locked"] = locked

        lin = first_float(pkt, ("lin_x", "linx", "linear_x"))
        ang = first_float(pkt, ("ang_z", "angz", "angular_z"))
        brake = first_float(pkt, ("brake",), default=0.0) > cfg.brake_threshold

        motion.command(lin, ang, locked, brake)

        # AI-enable: gamepad sends "enable" for N packets after chord press
        ai_request = pkt.get("ai_request")
        if ai_request == "enable":
            motion.set_ai_enabled(True)
        # Note: there's no explicit "disable" over the wire today. Add one
        # when you decide how AI mode ends (chord press again? explicit
        # button? timeout?).

        if lights is not None:
            lights.set_robot_lock(locked)

        if lock_present:
            session_mgr.set_robot_lock(locked)

        # PTZ head (kept — head is still in the trimmed payload)
        if ptz is not None:
            head = pkt.get("head")
            if head:
                ptz.command(str(head))
            speed_label = pkt.get("speed")
            if speed_label:
                shared["speed_label"] = speed_label
                if speed_label != prev_state["speed_label"]:
                    if prev_state["speed_label"] is not None:
                        ptz.capture_home()
                    prev_state["speed_label"] = speed_label

    # ── TCP event dispatcher ────────────────────────────────────────────────

    def on_event(envelope: dict) -> tuple[str, Optional[str]]:
        """Return (status, error_or_none) so the TCP server can ack."""
        type_ = (envelope.get("type") or "").strip().lower()
        data  = envelope.get("data") or {}

        try:
            if type_ == "lights":
                if lights is not None:
                    lights.command(envelope)
                return "ok", None

            if type_ == "indicator":
                if lights is not None:
                    lights.command(envelope)
                return "ok", None

            if type_ == "audio":
                if audio is not None:
                    vol = data.get("volume_pct")
                    if vol is not None:
                        audio.set_volume(int(vol))
                return "ok", None

            if type_ == "talk":
                # Two effects for talk: (1) trigger the light blink,
                # (2) speak the text if present.
                if lights is not None:
                    lights.command(envelope)
                if audio is not None:
                    text = data.get("text")
                    if text:
                        audio.speak(str(text))
                return "ok", None

            if type_ == "music":
                if audio is not None:
                    action = (data.get("action") or "").strip().lower()
                    if action == "play":
                        track = data.get("track")
                        if track is not None:
                            audio.play_music(int(track))
                return "ok", None

            if type_ == "ptz":
                if ptz is not None:
                    action = (data.get("action") or "").strip().lower()
                    if action == "capture_home":
                        ptz.capture_home()
                    elif action == "goto_home":
                        ptz.goto_home()
                return "ok", None

            return "error", f"unknown_type:{type_}"
        except Exception as exc:
            return "error", f"handler_exception:{exc}"

    # ── Wire listeners ──────────────────────────────────────────────────────

    udp_motion = UdpListener(cfg.udp_listen_ip, cfg.udp_motion_port, "motion",
                             on_motion_packet)
    tcp_events = TcpEventServer(cfg.udp_listen_ip, cfg.tcp_events_port, on_event)
    udp_motion.start()
    tcp_events.start()

    # Local gamepad — same dispatchers, in-process. Motion callbacks accept
    # (pkt, addr, port); events callback needs the same signature so the
    # local path looks identical to the TCP one from teleop's view.
    local = LocalGamepad(
        on_motion=on_motion_packet,
        on_events=lambda envelope, addr, port: on_event(envelope),
        initial_robot_lock=True,
        priority_value=cfg.local_dongle_priority,
    )
    local.start()
    log("teleop", "local gamepad started")

    # ── Signal handling & main wait ─────────────────────────────────────────

    running = threading.Event()
    running.set()

    def on_signal(*_):
        running.clear()

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    log("teleop", "ready")
    try:
        while running.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass

    # ── Shutdown ────────────────────────────────────────────────────────────
    log("teleop", "shutting down…")
    session_mgr.stop()
    for name, sub in [
        ("udp_motion",    udp_motion),
        ("tcp_events",    tcp_events),
        ("local",         local),
        ("azure_tel",     azure_tel),
        ("ptz",           ptz),
        ("lights",        lights),
        ("audio",         audio),
        ("motion",        motion),
        ("gps",           gps),
        ("temphum",       temphum),
        ("battery",       battery),
        ("lidar",         lidar),
        ("cameras",       cameras),
    ]:
        if sub is None:
            continue
        try:
            sub.stop()
        except Exception as exc:
            log("teleop", f"{name} stop error: {exc}")

    log("teleop", "done.")


if __name__ == "__main__":
    main()