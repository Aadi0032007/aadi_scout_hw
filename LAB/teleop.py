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
    - Motion UDP payload trimmed. Fields still consumed:
          robot_lock, lin_x, ang_z, brake, head, speed, ai_request
    - PTZ home capture/return via TCP event:
          {"type":"ptz","data":{"action":"capture_home"|"goto_home"}}
    - Telemetry: Azure dashboard WS + IoT Hub via DPS.
    - BatteryReader now streams `docker exec rostopic echo /bms_fb` instead
      of polling per tick — see sensors.py. Teleop passes `stale_after_sec`.
    - azure-iot-device / paho / websockets loggers muted to WARNING at import
      time so the console isn't drowned in `INFO:azure...publishing on ...`
      per telemetry tick. Real errors still surface.
    - Snapshot now carries lin_x / ang_z straight from motion.published_state()
      (the actual values forwarded to the segway_ros1 UDP endpoint).
    - Startup prints the full snapshot once so every field can be eyeballed
      as populated (or MISSING) before the console goes quiet.
"""

import json
import logging
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
#  Third-party log noise suppression
# ══════════════════════════════════════════════════════════════════════════════
# The Azure IoT SDK and its paho MQTT transport log at INFO for every hub
# reconnect + every message publish. At 1 Hz dashboard + 30 s IoT Hub cadence
# that's a wall of text per second. Keep WARNING+ so real problems (auth
# failure, disconnect) still surface.
for _name in (
    "azure",
    "azure.iot",
    "azure.iot.device",
    "azure.iot.device.common",
    "azure.iot.device.common.mqtt_transport",
    "azure.iot.device.common.pipeline",
    "azure.iot.device.common.pipeline.pipeline_stages_mqtt",
    "azure.iot.device.iothub",
    "azure.iot.device.iothub.aio",
    "azure.iot.device.iothub.aio.async_clients",
    "azure.iot.device.iothub.abstract_clients",
    "azure.iot.device.provisioning",
    "azure.iot.device.provisioning.aio",
    "azure.iot.device.provisioning.aio.async_provisioning_device_client",
    "azure.iot.device.provisioning.pipeline",
    "azure.iot.device.provisioning.pipeline.pipeline_stages_provisioning",
    "azure.iot.device.provisioning.abstract_provisioning_device_client",
    "paho",
    "paho.mqtt",
    "websockets",
    "websockets.client",
    "websockets.protocol",
):
    logging.getLogger(_name).setLevel(logging.WARNING)


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
    def __init__(
        self,
        bind_ip:  str,
        port:     int,
        on_event: Callable[[dict], tuple[str, Optional[str]]],
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
#  Session manager — recorder only
# ══════════════════════════════════════════════════════════════════════════════

class SessionManager(threading.Thread):
    def __init__(self, recorder, debounce_sec: float = 0.75) -> None:
        super().__init__(daemon=True, name="session-manager")
        self._recorder = recorder
        self._debounce_sec = debounce_sec

        self._lock = threading.Lock()
        self._last_edge_t = 0.0
        self._requested_locked: Optional[bool] = None
        self._pending_locked: Optional[bool] = None
        self._current_locked = True
        self._stop = threading.Event()
        self._wake = threading.Event()

    def set_robot_lock(self, locked: bool) -> None:
        locked = bool(locked)
        with self._lock:
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

# Every key the snapshot may emit. Used by the 10-pass startup debug printer
# so a MISSING field is obvious at a glance.
_SNAPSHOT_KEYS = (
    "lin_x", "ang_z",
    "speed_pct", "speed_mode",
    "robot_battery_pct",
    "box_temp_F", "humidity_pct", "cpu_temp_F",
    "gps_lat", "gps_lng", "gps_alt", "gps_orient", "gps_fix",
)


def _speed_mode_from_pct(pct: float) -> str:
    if pct < _SPEED_MODE_CUTS[0]:
        return "slow"
    if pct < _SPEED_MODE_CUTS[1]:
        return "medium"
    return "fast"


def _read_cpu_temp_f() -> Optional[float]:
    """Jetson CPU temperature in °F from the kernel thermal zones."""
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
        lin_x / ang_z          ← motion.published_state() — exactly what was
                                  forwarded to the segway_ros1 UDP endpoint
                                  this tick (post-scale, post-gate, post-lidar)
        speed_pct / speed_mode ← lin_x magnitude vs MAX_LIN_X + gamepad label
        robot_battery_pct      ← battery.get()["bat_soc"]
        box_temp_F / humidity  ← temphum.get()["temp_f" / "humidity_pct"]
        cpu_temp_F             ← Jetson thermal zone
        gps_lat/lng/alt/orient/fix ← gps.get()
    Missing values are simply omitted.
    """
    def snapshot() -> dict:
        out: dict = {}

        # Motion — the real numbers going out on the wire to the ROS1 container.
        lin: Optional[float] = None
        try:
            lin, ang = motion.published_state()
            out["lin_x"] = round(float(lin), 4)
            out["ang_z"] = round(float(ang), 4)
        except Exception:
            pass

        # Speed % derived from the same lin_x we just recorded, so the two
        # fields always agree — no re-fetch, no race.
        if lin is not None:
            try:
                pct = (min(100.0, abs(float(lin)) / MAX_LIN_X * 100.0)
                       if MAX_LIN_X else 0.0)
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

    log("teleop", "=" * 60)
    log("teleop", f"cache_dir  = {cfg.cache_dir}")
    log("teleop", f"record_fps = {cfg.record_fps}")
    log("teleop", f"ports      = motion:{cfg.udp_motion_port}(UDP) events:{cfg.tcp_events_port}(TCP)")
    log("teleop", f"rtsp       = {cfg.gst_rtsp_bind}:{cfg.gst_rtsp_port}  usb_mount=/{cfg.usb_stream_mount}")
    log("teleop", f"telemetry  = udp://{cfg.udp_telemetry_host}:{cfg.udp_telemetry_port} @ {cfg.udp_telemetry_hz}Hz")
    log("teleop", "local_gamepad = always enabled")
    log("teleop", "=" * 60)

    from .cameras       import CamerasManager
    from .lights        import LightsController
    from .motion        import MotionController
    from .ptz           import PtzController
    from .audio         import AudioController
    from .record         import SessionRecorder
    from .azure_telemetry import AzureTelemetryPublisher
    from .local_gamepad   import LocalGamepad
    from .sensors       import GpsReader, TempHumReader, BatteryReader, LidarReader

    cameras = CamerasManager(cfg)
    cameras.start()

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

    lidar_block_fn = (
        lidar.is_blocked_forward
        if (lidar is not None and cfg.lidar_safety_brake)
        else None
    )
    if lidar_block_fn is not None:
        log("teleop", "lidar forward-brake gate ENABLED")

    motion = MotionController(
        docker_host=cfg.docker_motion_host,
        docker_port=cfg.docker_motion_port,
        publish_hz=cfg.motion_publish_hz,
        watchdog_sec=cfg.motion_watchdog_sec,
        ang_z_scale=cfg.ang_z_scale,
        lidar_block_fn=lidar_block_fn,
    )
    motion.start()

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
            # Streaming design — no more per-poll timeout / shell exec. See
            # sensors.py::BatteryReader. `stale_after_sec` is how long we
            # tolerate silence on /bms_fb before tearing down and respawning.
            battery = BatteryReader(
                container=cfg.battery_container,
                topic=cfg.battery_topic,
                ros_setup=cfg.battery_ros_setup,
                ws_setup=cfg.battery_ws_setup,
                stale_after_sec=cfg.battery_stale_after_sec,
            )
            battery.start()
        except Exception as exc:
            log("teleop", f"battery init failed: {exc} — disabled")
            battery = None

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
    recorder.set_robot_lock(True)

    session_mgr = SessionManager(recorder)
    session_mgr.start()

    shared: dict = {"speed_label": None}

    robot_id = os.environ.get("AZURE_DEVICE_ID", "iwu-scout-001")
    dashboard_snap = _make_dashboard_snapshot_fn(
        motion, temphum, gps, battery, lambda: shared.get("speed_label")
    )

    # One-shot snapshot dump. Every expected key is shown as value or MISSING
    # so it's obvious which sources aren't populating. Runs synchronously —
    # sensors that haven't produced their first reading yet will show MISSING,
    # which is exactly what you want at startup.
    try:
        snap = dashboard_snap() or {}
        parts = [
            f"{k}={snap[k]}" if k in snap else f"{k}=MISSING"
            for k in _SNAPSHOT_KEYS
        ]
        log("snapdbg", " ".join(parts))
    except Exception as exc:
        log("snapdbg", f"snapshot error: {exc}")

    azure_tel = AzureTelemetryPublisher(
        robot_id=robot_id,
        snapshot_fn=dashboard_snap,
        env_file=str(Path(__file__).parent / ".env"),
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

        ai_request = pkt.get("ai_request")
        if ai_request == "enable":
            motion.set_ai_enabled(True)

        if lights is not None:
            lights.set_robot_lock(locked)

        if lock_present:
            session_mgr.set_robot_lock(locked)

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

    def on_event(envelope: dict) -> tuple[str, Optional[str]]:
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

    udp_motion = UdpListener(cfg.udp_listen_ip, cfg.udp_motion_port, "motion",
                             on_motion_packet)
    tcp_events = TcpEventServer(cfg.udp_listen_ip, cfg.tcp_events_port, on_event)
    udp_motion.start()
    tcp_events.start()

    local = LocalGamepad(
        on_motion=on_motion_packet,
        on_events=lambda envelope, addr, port: on_event(envelope),
        initial_robot_lock=True,
        priority_value=cfg.local_dongle_priority,
    )
    local.start()
    log("teleop", "local gamepad started")

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