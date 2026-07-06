# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations

"""
teleop.py — REDESIGN v3 (browser + bridge-server integration).

Major structural changes vs previous version:

    - TCP event server REMOVED. All non-motion events now arrive via the
      fleet WebSocket that the robot connects OUT to (bridge_ws.py).
    - HeartbeatPublisher ADDED. Without the 60s POST the browser can't find
      us. See heartbeat.py.
    - UDP motion port stays 55999. Payload is the bridge-server's trimmed
      schema: {seq, t, brain, lin_x, ang_z, brake:int, head}. No robot_lock.
    - robot_lock is WS-authoritative. UDP silence just triggers motion.py's
      watchdog (brake in place), does NOT latch lock. The person can resume
      from browser or after restarting their gamepad.
    - Local dongle stays. Its own dispatcher on_local_event keeps the old
      TCP-shape envelope ({seq,t,type,data}) since it's richer than the
      browser's flat shape (talk duration, music track, indicator side).
    - bubble_mode over WS → motion.set_lidar_block_enabled(bool).
    - ai_mode over WS → motion.set_ai_enabled(bool). Chord over gamepad
      still works via ai_request="enable" in local motion packet.
    - PTZ head has two writers: UDP path (per motion packet) and WS path
      (browser). Last-write-wins per tick. Acceptable — the browser and
      dongle aren't typically active simultaneously.
"""

import json
import logging
import os
import signal
import socket
import threading
import time
from pathlib import Path
from typing import Any, Callable, Optional

from .common import first_float, log, now_mono, truthy
from .config import LabConfig


# ══════════════════════════════════════════════════════════════════════════════
#  Third-party log noise suppression
# ══════════════════════════════════════════════════════════════════════════════
# Azure IoT SDK + paho MQTT + websockets are chatty at INFO. Keep WARNING+ so
# real problems still surface.
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
            # Only treat a *change* as an edge — otherwise the steady stream
            # of same-value calls (from the WS) keeps resetting the debounce
            # timer and the unlock→start / lock→stop transition never fires.
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
        # Finalize any in-progress recording so mp4mux writes moov and
        # session.json is created. Safe to call when not recording.
        try:
            self._recorder.set_robot_lock(True)
            self._recorder.stop()
        except Exception as exc:
            log("session", f"final recorder stop error: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
#  Telemetry snapshot helper (dashboard schema, real values)
# ══════════════════════════════════════════════════════════════════════════════

_SPEED_MODE_CUTS = (34.0, 67.0)


def _speed_mode_from_pct(pct: float) -> str:
    if pct < _SPEED_MODE_CUTS[0]:
        return "slow"
    if pct < _SPEED_MODE_CUTS[1]:
        return "medium"
    return "fast"


def _read_cpu_temp_f() -> Optional[float]:
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
    def snapshot() -> dict:
        out: dict = {}

        try:
            lin, _ang = motion.published_state()
            out["speed_pct"] = round(float(lin) * 100.0, 1)
        except Exception:
            pass

        label = None
        try:
            label = speed_label_fn()
        except Exception:
            pass
        out["speed_mode"] = (str(label) if label
                             else _speed_mode_from_pct(abs(out.get("speed_pct", 0.0))))

        try:
            if battery is not None:
                soc = battery.get().get("bat_soc")
                if soc is not None:
                    out["robot_battery_pct"] = round(float(soc), 1)
        except Exception:
            pass

        try:
            if temphum is not None:
                th = temphum.get()
                if th.get("temp_f") is not None:
                    out["box_temp_F"] = round(float(th["temp_f"]), 1)
                if th.get("humidity_pct") is not None:
                    out["humidity_pct"] = round(float(th["humidity_pct"]), 1)
        except Exception:
            pass

        cpu_f = _read_cpu_temp_f()
        if cpu_f is not None:
            out["cpu_temp_F"] = round(cpu_f, 1)

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


def _debug_dump_snapshot_once(snap_fn: Callable[[], dict],
                              delay_sec: float = 3.0) -> None:
    def _run():
        time.sleep(delay_sec)
        try:
            snap = snap_fn() or {}
        except Exception as exc:
            log("teleop", f"snapshot dump error: {exc}")
            return
        log("teleop", "── snapshot preview ──")
        if not snap:
            log("teleop", "    (empty)")
        else:
            for k in sorted(snap.keys()):
                log("teleop", f"    {k} = {snap[k]!r}")
        log("teleop", "──────────────────────")
    threading.Thread(target=_run, daemon=True, name="snapshot-dump").start()


# ══════════════════════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    cfg = LabConfig.load_secrets()

    # ── Startup banner ──────────────────────────────────────────────────────
    log("teleop", "=" * 60)
    log("teleop", f"robot_id   = {cfg.robot_id}")
    log("teleop", f"cache_dir  = {cfg.cache_dir}")
    log("teleop", f"record_fps = {cfg.record_fps}")
    log("teleop", f"motion     = udp://{cfg.udp_listen_ip}:{cfg.udp_motion_port}")
    log("teleop", f"fleet_ws   = {cfg.fleet_ws_url_template.format(robot_id=cfg.robot_id)}")
    log("teleop", f"fleet_reg  = {cfg.fleet_register_url} every {cfg.heartbeat_interval_sec}s")
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
    from .bridge_ws     import BridgeWsClient
    from .heartbeat     import HeartbeatPublisher

    # ── Cameras ─────────────────────────────────────────────────────────────
    cameras = CamerasManager(cfg)
    cameras.start()

    # ── Lidar (before motion so its block_fn can gate the drivetrain) ───────
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

    # Lidar block fn is ALWAYS wired if lidar exists. Runtime on/off is via
    # motion.set_lidar_block_enabled() driven by WS bubble_mode. The initial
    # state comes from cfg.lidar_safety_brake.
    lidar_block_fn = lidar.is_blocked_forward if lidar is not None else None
    if lidar_block_fn is not None:
        log("teleop",
            f"lidar forward-brake gate wired "
            f"(initial={'ON' if cfg.lidar_safety_brake else 'OFF'})")

    # ── Motion ──────────────────────────────────────────────────────────────
    motion = MotionController(
        docker_host=cfg.docker_motion_host,
        docker_port=cfg.docker_motion_port,
        publish_hz=cfg.motion_publish_hz,
        watchdog_sec=cfg.motion_watchdog_sec,
        ang_z_scale=cfg.ang_z_scale,
        lidar_block_fn=lidar_block_fn,
        lidar_block_enabled=cfg.lidar_safety_brake,
    )
    motion.start()

    # ── Optional actuators ──────────────────────────────────────────────────
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

    # ── Sensors ─────────────────────────────────────────────────────────────
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
                stale_after_sec=cfg.battery_stale_after_sec,
            )
            battery.start()
        except Exception as exc:
            log("teleop", f"battery init failed: {exc} — disabled")
            battery = None

    # ── Recorder ────────────────────────────────────────────────────────────
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

    # ── Shared mutable state visible to dispatchers ─────────────────────────
    shared: dict = {
        "speed_label": None,
    }

    # ── Azure telemetry ─────────────────────────────────────────────────────
    dashboard_snap = _make_dashboard_snapshot_fn(
        motion, temphum, gps, battery, lambda: shared.get("speed_label")
    )

    azure_tel = AzureTelemetryPublisher(
        robot_id=cfg.robot_id,
        snapshot_fn=dashboard_snap,
        env_file=str(Path(__file__).parent / ".env"),
        dashboard_interval_s=1.0,
        iot_interval_s=30.0,
    )
    azure_tel.start()

    _debug_dump_snapshot_once(dashboard_snap, delay_sec=3.0)

    # ── Arbiter + lock state ────────────────────────────────────────────────
    arbiter = SourceArbiter(
        priorities={
            "local":  cfg.local_dongle_priority,
            "remote": cfg.remote_gamepad_priority,
        },
        timeout_sec=cfg.source_activity_timeout_sec,
    )
    # Lock is owned by the WS channel. It's also written by the local dongle.
    # Bridge (remote UDP) does NOT touch it.
    lock_state = {"locked": True}
    prev_state = {"speed_label": None}
    ws_state   = {"last_ai_mode": None, "last_bubble_mode": None}

    # ──────────────────────────────────────────────────────────────────────
    #  Dispatchers
    # ──────────────────────────────────────────────────────────────────────

    def _apply_lock_change(new_locked: bool, source_label: str) -> None:
        """Central place to fan out a lock edge."""
        if new_locked == lock_state["locked"]:
            return
        log("teleop",
            f"LOCK EDGE ({source_label}): "
            f"{lock_state['locked']} -> {new_locked}")
        lock_state["locked"] = new_locked
        if lights is not None:
            lights.set_robot_lock(new_locked)
        session_mgr.set_robot_lock(new_locked)
        if ptz is not None:
            # PTZ has its own lock independent of the drivetrain; mirror
            # the drivetrain lock so an operator can't pan while locked.
            ptz.set_ptz_unlock_state(not new_locked)

    # ── UDP motion dispatcher ───────────────────────────────────────────────

    def on_motion_packet(pkt: dict, addr, port: int) -> None:
        """Handles UDP packets from bridge-server (remote) and the local
        dongle (in-process). Bridge sends a trimmed schema without
        robot_lock; the local dongle sends the richer schema."""
        source = "local" if pkt.get("_local") else "remote"
        arbiter.report(source)
        if not arbiter.is_active(source):
            return

        # Lock: only the LOCAL dongle can toggle lock via UDP. The bridge
        # never sends robot_lock — lock over the network is WS-only.
        if source == "local":
            locked, lock_present = parse_lock_state(pkt, lock_state["locked"])
            if lock_present:
                _apply_lock_change(locked, "local dongle")

        locked_now = lock_state["locked"]

        lin   = first_float(pkt, ("lin_x", "linx", "linear_x"))
        ang   = first_float(pkt, ("ang_z", "angz", "angular_z"))
        brake = first_float(pkt, ("brake",), default=0.0) > cfg.brake_threshold

        motion.command(lin, ang, locked_now, brake, origin="human")

        # AI-enable chord from the local dongle. Bridge doesn't send this.
        if pkt.get("ai_request") == "enable":
            motion.set_ai_enabled(True)

        # PTZ head (both bridge and local dongle include this field).
        if ptz is not None:
            head = pkt.get("head")
            if head:
                ptz.command(str(head))

        # speed label (local dongle only) — passthrough for telemetry.
        speed_label = pkt.get("speed")
        if speed_label:
            shared["speed_label"] = speed_label
            if speed_label != prev_state["speed_label"]:
                if prev_state["speed_label"] is not None and ptz is not None:
                    ptz.capture_home()
                prev_state["speed_label"] = speed_label

    # ── Fleet WS dispatcher ─────────────────────────────────────────────────

    def on_ws_message(msg: dict) -> None:
        """Flat dict from the browser via streams.revobots.ai relay."""

        # 1) Lock — WS is authoritative for the network path.
        if "robot_lock" in msg:
            _apply_lock_change(truthy(msg["robot_lock"]), "ws")

        # 2) Drive over WS — protocol allows it (see util_receive_browser_cmds
        #    summarize()), current bridge doesn't send it, but this future-
        #    proofs browser drive buttons with no extra code. Watchdog in
        #    motion.py will zero if the browser stops sending.
        if "lin_x" in msg or "ang_z" in msg:
            lin = first_float(msg, ("lin_x",), default=0.0)
            ang = first_float(msg, ("ang_z",), default=0.0)
            brk = truthy(msg.get("brake", False))
            motion.command(lin, ang, lock_state["locked"], brk, origin="human")

        # 3) PTZ head from browser. Separate from UDP-side head — last write
        #    per tick wins in PtzController; acceptable because browser and
        #    dongle aren't typically active simultaneously.
        if "head" in msg and ptz is not None:
            head = msg["head"] or "center"
            ptz.command(str(head))

        # 4) speed_mode — label only, passthrough to telemetry.
        if "speed_mode" in msg:
            new_label = str(msg["speed_mode"])
            prev = shared.get("speed_label")
            shared["speed_label"] = new_label
            if prev is not None and prev != new_label and ptz is not None:
                ptz.capture_home()

        # 5) bubble_mode — toggle lidar safety brake at runtime.
        if "bubble_mode" in msg:
            on = truthy(msg["bubble_mode"])
            if on != ws_state["last_bubble_mode"]:
                ws_state["last_bubble_mode"] = on
                motion.set_lidar_block_enabled(on)

        # 6) ai_mode — direct on/off from browser (chord over gamepad also
        #    works, they're two independent paths to the same setter).
        if "ai_mode" in msg:
            on = truthy(msg["ai_mode"])
            if on != ws_state["last_ai_mode"]:
                ws_state["last_ai_mode"] = on
                motion.set_ai_enabled(on)

        # 7) high_visibility — reuse the lights all-on/all-off handler.
        if "high_visibility" in msg and lights is not None:
            on = truthy(msg["high_visibility"])
            lights.command({
                "type": "lights",
                "data": {"headlights": on, "parklights": on, "strobe": on},
            })

        # 8) TTS (browser sends type="stt") — speak + light blink.
        if msg.get("type") == "stt":
            text = str(msg.get("text", "") or "").strip()
            if text:
                if audio is not None:
                    audio.speak(text)
                if lights is not None:
                    lights.command({
                        "type": "talk",
                        "data": {"text": text,
                                 "duration": cfg.talk_default_duration},
                    })

        # 9) charging — passthrough / log only for now.
        if "charging" in msg:
            log("teleop", f"charging state from browser: {msg['charging']}")

        # 10) display_text / set_wallpaper — no display subsystem yet.
        if msg.get("type") in ("display_text", "set_wallpaper"):
            log("teleop", f"TODO {msg.get('type')}: {msg}")

    # ── Local dongle event dispatcher ──────────────────────────────────────

    def on_local_event(envelope: dict, addr, port: int) -> None:
        """Dispatcher for the local dongle's TCP-shape envelopes
        {seq, t, type, data}. Kept separate from on_ws_message because the
        envelope shape is richer (talk duration, music track, indicator
        side) and doesn't cleanly flatten to the browser's schema."""
        type_ = (envelope.get("type") or "").strip().lower()
        data  = envelope.get("data") or {}
        try:
            if type_ in ("lights", "indicator", "talk") and lights is not None:
                lights.command(envelope)
            if type_ == "talk" and audio is not None:
                text = data.get("text")
                if text:
                    audio.speak(str(text))
            if type_ == "audio" and audio is not None:
                vol = data.get("volume_pct")
                if vol is not None:
                    audio.set_volume(int(vol))
            if type_ == "music" and audio is not None:
                action = (data.get("action") or "").strip().lower()
                if action == "play":
                    track = data.get("track")
                    if track is not None:
                        audio.play_music(int(track))
            if type_ == "ptz" and ptz is not None:
                action = (data.get("action") or "").strip().lower()
                if action == "capture_home":
                    ptz.capture_home()
                elif action == "goto_home":
                    ptz.goto_home()
        except Exception as exc:
            log("teleop", f"local event handler error: {exc}")

    # ── Wire startup ────────────────────────────────────────────────────────

    udp_motion = UdpListener(cfg.udp_listen_ip, cfg.udp_motion_port, "motion",
                             on_motion_packet)
    udp_motion.start()

    bridge_ws = BridgeWsClient(
        url=cfg.fleet_ws_url_template.format(robot_id=cfg.robot_id),
        on_message=on_ws_message,
    )
    bridge_ws.start()

    heartbeat = HeartbeatPublisher(
        register_url=cfg.fleet_register_url,
        robot_id=cfg.robot_id,
        pilot_camera=cfg.fleet_pilot_camera,
        camera_names=cfg.fleet_camera_names,
        interval_sec=cfg.heartbeat_interval_sec,
        ip_fallback=cfg.tailscale_ip_fallback,
    )
    heartbeat.start()

    local = LocalGamepad(
        on_motion=on_motion_packet,
        on_events=on_local_event,
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
    # Stop network inputs first so no more browser/UDP messages arrive after
    # subsystems start tearing down.
    log("teleop", "shutting down…")
    session_mgr.stop()
    for name, sub in [
        ("udp_motion",    udp_motion),
        ("bridge_ws",     bridge_ws),
        ("heartbeat",     heartbeat),
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