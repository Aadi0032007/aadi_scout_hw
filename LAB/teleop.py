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


_DASHBOARD_EXPECTED_KEYS = (
    "speed_pct",
    "speed_mode",
    "robot_battery_pct",
    "box_temp_F",
    "humidity_pct",
    "cpu_temp_F",
    "gps_lat",
    "gps_lng",
    "gps_alt",
    "gps_orient",
    "gps_fix",
)


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
        # Highlight what's missing — dashboard columns without a source ready.
        missing = [k for k in _DASHBOARD_EXPECTED_KEYS if k not in snap]
        if missing:
            log("teleop", "── snapshot MISSING ──")
            for k in missing:
                log("teleop", f"    {k}  (no source ready yet)")
        else:
            log("teleop", "── snapshot: all expected keys present ──")
        log("teleop", "──────────────────────")
    threading.Thread(target=_run, daemon=True, name="snapshot-dump").start()


# ══════════════════════════════════════════════════════════════════════════════
#  WS "tts" and "ttd" payload handlers
# ══════════════════════════════════════════════════════════════════════════════
#
# The browser overloads two field names:
#
#   tts: "text: <words>"       → speech
#        "track: <file>.wav"   → music
#        "<words>"             → speech (free-form textbox, no prefix)
#
#   ttd: "img: <file>.png"     → wallpaper (no display subsystem yet)
#        "<words>"             → display text (no display subsystem yet)
#
# Prefix matching is case-insensitive and permissive: "text:hi", "text: hi",
# "TEXT :  hi" all work. Anything not matching a known prefix is treated as
# the free-form case.


_TTS_PREFIXES = ("text", "track")
_TTD_PREFIXES = ("img",)


def _split_prefix(raw: str, known: tuple) -> tuple[str, str]:
    """Return (prefix_lower, remainder) if the string starts with a known
    prefix followed by ':'. Otherwise ("", raw) meaning "free-form"."""
    if ":" not in raw:
        return "", raw
    head, tail = raw.split(":", 1)
    head_l = head.strip().lower()
    if head_l in known:
        return head_l, tail.strip()
    return "", raw


def _play_music_by_filename(audio, cfg, filename: str) -> None:
    """Map a filename from the browser to a track number in cfg.music_tracks,
    then call audio.play_music(N). Match is case-insensitive on basename."""
    import os as _os
    target = _os.path.basename(filename).strip().lower()
    for track_num, track_file in (cfg.music_tracks or {}).items():
        if _os.path.basename(track_file).strip().lower() == target:
            audio.play_music(int(track_num))
            return
    log("teleop",
        f"music track {filename!r} not in cfg.music_tracks — add it to play")


def _speak_and_blink(text: str, audio, lights, cfg) -> None:
    if not text:
        return
    if audio is not None:
        audio.speak(text)
    if lights is not None:
        lights.command({
            "type": "talk",
            "data": {"text": text, "duration": cfg.talk_default_duration},
        })


def _handle_tts(raw: str, audio, lights, cfg) -> None:
    if not raw:
        return
    prefix, body = _split_prefix(raw, _TTS_PREFIXES)
    if prefix == "text":
        _speak_and_blink(body, audio, lights, cfg)
    elif prefix == "track":
        if audio is not None and body:
            _play_music_by_filename(audio, cfg, body)
    else:
        # Free-form textbox — treat as speech.
        _speak_and_blink(raw, audio, lights, cfg)


def _handle_ttd(raw: str) -> None:
    if not raw:
        return
    prefix, body = _split_prefix(raw, _TTD_PREFIXES)
    if prefix == "img":
        log("teleop", f"ws ttd wallpaper={body!r} (TODO: no display subsystem)")
    else:
        log("teleop", f"ws ttd display_text={raw!r} (TODO: no display subsystem)")


def _log_tts_intent(raw: str) -> None:
    """One-line describe-what-this-tts-message-will-do log."""
    if not raw:
        log("teleop", "ws tts (empty)")
        return
    prefix, body = _split_prefix(raw, _TTS_PREFIXES)
    if prefix == "text":
        preview = body if len(body) <= 60 else body[:57] + "..."
        log("teleop", f"ws tts speak={preview!r}")
    elif prefix == "track":
        log("teleop", f"ws tts music={body!r}")
    else:
        preview = raw if len(raw) <= 60 else raw[:57] + "..."
        log("teleop", f"ws tts speak(free-form)={preview!r}")


# ══════════════════════════════════════════════════════════════════════════════
#  UDP flow tracker — one-line status transitions, no per-packet noise
# ══════════════════════════════════════════════════════════════════════════════


class UdpFlowTracker:
    """Tracks whether UDP motion packets are actively arriving from a given
    source label. Logs exactly two transitions:

        [teleop] UDP motion FLOWING from <source> (<addr>)
        [teleop] UDP motion STOPPED (no packet for <n>s)

    A background watcher thread promotes the "stopped" edge; we don't wait
    for the next packet to notice silence. Independent of motion.py's
    per-tick watchdog — this is purely for human-visible logging.
    """

    def __init__(self, label: str, stall_sec: float = 1.0) -> None:
        self._label = label
        self._stall_sec = stall_sec
        self._lock = threading.Lock()
        self._last_t = 0.0
        self._last_addr: Optional[tuple] = None
        self._flowing = False
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._watch, daemon=True, name=f"udp-flow-{label}"
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def observe(self, addr: tuple) -> None:
        """Call once per received UDP packet from this source."""
        with self._lock:
            was_flowing = self._flowing
            self._last_t = now_mono()
            self._last_addr = addr
            self._flowing = True
        if not was_flowing:
            log("teleop",
                f"UDP motion FLOWING from {self._label} ({addr[0]}:{addr[1]})")

    def _watch(self) -> None:
        while not self._stop.is_set():
            self._stop.wait(timeout=0.25)
            with self._lock:
                if not self._flowing:
                    continue
                age = now_mono() - self._last_t
                if age > self._stall_sec:
                    self._flowing = False
                    addr = self._last_addr
            log("teleop",
                f"UDP motion STOPPED "
                f"(no packet from {self._label} for {age:.1f}s"
                + (f", last={addr[0]}:{addr[1]}" if addr else "")
                + ")")


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
    ws_state   = {
        "last_ai_mode":     None,
        "last_bubble_mode": None,
        "last_xwalk":       None,
        "last_yield":       None,
        "last_volume":      None,
    }

    # UDP flow trackers — one per source label. Independent of motion.py's
    # per-tick watchdog, purely for human-visible transition logging.
    udp_flow_remote = UdpFlowTracker("remote (bridge)")
    udp_flow_local  = UdpFlowTracker("local dongle")
    udp_flow_remote.start()
    udp_flow_local.start()

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
        # Log flow transitions only, not per-packet spam.
        (udp_flow_local if source == "local" else udp_flow_remote).observe(addr)
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
        """Flat dict from the browser via streams.revobots.ai relay.

        Real observed schema (2026-07 browser build):

          Speech / audio:
            {"tts": "text: <words>"}         speak text (prefixed)
            {"tts": "<words>"}               speak text (free-form textbox)
            {"tts": "track: <file>.wav"}     play music by filename
            {"volume": 0..100}               audio volume percent
                                             (browser fires twice per change)

          Display (no subsystem yet — log only):
            {"ttd": "img: <file>.png"}       set wallpaper by filename
            {"ttd": "<words>"}               display text (free-form textbox)

          Drivetrain:
            {"lock": 0|1}                    0=unlocked, 1=locked
            {"lin_x": float}                 drive; sent alone, not paired
            {"ang_z": float}                 drive; sent alone, not paired
            {"speed_mode": "slow|medium|fast"}   label passthrough

          Feature toggles:
            {"AI": 0|1}                      ai mode on/off
            {"bubble": 0|1}                  lidar safety brake on/off
            {"xwalk": 0|1}                   crosswalk mode (subsystem TBD)
            {"yield": 0|1}                   yield mode (subsystem TBD)
            {"left_turn": 0|1}               left turn signal (via lights)
            {"right_turn": 0|1}              right turn signal (via lights)
            {"head": "left|right|up|down|center"}    PTZ direction

        Every message carries {"seq", "t", "brain"} envelope fields.

        Each button click sends ONLY the changed field — drive fields never
        arrive paired, so lin_x and ang_z are handled independently. Missing
        axis is kept at its current value; motion.py's watchdog zeros both
        on WS silence.
        """
        # ── Lock — WS is authoritative ──────────────────────────────────────
        if "lock" in msg:
            _apply_lock_change(truthy(msg["lock"]), "ws")

        # ── Drive (single-axis at a time) ───────────────────────────────────
        if "lin_x" in msg or "ang_z" in msg:
            cur_lin, cur_ang_scaled = motion.published_state()
            cur_ang = (cur_ang_scaled / cfg.ang_z_scale
                       if cfg.ang_z_scale else 0.0)
            lin = first_float(msg, ("lin_x",), default=cur_lin)
            ang = first_float(msg, ("ang_z",), default=cur_ang)
            motion.command(lin, ang, lock_state["locked"], False,
                           origin="human")
            if "lin_x" in msg:
                log("teleop", f"ws drive lin_x={lin:+.3f}")
            if "ang_z" in msg:
                log("teleop", f"ws drive ang_z={ang:+.3f}")

        # ── PTZ head ────────────────────────────────────────────────────────
        if "head" in msg and ptz is not None:
            head_val = str(msg["head"] or "center")
            ptz.command(head_val)
            log("teleop", f"ws ptz head={head_val}")

        # ── speed_mode — label passthrough ──────────────────────────────────
        if "speed_mode" in msg:
            new_label = str(msg["speed_mode"])
            prev = shared.get("speed_label")
            shared["speed_label"] = new_label
            log("teleop", f"ws speed_mode={new_label}")
            if prev is not None and prev != new_label and ptz is not None:
                ptz.capture_home()

        # ── bubble → lidar safety brake ─────────────────────────────────────
        if "bubble" in msg:
            on = truthy(msg["bubble"])
            if on != ws_state["last_bubble_mode"]:
                ws_state["last_bubble_mode"] = on
                motion.set_lidar_block_enabled(on)   # logs internally

        # ── AI mode ─────────────────────────────────────────────────────────
        if "AI" in msg:
            on = truthy(msg["AI"])
            if on != ws_state["last_ai_mode"]:
                ws_state["last_ai_mode"] = on
                motion.set_ai_enabled(on)   # logs internally

        # ── xwalk — log-only until subsystem is defined ────────────────────
        if "xwalk" in msg:
            on = truthy(msg["xwalk"])
            if on != ws_state["last_xwalk"]:
                ws_state["last_xwalk"] = on
                log("teleop", f"ws xwalk={on} (no subsystem wired yet)")

        # ── yield — log-only until subsystem is defined ─────────────────────
        if "yield" in msg:
            on = truthy(msg["yield"])
            if on != ws_state["last_yield"]:
                ws_state["last_yield"] = on
                log("teleop", f"ws yield={on} (no subsystem wired yet)")

        # ── left_turn / right_turn — turn signal indicators ────────────────
        if "left_turn" in msg and lights is not None:
            on = truthy(msg["left_turn"])
            side = "left" if on else "center"
            lights.command({"type": "indicator", "data": {"side": side}})
            log("teleop", f"ws left_turn={'ON' if on else 'off'}")
        if "right_turn" in msg and lights is not None:
            on = truthy(msg["right_turn"])
            side = "right" if on else "center"
            lights.command({"type": "indicator", "data": {"side": side}})
            log("teleop", f"ws right_turn={'ON' if on else 'off'}")

        # ── volume ──────────────────────────────────────────────────────────
        if "volume" in msg and audio is not None:
            try:
                vol = int(msg["volume"])
            except (TypeError, ValueError):
                vol = None
            if vol is not None and vol != ws_state["last_volume"]:
                ws_state["last_volume"] = vol
                audio.set_volume(vol)   # logs internally

        # ── tts (speech or music) ───────────────────────────────────────────
        if "tts" in msg:
            raw = str(msg["tts"] or "").strip()
            _log_tts_intent(raw)
            _handle_tts(raw, audio, lights, cfg)

        # ── ttd (wallpaper or display text) ─────────────────────────────────
        if "ttd" in msg:
            raw = str(msg["ttd"] or "").strip()
            _handle_ttd(raw)

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
    udp_flow_remote.stop()
    udp_flow_local.stop()
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