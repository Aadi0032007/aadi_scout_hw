# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations

"""
REVO Scout LAB — unified controller and recorder.

CURRENT HARDWARE PROFILE:
    enabled : motion, cameras, GPS, recorder, stream, local gamepad
    DISABLED: lights, PTZ, audio   (hardware not yet connected)

To re-enable any of those three subsystems, search this file for the
marker  RE-ENABLE-WHEN-HARDWARE-INSTALLED  and uncomment the block(s)
under each one. The dispatchers below check for None before calling so
the order in which you re-enable doesn't matter.

Production behavior (unchanged):
    Local gamepad: always enabled, evdev-based, event-driven.
    Stable unlock: start stream + start fresh recording session.
    Stable lock:   stop/finalize recording + stop stream.
    Missing robot_lock/lock: keep previous lock state.
    Source priority: local higher than remote.
"""

import json
import signal
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Callable, Optional

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

# ── RE-ENABLE-WHEN-HARDWARE-INSTALLED: audio, lights, PTZ imports ──────────
# from LAB.audio         import AudioController
# from LAB.lights        import LightsController
# from LAB.ptz           import PtzController

from LAB.cameras       import MultiCameraCapture
from LAB.common        import first_float, log, now_mono, truthy
from LAB.config        import LabConfig
from LAB.local_gamepad import LocalGamepad
from LAB.motion        import MotionController
from LAB.record        import SessionRecorder
from LAB.sensors       import GpsReader, ImuReader
from LAB.stream        import DailyStream


# ── UDP listener ──────────────────────────────────────────────────────────────

class UdpListener(threading.Thread):
    def __init__(self, host: str, port: int, label: str, on_packet) -> None:
        super().__init__(daemon=True, name=f"udp-{label}")
        self._host = host
        self._port = port
        self._label = label
        self._on_packet = on_packet
        self._stop = threading.Event()
        self._sock: Optional[socket.socket] = None

    def run(self) -> None:
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._sock.bind((self._host, self._port))
            self._sock.settimeout(0.2)
        except OSError as exc:
            log("teleop", f"UDP bind failed {self._host}:{self._port} ({self._label}): {exc}")
            return

        log("teleop", f"UDP listener {self._label} on {self._host}:{self._port}")

        while not self._stop.is_set():
            try:
                data, addr = self._sock.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError:
                break

            try:
                pkt = json.loads(data.decode("utf-8", errors="replace"))
                if not isinstance(pkt, dict):
                    continue
            except json.JSONDecodeError:
                continue

            try:
                self._on_packet(pkt, addr, self._port)
            except Exception as exc:
                log("teleop", f"{self._label} dispatch error: {exc}")

        try:
            if self._sock is not None:
                self._sock.close()
        except Exception:
            pass

    def stop(self) -> None:
        self._stop.set()


# ── Source arbitration ────────────────────────────────────────────────────────

class SourceArbiter:
    """Lower priority number wins. local=100, remote=200."""

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


# ── Helpers ──────────────────────────────────────────────────────────────────

def parse_lock_state(pkt: dict, last_known_locked: bool) -> tuple[bool, bool]:
    if "robot_lock" in pkt:
        return truthy(pkt["robot_lock"]), True
    if "lock" in pkt:
        return truthy(pkt["lock"]), True
    return last_known_locked, False


# ── Stream + recording manager (unchanged) ───────────────────────────────────

class SessionAndStreamManager(threading.Thread):
    def __init__(
        self,
        recorder: SessionRecorder,
        stream_factory: Callable[[], DailyStream],
        debounce_sec: float = 0.75,
    ) -> None:
        super().__init__(daemon=True, name="session-stream-manager")
        self._recorder = recorder
        self._stream_factory = stream_factory
        self._debounce_sec = debounce_sec

        self._cv = threading.Condition()
        self._stop_thread = False

        self._desired_locked = True
        self._applied_locked = True
        self._last_change = time.monotonic()

        self._stream: Optional[DailyStream] = None
        self._stream_running = False
        self._pending_camera: Optional[str] = None

    def set_robot_lock(self, locked: bool) -> None:
        locked = bool(locked)
        with self._cv:
            if locked == self._desired_locked:
                return
            log("teleop", f"manager desired lock change: {self._desired_locked} -> {locked}")
            self._desired_locked = locked
            self._last_change = time.monotonic()
            self._cv.notify()

    def switch_source(self, source_name: str) -> None:
        if not source_name:
            return
        with self._cv:
            self._pending_camera = str(source_name)
            stream = self._stream if self._stream_running else None
        if stream is not None:
            try:
                stream.switch_source(str(source_name))
            except Exception as exc:
                log("teleop", f"stream camera switch error: {exc}")

    def set_stream_robot_lock(self, locked: bool) -> None:
        with self._cv:
            stream = self._stream if self._stream_running else None
        if stream is not None:
            try:
                stream.set_robot_lock(bool(locked))
            except Exception:
                pass

    def run(self) -> None:
        while True:
            with self._cv:
                if self._stop_thread:
                    break
                if self._desired_locked == self._applied_locked:
                    self._cv.wait(timeout=0.25)
                    continue

                stable_for = time.monotonic() - self._last_change
                wait_for = self._debounce_sec - stable_for
                if wait_for > 0:
                    self._cv.wait(timeout=wait_for)
                    continue

                target_locked = self._desired_locked

            try:
                if target_locked:
                    self._apply_locked()
                else:
                    self._apply_unlocked()
            except Exception as exc:
                log("teleop", f"session/stream manager error: {exc}")

            with self._cv:
                self._applied_locked = target_locked

    def _apply_unlocked(self) -> None:
        log("teleop", "stable unlock — starting stream + new recording session")
        try:
            if not self._stream_running:
                stream = self._stream_factory()
                stream.set_robot_lock(False)
                with self._cv:
                    pending_camera = self._pending_camera
                if pending_camera:
                    try:
                        stream.switch_source(pending_camera)
                    except Exception as exc:
                        log("teleop", f"initial stream camera switch error: {exc}")
                stream.start()
                with self._cv:
                    self._stream = stream
                    self._stream_running = True
        except Exception as exc:
            log("teleop", f"stream start error: {exc}")

        try:
            if not self._recorder.is_active():
                self._recorder.set_robot_lock(False)
                self._recorder.start()
        except Exception as exc:
            log("teleop", f"recorder start error: {exc}")

    def _apply_locked(self) -> None:
        log("teleop", "stable lock — stopping recording + stream")
        try:
            self._recorder.set_robot_lock(True)
            self._recorder.stop()
        except Exception as exc:
            log("teleop", f"recorder stop error: {exc}")

        stream_to_stop: Optional[DailyStream] = None
        with self._cv:
            if self._stream_running and self._stream is not None:
                stream_to_stop = self._stream
            self._stream = None
            self._stream_running = False

        if stream_to_stop is not None:
            try:
                stream_to_stop.set_robot_lock(True)
            except Exception:
                pass
            try:
                stream_to_stop.stop()
            except Exception as exc:
                log("teleop", f"stream stop error: {exc}")

    def stop(self) -> None:
        with self._cv:
            self._stop_thread = True
            self._cv.notify()

        try:
            self.join(timeout=2.0)
        except Exception:
            pass

        try:
            self._recorder.set_robot_lock(True)
            self._recorder.stop()
        except Exception as exc:
            log("teleop", f"final recorder stop error: {exc}")

        stream_to_stop: Optional[DailyStream] = None
        with self._cv:
            if self._stream_running and self._stream is not None:
                stream_to_stop = self._stream
            self._stream = None
            self._stream_running = False

        if stream_to_stop is not None:
            try:
                stream_to_stop.stop()
            except Exception as exc:
                log("teleop", f"final stream stop error: {exc}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = LabConfig.load_secrets(None)

    log("teleop", "=" * 60)
    log("teleop", f"cache_dir   = {cfg.cache_dir}")
    log("teleop", f"record_fps  = {cfg.record_fps}")
    log("teleop", f"stream_fps  = {cfg.stream_fps}")
    log(
        "teleop",
        f"ports       = motion:{cfg.udp_motion_port} "
        f"events:{cfg.udp_events_port} tts:{cfg.udp_tts_port}",
    )
    log("teleop", "local_gamepad = always enabled")
    log("teleop", "disabled subsystems: lights, PTZ, audio (hardware not connected)")
    log("teleop", "=" * 60)

    # ── Core subsystems ──────────────────────────────────────────────────────

    cameras = MultiCameraCapture.from_configs(cfg.cameras)

    # imu = ImuReader(port=cfg.imu_port_hint, baud=cfg.imu_baud); imu.start()
    imu = None

    gps = GpsReader(udp_host=cfg.gps_udp_host, udp_port=cfg.gps_udp_port)
    gps.start()

    motion = MotionController(
        docker_host=cfg.docker_motion_host,
        docker_port=cfg.docker_motion_port,
        publish_hz=cfg.motion_publish_hz,
        watchdog_sec=cfg.motion_watchdog_sec,
        ang_z_scale=cfg.ang_z_scale,
    )
    motion.start()

    # ── RE-ENABLE-WHEN-HARDWARE-INSTALLED: audio ────────────────────────────
    audio = None
    # audio = AudioController(
    #     piper_model=cfg.piper_model,
    #     music_dir=cfg.music_dir,
    #     music_tracks=cfg.music_tracks,
    #     startup_volume_pct=cfg.startup_volume_pct,
    #     preferred_sink_patterns=cfg.preferred_sink_patterns,
    #     preferred_source_patterns=cfg.preferred_source_patterns,
    #     piper_speaker_id=cfg.piper_speaker_id,
    # )
    # audio.start()

    # ── RE-ENABLE-WHEN-HARDWARE-INSTALLED: lights ───────────────────────────
    lights = None
    # lights = LightsController(
    #     blink_period_sec=cfg.blink_period_sec,
    #     signal_timeout_sec=cfg.signal_timeout_sec,
    #     talk_default_duration=cfg.talk_default_duration,
    #     all_lights_cooldown_sec=cfg.all_lights_cooldown_sec,
    #     all_lights_blink_sec=cfg.all_lights_blink_sec,
    # )
    # lights.start()

    # ── RE-ENABLE-WHEN-HARDWARE-INSTALLED: PTZ ──────────────────────────────
    ptz = None
    # ptz = PtzController(
    #     ip=cfg.ptz_ip,
    #     port=cfg.ptz_port,
    #     user=cfg.ptz_user,
    #     password=cfg.ptz_password or "",
    #     pan_speed=cfg.ptz_pan_speed,
    #     tilt_speed=cfg.ptz_tilt_speed,
    #     loop_hz=cfg.ptz_loop_hz,
    #     deadband_sec=cfg.ptz_deadband_sec,
    #     stop_after_sec=cfg.ptz_stop_after_sec,
    # )
    # ptz.start()
    # ptz.set_ptz_unlock_state(True)

    # ── Stream factory ───────────────────────────────────────────────────────

    def make_stream() -> DailyStream:
        return DailyStream(
            api_key=cfg.daily_api_key,
            room_url=cfg.daily_room_url,
            room_name=cfg.daily_room_name,
            width=cfg.stream_width,
            height=cfg.stream_height,
            fps=cfg.stream_fps,
            cameras=cameras,
            name_aliases=cfg.camera_name_aliases,
            initial_main_source=cfg.initial_main_source,
            pip_enabled=cfg.pip_enabled,
            pip_left_source=cfg.pip_left_source,
            pip_right_source=cfg.pip_right_source,
            pip_width=cfg.pip_width,
            pip_height=cfg.pip_height,
            pip_margin=cfg.pip_margin,
            pip_gap=cfg.pip_gap,
            pip_stale_sec=cfg.pip_stale_sec,
            pip_show_label=cfg.pip_show_label,
            overlay_speed_badge=cfg.overlay_speed_badge,
            overlay_camera_name=cfg.overlay_camera_name,
            overlay_timestamp=cfg.overlay_timestamp,
            mic_rtsp_url=cfg.mic_rtsp_url,
            mic_rtsp_transport=cfg.mic_rtsp_transport,
            mic_sample_rate=cfg.mic_sample_rate,
            mic_channels=cfg.mic_channels,
            mic_frame_ms=cfg.mic_frame_ms,
            motion_state_fn=motion.state,
        )

    # ── Recorder ─────────────────────────────────────────────────────────────

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
        gps_get_fn=gps.get,
    )
    recorder.set_robot_lock(True)

    session_stream_manager = SessionAndStreamManager(
        recorder=recorder,
        stream_factory=make_stream,
        debounce_sec=0.75,
    )
    session_stream_manager.start()

    # ── Source arbitration ───────────────────────────────────────────────────

    arbiter = SourceArbiter(
        priorities={
            "local": cfg.local_dongle_priority,
            "remote": cfg.remote_gamepad_priority,
        },
        timeout_sec=cfg.source_activity_timeout_sec,
    )

    prev_state = {
        "a_pressed":   False,
        "b_pressed":   False,
        "button_8":    False,
        "speed_label": None,
    }

    lock_state = {"locked": True}

    # ── Dispatchers ──────────────────────────────────────────────────────────

    def on_motion_packet(pkt: dict, addr, port: int) -> None:
        source = "local" if pkt.get("_local") else "remote"

        arbiter.report(source)
        if not arbiter.is_active(source):
            return

        locked, lock_present = parse_lock_state(pkt, lock_state["locked"])

        if lock_present and locked != lock_state["locked"]:
            log(
                "teleop",
                f"LOCK EDGE source={source} addr={addr}: "
                f"{lock_state['locked']} -> {locked} "
                f"raw_robot_lock={pkt.get('robot_lock', '<missing>')} "
                f"raw_lock={pkt.get('lock', '<missing>')} "
                f"active={arbiter.active()}",
            )

        lock_state["locked"] = locked

        lin = first_float(pkt, ("lin_x", "linx", "linear_x"))
        ang = first_float(pkt, ("ang_z", "angz", "angular_z"))
        brake = first_float(pkt, ("brake",), default=0.0) > cfg.brake_threshold

        motion.command(lin, ang, locked, brake)

        # ── RE-ENABLE-WHEN-HARDWARE-INSTALLED: lights edge updates ──────
        if lights is not None:
            lights.set_robot_lock(locked)

        session_stream_manager.set_stream_robot_lock(locked)

        if lock_present:
            session_stream_manager.set_robot_lock(locked)

        cam = pkt.get("camera") or pkt.get("cam") or pkt.get("video_source")
        if cam:
            session_stream_manager.switch_source(str(cam))

        # ── RE-ENABLE-WHEN-HARDWARE-INSTALLED: PTZ head + home buttons ──
        if ptz is not None:
            head = pkt.get("head")
            if head:
                ptz.command(str(head))

            speed_label = pkt.get("speed")
            if speed_label and speed_label != prev_state["speed_label"]:
                if prev_state["speed_label"] is not None:
                    ptz.capture_home()
                prev_state["speed_label"] = speed_label

            a_pressed = truthy(pkt.get("a", False)) or pkt.get("button") == 1
            b_pressed = truthy(pkt.get("b", False)) or pkt.get("button") == 2
            button_8 = pkt.get("button") == cfg.ptz_home_button

            ab_combo = a_pressed and b_pressed
            prev_ab = prev_state["a_pressed"] and prev_state["b_pressed"]

            if ab_combo and not prev_ab:
                ptz.capture_home()

            if button_8 and not prev_state["button_8"]:
                ptz.goto_home()

            prev_state["a_pressed"] = a_pressed
            prev_state["b_pressed"] = b_pressed
            prev_state["button_8"]  = button_8

    def on_events_packet(pkt: dict, addr, port: int) -> None:
        event = (pkt.get("event") or "").strip().lower()

        # ── RE-ENABLE-WHEN-HARDWARE-INSTALLED: lights/audio events ──────
        if event in ("lights", "signals", "talk"):
            if lights is not None:
                lights.command(pkt)

        if event == "audio" and audio is not None:
            vol = pkt.get("volume_pct")
            if vol is not None:
                audio.set_volume(int(vol))

        if event == "music" and audio is not None:
            action = (pkt.get("action") or "").strip().lower()
            if action in ("play", "pla2"):
                track = pkt.get("track")
                if track is not None:
                    audio.play_music(int(track))

    def on_tts_packet(pkt: dict, addr, port: int) -> None:
        # ── RE-ENABLE-WHEN-HARDWARE-INSTALLED: audio TTS ────────────────
        if audio is None:
            return
        if pkt.get("type") == "stt":
            text = pkt.get("text", "")
            if text:
                audio.speak(str(text))

    # ── UDP listeners ────────────────────────────────────────────────────────

    udp_motion = UdpListener(cfg.udp_listen_ip, cfg.udp_motion_port, "motion", on_motion_packet)
    udp_events = UdpListener(cfg.udp_listen_ip, cfg.udp_events_port, "events", on_events_packet)
    udp_tts    = UdpListener(cfg.udp_listen_ip, cfg.udp_tts_port,    "tts",    on_tts_packet)

    udp_motion.start()
    udp_events.start()
    udp_tts.start()

    # ── Local gamepad always on ──────────────────────────────────────────────

    local = LocalGamepad(
        on_motion=on_motion_packet,
        on_events=on_events_packet,
        on_tts=on_tts_packet,
        initial_robot_lock=True,
        priority_value=cfg.local_dongle_priority,
    )
    local.start()
    log("teleop", "local gamepad started")

    # ── Signal handling ──────────────────────────────────────────────────────

    running = threading.Event()
    running.set()

    def on_signal(*_) -> None:
        running.clear()

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    log("teleop", "ready — local always on; stable unlock starts stream+recording; stable lock stops both")

    try:
        while running.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass

    # ── Shutdown ─────────────────────────────────────────────────────────────

    log("teleop", "shutting down…")

    try:
        session_stream_manager.stop()
    except Exception as exc:
        log("teleop", f"session/stream manager stop error: {exc}")

    for sub_name, sub in [
        ("udp_motion", udp_motion),
        ("udp_events", udp_events),
        ("udp_tts",    udp_tts),
        ("local",      local),
        ("ptz",        ptz),       # None today, harmless
        ("lights",     lights),    # None today, harmless
        ("audio",      audio),     # None today, harmless
        ("motion",     motion),
        # ("imu",      imu),
        ("gps",        gps),
        ("cameras",    cameras),
    ]:
        if sub is None:
            continue

        try:
            if hasattr(sub, "stop"):
                sub.stop()
            elif hasattr(sub, "stop_all"):
                sub.stop_all()
        except Exception as exc:
            log("teleop", f"{sub_name} stop error: {exc}")

    log("teleop", "done.")


if __name__ == "__main__":
    main()