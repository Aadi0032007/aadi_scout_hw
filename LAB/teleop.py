# -*- coding: utf-8 -*-
"""
REVO Scout LAB — camera, motion, GPS, recording, and streaming only.

Kept:
    - Camera capture
    - Motion UDP command handling
    - GPS reader
    - Session recording
    - Daily camera streaming

Removed:
    - Audio / TTS
    - Lights / signals / talk events
    - PTZ camera control
    - Local gamepad
    - IMU
    - Extra UDP event listeners

Important Daily streaming behavior:
    Daily's native SDK context can only be created once per process.
    So this file creates/starts DailyStream once, then only pauses/resumes
    publishing via stream.set_robot_lock(...). It does not recreate DailyStream
    on every lock/unlock cycle.
"""
from __future__ import annotations

import json
import signal
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Optional

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from LAB.cameras import MultiCameraCapture
from LAB.common import first_float, log, truthy
from LAB.config import LabConfig
from LAB.motion import MotionController
from LAB.record import SessionRecorder
from LAB.sensors import GpsReader
from LAB.stream import DailyStream


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


# ── Helpers ──────────────────────────────────────────────────────────────────

def parse_lock_state(pkt: dict, last_known_locked: bool) -> tuple[bool, bool]:
    """
    Return:
        locked, lock_field_present

    Missing robot_lock / lock keeps the previous lock state.
    """
    if "robot_lock" in pkt:
        return truthy(pkt["robot_lock"]), True

    if "lock" in pkt:
        return truthy(pkt["lock"]), True

    return last_known_locked, False


def start_recording_if_needed(recorder: SessionRecorder) -> None:
    try:
        if not recorder.is_active():
            recorder.set_robot_lock(False)
            recorder.start()
    except Exception as exc:
        log("teleop", f"recorder start error: {exc}")


def stop_recording_if_needed(recorder: SessionRecorder) -> None:
    try:
        recorder.set_robot_lock(True)
        if recorder.is_active():
            recorder.stop()
    except Exception as exc:
        log("teleop", f"recorder stop error: {exc}")


def set_stream_lock(stream: DailyStream, locked: bool) -> None:
    try:
        stream.set_robot_lock(bool(locked))
    except Exception as exc:
        log("teleop", f"stream robot_lock error: {exc}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = LabConfig.load_secrets(None)

    log("teleop", "=" * 60)
    log("teleop", f"cache_dir  = {cfg.cache_dir}")
    log("teleop", f"record_fps = {cfg.record_fps}")
    log("teleop", f"stream_fps = {cfg.stream_fps}")
    log("teleop", f"motion UDP = {cfg.udp_listen_ip}:{cfg.udp_motion_port}")
    log("teleop", "enabled    = cameras, motion, GPS, recording, streaming")
    log("teleop", "=" * 60)

    # ── Core subsystems ──────────────────────────────────────────────────────

    cameras = MultiCameraCapture.from_configs(cfg.cameras)

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

    # DailyStream is created and started exactly once. Do not recreate this
    # object after lock/unlock, because Daily's native context is singleton-like.
    stream = DailyStream(
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
        # Camera streaming only. Keeping this None prevents DailyStream from
        # trying to spawn ffmpeg for microphone audio.
        mic_rtsp_url=None,
        mic_rtsp_transport=cfg.mic_rtsp_transport,
        mic_sample_rate=cfg.mic_sample_rate,
        mic_channels=cfg.mic_channels,
        mic_frame_ms=cfg.mic_frame_ms,
        motion_state_fn=motion.state,
    )
    stream.set_robot_lock(True)
    stream.start()
    log("teleop", "stream started once; lock/unlock only pauses/resumes publishing")

    lock_state = {
        "locked": True,
    }

    # ── Dispatcher ───────────────────────────────────────────────────────────

    def on_motion_packet(pkt: dict, addr, port: int) -> None:
        locked, lock_present = parse_lock_state(pkt, lock_state["locked"])
        prev_locked = lock_state["locked"]

        if lock_present and locked != prev_locked:
            log(
                "teleop",
                f"LOCK EDGE addr={addr}: "
                f"{prev_locked} -> {locked} "
                f"raw_robot_lock={pkt.get('robot_lock', '<missing>')} "
                f"raw_lock={pkt.get('lock', '<missing>')}",
            )

        lock_state["locked"] = locked

        lin = first_float(pkt, ("lin_x", "linx", "linear_x"))
        ang = first_float(pkt, ("ang_z", "angz", "angular_z"))
        brake = first_float(pkt, ("brake",), default=0.0) > cfg.brake_threshold

        motion.command(lin, ang, locked, brake)

        # Your motion packet lock state remains the single source of truth.
        # Stream is never stopped/recreated here; it only receives lock state.
        if lock_present:
            set_stream_lock(stream, locked)

            if locked:
                stop_recording_if_needed(recorder)
            else:
                start_recording_if_needed(recorder)

        cam = pkt.get("camera") or pkt.get("cam") or pkt.get("video_source")
        if cam:
            try:
                stream.switch_source(str(cam))
            except Exception as exc:
                log("teleop", f"stream camera switch error: {exc}")

    # ── UDP listener ─────────────────────────────────────────────────────────

    udp_motion = UdpListener(
        cfg.udp_listen_ip,
        cfg.udp_motion_port,
        "motion",
        on_motion_packet,
    )
    udp_motion.start()

    # ── Signal handling ──────────────────────────────────────────────────────

    running = threading.Event()
    running.set()

    def on_signal(*_) -> None:
        running.clear()

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    log(
        "teleop",
        "ready — stream is single-instance; unlock starts recording; lock stops recording",
    )

    try:
        while running.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass

    # ── Shutdown ─────────────────────────────────────────────────────────────

    log("teleop", "shutting down…")

    try:
        recorder.set_robot_lock(True)
        recorder.stop()
    except Exception as exc:
        log("teleop", f"final recorder stop error: {exc}")

    for sub_name, sub in [
        ("udp_motion", udp_motion),
        ("stream", stream),
        ("motion", motion),
        ("gps", gps),
        ("cameras", cameras),
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
