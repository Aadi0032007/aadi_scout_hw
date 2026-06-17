# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations


"""
Daily streaming with PiP and badges.

Joins a Daily.co room and publishes:
    - One virtual camera at stream_width × stream_height @ stream_fps
    - One virtual microphone (RTSP audio from the orbital camera)

Each outgoing frame is composed:
    1. Pull latest frame from the current "main" camera source
    2. Pull latest frames from the two thumbnail sources (pilot + rear)
    3. Composite thumbnails as picture-in-picture overlays
    4. Draw badges: speed, camera-name, optional timestamp
    5. Convert BGR → RGB
    6. Push to Daily

robot_lock=True stops video and audio publishing entirely so the operator
sees a frozen frame as visual confirmation of lock state. Same as original.
"""

import json
import subprocess
import sys
import threading
import time
from typing import Callable, Optional

import cv2
import numpy as np
import requests

from .common import log

# Global import for EventHandler so the _CallHandler class can inherit from it
try:
    from daily import EventHandler
except ImportError:
    EventHandler = object


class DailyStream:
    def __init__(
        self,
        api_key:           str,
        room_url:          str,
        room_name:         str,
        width:             int,
        height:            int,
        fps:               int,
        cameras,                                # MultiCameraCapture
        name_aliases:      dict,
        initial_main_source: str,
        pip_enabled:       bool,
        pip_left_source:   str,
        pip_right_source:  str,
        pip_width:         int,
        pip_height:        int,
        pip_margin:        int,
        pip_gap:           int,
        pip_stale_sec:     float,
        pip_show_label:    bool,
        overlay_speed_badge:   bool,
        overlay_camera_name:   bool,
        overlay_timestamp:     bool,
        mic_rtsp_url:      str,
        mic_rtsp_transport: str,
        mic_sample_rate:   int,
        mic_channels:      int,
        mic_frame_ms:      int,
        motion_state_fn:   Optional[Callable[[], tuple]] = None,
        temphum_get_fn:    Optional[Callable[[], dict]] = None,
        overlay_temphum:   bool = False,
        temphum_temp_yellow_f: float = 70.0,
        temphum_temp_red_f:    float = 90.0,
        temphum_stale_after_sec: float = 10.0,
    ) -> None:
        self._api_key      = api_key
        self._room_url     = room_url
        self._room_name    = room_name
        self._width        = width
        self._height       = height
        self._fps          = max(1, fps)
        self._cameras      = cameras
        self._aliases      = {k.strip().lower(): v for k, v in (name_aliases or {}).items()}
        self._motion_state = motion_state_fn      # () → (lin_x, ang_z, locked, braking)

        # PiP / overlay config
        self._pip_enabled       = pip_enabled
        self._pip_left          = pip_left_source
        self._pip_right         = pip_right_source
        self._pip_w             = pip_width
        self._pip_h             = pip_height
        self._pip_margin        = pip_margin
        self._pip_gap           = pip_gap
        self._pip_stale_sec     = pip_stale_sec
        self._pip_show_label    = pip_show_label
        self._show_speed_badge  = overlay_speed_badge
        self._show_cam_name     = overlay_camera_name
        self._show_timestamp    = overlay_timestamp

        # TempHum overlay
        self._temphum_get_fn     = temphum_get_fn
        self._show_temphum       = overlay_temphum and (temphum_get_fn is not None)
        self._temphum_yellow_f   = float(temphum_temp_yellow_f)
        self._temphum_red_f      = float(temphum_temp_red_f)
        self._temphum_stale_sec  = float(temphum_stale_after_sec)

        # Mic
        self._mic_rtsp_url       = mic_rtsp_url
        self._mic_rtsp_transport = mic_rtsp_transport
        self._mic_sample_rate    = mic_sample_rate
        self._mic_channels       = mic_channels
        self._mic_frame_ms       = mic_frame_ms

        # State
        self._current_source: Optional[str] = None
        self._source_lock = threading.Lock()
        self._initial_main_source = initial_main_source
        self._robot_locked = False

        # Daily handles
        self._client     = None
        self._cam_device = None
        self._mic_device = None

        # Mic ffmpeg subprocess
        self._mic_proc: Optional[subprocess.Popen] = None
        self._mic_thread: Optional[threading.Thread] = None

        # Stream loop
        self._stop = threading.Event()
        self._stream_thread = threading.Thread(target=self._stream_loop, daemon=True, name="daily-stream")

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        if not self._api_key or not self._room_url:
            log("stream", "DAILY_API_KEY or DAILY_ROOM_URL missing — streaming disabled")
            return

        try:
            from daily import Daily, CallClient   # type: ignore

            token = self._get_token()
            Daily.init()

            self._cam_device = Daily.create_camera_device(
                "lab-virtual-cam", self._width, self._height, "RGB",
            )
            if self._mic_rtsp_url:
                self._mic_device = Daily.create_microphone_device(
                    "lab-virtual-mic",
                    sample_rate=self._mic_sample_rate,
                    channels=self._mic_channels,
                    non_blocking=True,
                )

            handler = _CallHandler()
            self._client = CallClient(event_handler=handler)

            mic_input = {
                "isEnabled": self._mic_device is not None,
                "settings":  {"deviceId": self._mic_device.name} if self._mic_device else {},
            }
            self._client.join(
                self._room_url,
                meeting_token=token,
                client_settings={
                    "inputs": {
                        "camera":     {"isEnabled": True, "settings": {"deviceId": self._cam_device.name}},
                        "microphone": mic_input,
                    },
                    "publishing": {
                        "camera":     {"isPublishing": True},
                        "microphone": {"isPublishing": self._mic_device is not None},
                    },
                },
            )

            if not handler.joined.wait(timeout=30):
                log("stream", "did not join Daily within 30s")
                return
            log("stream", f"joined room {self._room_url}")

            # Pick initial source — prefer configured, fall back to first available
            names = self._cameras.names()
            with self._source_lock:
                if self._initial_main_source and self._cameras.has(self._initial_main_source):
                    self._current_source = self._initial_main_source
                elif names:
                    self._current_source = names[0]

            if self._mic_device and self._mic_rtsp_url:
                self._start_mic()

            self._stream_thread.start()

        except ImportError:
            log("stream", "daily-python not installed — pip install daily-python")
        except Exception as exc:
            log("stream", f"start failed: {exc}")

    def stop(self) -> None:
        self._stop.set()
        try:
            self._stream_thread.join(timeout=2.0)
        except Exception:
            pass
        self._stop_mic()
        if self._client is not None:
            try:
                self._client.leave()
                self._client.release()
                self._client.destroy()
            except Exception:
                pass

    # ── public API ────────────────────────────────────────────────────────────

    def switch_source(self, name: str) -> None:
        """Switch the main camera. Accepts aliased names (e.g. 'pilot' → 'orbital')."""
        if not name:
            return
        target = self._resolve_alias(name)
        if not self._cameras.has(target):
            return
        with self._source_lock:
            if self._current_source != target:
                self._current_source = target
                log("stream", f"main → {target}")

    def set_robot_lock(self, locked: bool) -> None:
        """When True, freeze the published frame (last frame held)."""
        if locked != self._robot_locked:
            log("stream", f"robot_lock={'ON (publishing paused)' if locked else 'OFF'}")
        self._robot_locked = locked

    # ── alias resolution ─────────────────────────────────────────────────────

    def _resolve_alias(self, name: str) -> str:
        n = name.strip().lower()
        return self._aliases.get(n, n)

    # ── main streaming loop ───────────────────────────────────────────────────

    def _stream_loop(self) -> None:
        interval = 1.0 / self._fps
        next_tick = time.monotonic()
        last_frame: Optional[np.ndarray] = None

        while not self._stop.is_set():
            sleep_for = next_tick - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)
            next_tick = time.monotonic() + interval

            if self._cam_device is None:
                continue

            # When locked, hold the last published frame
            if self._robot_locked and last_frame is not None:
                self._push_rgb(last_frame)
                continue

            with self._source_lock:
                src = self._current_source
            if src is None:
                continue

            _, frame = self._cameras.read(src)
            if frame is None:
                continue

            composed = self._compose_frame(frame, src)
            last_frame = composed
            self._push_rgb(composed)

    def _push_rgb(self, frame_bgr: np.ndarray) -> None:
        try:
            if frame_bgr.shape[1] != self._width or frame_bgr.shape[0] != self._height:
                frame_bgr = cv2.resize(frame_bgr, (self._width, self._height), interpolation=cv2.INTER_AREA)
            rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
            self._cam_device.write_frame(rgb.tobytes())
        except Exception:
            pass

    # ── frame composition ────────────────────────────────────────────────────

    def _compose_frame(self, base: np.ndarray, src_name: str) -> np.ndarray:
        """Stack PiP thumbnails + badges onto a copy of the main frame."""
        # Resize to stream resolution before compositing so coordinates are predictable
        if base.shape[1] != self._width or base.shape[0] != self._height:
            frame = cv2.resize(base, (self._width, self._height), interpolation=cv2.INTER_AREA)
        else:
            frame = base.copy()

        # PiP thumbnails
        if self._pip_enabled:
            self._overlay_thumbnail(frame, self._pip_left,  position="left")
            self._overlay_thumbnail(frame, self._pip_right, position="right")

        # Badges
        if self._show_cam_name:
            self._overlay_camera_name(frame, src_name)

        if self._show_speed_badge and self._motion_state is not None:
            try:
                lin, _, locked, braking = self._motion_state()
                self._overlay_speed_badge(frame, lin, locked, braking)
            except Exception:
                pass

        if self._show_temphum:
            try:
                self._overlay_temphum(frame, self._temphum_get_fn())
            except Exception:
                pass

        if self._show_timestamp:
            self._overlay_timestamp(frame)

        return frame

    def _overlay_thumbnail(self, frame: np.ndarray, src: str, position: str) -> None:
        """Composite one PiP thumbnail. Skips silently if source is missing or stale."""
        if not self._cameras.has(src):
            return
        ts, thumb = self._cameras.read(src)
        if thumb is None or ts is None:
            return
        if (time.time() - ts) > self._pip_stale_sec:
            return   # don't show stale frames

        # Resize thumbnail to configured PiP dimensions
        small = cv2.resize(thumb, (self._pip_w, self._pip_h), interpolation=cv2.INTER_AREA)

        H, W = frame.shape[:2]
        y1 = self._pip_margin
        if position == "left":
            x1 = self._pip_margin
        else:   # right
            x1 = W - self._pip_w - self._pip_margin
        x2 = x1 + self._pip_w
        y2 = y1 + self._pip_h

        # Bounds check — don't crash if config makes thumbs too big
        if x2 > W or y2 > H or x1 < 0 or y1 < 0:
            return

        frame[y1:y2, x1:x2] = small
        cv2.rectangle(frame, (x1 - 1, y1 - 1), (x2 + 1, y2 + 1), (255, 255, 255), 1)

        if self._pip_show_label:
            label = f"thumb:{src}"
            cv2.putText(
                frame, label, (x1 + 6, y1 + 14),
                cv2.FONT_HERSHEY_SIMPLEX, 0.45, (255, 255, 255), 1, cv2.LINE_AA,
            )

    def _overlay_camera_name(self, frame: np.ndarray, name: str) -> None:
        cv2.putText(
            frame, name, (10, 28),
            cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 255, 255), 2, cv2.LINE_AA,
        )

    def _overlay_speed_badge(self, frame: np.ndarray, lin: float, locked: bool, braking: bool) -> None:
        H = frame.shape[0]
        if locked:
            text = "LOCKED"
            color = (0, 0, 255)         # red in BGR
        elif braking:
            text = "BRAKE"
            color = (0, 0, 255)
        else:
            pct = int(round(lin * 100.0))
            text = f"speed={pct:+d}%"
            color = (255, 255, 255)
        cv2.putText(
            frame, text, (10, H - 14),
            cv2.FONT_HERSHEY_SIMPLEX, 0.65, color, 2, cv2.LINE_AA,
        )

    def _overlay_timestamp(self, frame: np.ndarray) -> None:
        ts = time.strftime("%H:%M:%S", time.localtime())
        cv2.putText(
            frame, ts, (10, 58),
            cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2, cv2.LINE_AA,
        )

    def _overlay_temphum(self, frame: np.ndarray, snap: dict) -> None:
        """Temp + humidity in the bottom-right corner.

        Temperature color (BGR): green < yellow_f, yellow < red_f, else red.
        Humidity color: dark blue. Stale readings render dim gray.
        """
        if not snap or "temp_f" not in snap or "humidity_pct" not in snap:
            return

        t_f = float(snap["temp_f"])
        rh  = float(snap["humidity_pct"])
        age = float(snap.get("age_sec", 0.0))
        stale = age > self._temphum_stale_sec

        if stale:
            t_color  = (160, 160, 160)
            rh_color = (160, 160, 160)
        else:
            if   t_f <= self._temphum_yellow_f: t_color = (0, 200,   0)  # green
            elif t_f <= self._temphum_red_f:    t_color = (0, 255, 255)  # yellow
            else:                               t_color = (0,   0, 255)  # red
            rh_color = (139, 0, 0)  # dark blue (BGR)

        t_text  = f"T:{t_f:.1f}F"
        rh_text = f"H:{rh:.0f}%"

        font  = cv2.FONT_HERSHEY_SIMPLEX
        scale = 0.65
        thick = 2

        H, W = frame.shape[:2]
        (tw, th), _ = cv2.getTextSize(t_text,  font, scale, thick)
        (hw, hh), _ = cv2.getTextSize(rh_text, font, scale, thick)
        right_pad = 10
        bottom_pad = 14
        gap = 6

        # Right-aligned, stacked bottom-up: humidity below temperature.
        x_h = W - hw - right_pad
        y_h = H - bottom_pad
        x_t = W - tw - right_pad
        y_t = y_h - hh - gap

        cv2.putText(frame, t_text,  (x_t, y_t), font, scale, t_color,  thick, cv2.LINE_AA)
        cv2.putText(frame, rh_text, (x_h, y_h), font, scale, rh_color, thick, cv2.LINE_AA)

    # ── Daily token + mic ─────────────────────────────────────────────────────

    def _get_token(self) -> str:
        r = requests.post(
            "https://api.daily.co/v1/meeting-tokens",
            headers={"Authorization": f"Bearer {self._api_key}", "Content-Type": "application/json"},
            data=json.dumps({
                "properties": {
                    "room_name": self._room_name,
                    "is_owner":  True,
                    "exp":       int(time.time()) + 3600,
                }
            }),
            timeout=15,
        )
        r.raise_for_status()
        return r.json()["token"]

    def _start_mic(self) -> None:
        cmd = [
            "ffmpeg",
            "-hide_banner", "-loglevel", "error",
            "-fflags", "+nobuffer+flush_packets",
            "-flags", "low_delay",
            "-analyzeduration", "0", "-probesize", "32",
            "-rtsp_transport", self._mic_rtsp_transport,
            "-i", self._mic_rtsp_url,
            "-vn",
            "-ac", str(self._mic_channels),
            "-ar", str(self._mic_sample_rate),
            "-acodec", "pcm_s16le",
            "-f", "s16le",
            "-",
        ]
        try:
            self._mic_proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
            )
            self._mic_thread = threading.Thread(target=self._mic_reader, daemon=True, name="mic-reader")
            self._mic_thread.start()
            log("stream", f"mic capture started ({self._mic_rtsp_transport})")
        except Exception as exc:
            log("stream", f"mic start failed: {exc}")
            self._mic_proc = None

    def _mic_reader(self) -> None:
        # 5ms PCM s16le mono: 16000 * 0.005 * 2 = 160 bytes
        chunk_size = max(1, int(self._mic_sample_rate * (self._mic_frame_ms / 1000.0))) * self._mic_channels * 2
        while not self._stop.is_set():
            if self._mic_proc is None or self._mic_proc.stdout is None:
                break

            # When locked, drop audio (don't publish silence — Daily would still bill bandwidth)
            if self._robot_locked:
                try:
                    self._mic_proc.stdout.read(chunk_size)   # drain but don't push
                except Exception:
                    break
                continue

            try:
                data = self._mic_proc.stdout.read(chunk_size)
            except Exception:
                break
            if not data:
                break
            if self._mic_device is not None:
                try:
                    self._mic_device.write_frames(data)
                except Exception:
                    pass

    def _stop_mic(self) -> None:
        if self._mic_proc is not None:
            try:
                self._mic_proc.terminate()
                self._mic_proc.wait(timeout=2.0)
            except Exception:
                try:
                    self._mic_proc.kill()
                except Exception:
                    pass
            self._mic_proc = None


class _CallHandler(EventHandler):
    """Minimal event handler — just signals when we've joined."""
    def __init__(self):
        super().__init__()
        self.joined = threading.Event()
        self.left   = threading.Event()

    def on_call_state_updated(self, state):
        log("stream", f"call state: {state}")
        if state == "joined":
            self.joined.set()
        if state == "left":
            self.left.set()

    def on_error(self, msg):
        print(f"[stream] Daily error: {msg}", file=sys.stderr)