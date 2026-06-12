# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations

"""
Session recorder — floor camera as H.264 MP4 + telemetry as JSONL.

One session = one folder under cache_dir:
    session_YYYYMMDD_HHMMSS/
        video.mp4
        data.jsonl
        session.json

Designed for:
    unlock -> recorder.start()
    lock   -> recorder.stop()

stop() is robust:
    - safe if called twice
    - still writes session.json after ffmpeg pipe failure
    - finalizes any open session even if _active is already False
"""

import json
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

import numpy as np

from .common import log


class SessionRecorder:
    def __init__(
        self,
        base_dir:           str,
        camera_name:        str,
        cameras,
        width:              int,
        height:             int,
        fps:                int,
        video_bitrate:      str,
        encoder_preference: list,
        motion_state_fn:    Optional[Callable[[], tuple]] = None,
        # imu_get_fn:         Optional[Callable[[], dict]]  = None,
        gps_get_fn:         Optional[Callable[[], dict]]  = None,
    ) -> None:
        self._base_dir       = Path(base_dir)
        self._camera_name    = camera_name
        self._cameras        = cameras
        self._width          = width
        self._height         = height
        self._fps            = max(1, fps)
        self._video_bitrate  = video_bitrate
        self._encoder_pref   = list(encoder_preference)
        self._motion_state   = motion_state_fn
        # self._imu_get        = imu_get_fn
        self._gps_get        = gps_get_fn

        self._session_dir:   Optional[Path] = None
        self._video_path:    Optional[Path] = None
        self._jsonl_path:    Optional[Path] = None
        self._ffmpeg:        Optional[subprocess.Popen] = None
        self._encoder_used:  Optional[str] = None
        self._jsonl_file = None
        self._jsonl_lock = threading.Lock()

        self._frame_index = 0
        self._start_unix: float = 0.0
        self._start_mono: float = 0.0

        self._active = False
        self._stop = threading.Event()
        self._robot_locked = False
        self._tick_thread: Optional[threading.Thread] = None

    def start(self) -> bool:
        """
        Open a new session and begin recording.
        """
        if self._active:
            return True

        if self._session_dir is not None:
            self.stop()

        if not self._cameras.has(self._camera_name):
            log("record", f"camera {self._camera_name!r} not available — cannot record")
            return False

        if not self._open_session():
            return False

        self._active = True
        self._stop.clear()

        self._tick_thread = threading.Thread(
            target=self._tick_loop,
            daemon=True,
            name="rec-tick",
        )
        self._tick_thread.start()

        log("record", f"▶  recording → {self._session_dir}")
        return True

    def stop(self) -> None:
        """
        Stop recording and finalize MP4 + JSONL + session.json.
        """
        if self._session_dir is None:
            return

        self._active = False
        self._stop.set()

        if self._tick_thread is not None:
            try:
                self._tick_thread.join(timeout=2.0)
            except Exception:
                pass
            self._tick_thread = None

        if self._ffmpeg is not None:
            try:
                if self._ffmpeg.stdin is not None:
                    try:
                        self._ffmpeg.stdin.close()
                    except Exception:
                        pass

                try:
                    self._ffmpeg.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    log("record", "ffmpeg finalize timeout — killing process")
                    try:
                        self._ffmpeg.kill()
                    except Exception:
                        pass
                    try:
                        self._ffmpeg.wait(timeout=2)
                    except Exception:
                        pass

            except Exception as exc:
                log("record", f"ffmpeg stop error: {exc}")
                try:
                    self._ffmpeg.kill()
                except Exception:
                    pass
            finally:
                self._ffmpeg = None

        with self._jsonl_lock:
            if self._jsonl_file is not None:
                try:
                    self._jsonl_file.flush()
                    self._jsonl_file.close()
                except Exception as exc:
                    log("record", f"jsonl close error: {exc}")
                finally:
                    self._jsonl_file = None

        try:
            self._write_session_metadata()
        except Exception as exc:
            log("record", f"session.json write error: {exc}")

        log("record", f"■  stopped — {self._frame_index} frames → {self._session_dir}")

        self._session_dir = None
        self._video_path = None
        self._jsonl_path = None
        self._encoder_used = None
        self._frame_index = 0
        self._start_unix = 0.0
        self._start_mono = 0.0
        self._stop.clear()

    def set_robot_lock(self, locked: bool) -> None:
        self._robot_locked = bool(locked)

    def is_active(self) -> bool:
        return self._active

    def _open_session(self) -> bool:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._session_dir = self._base_dir / f"session_{stamp}"

        try:
            self._session_dir.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            log("record", f"cannot create {self._session_dir}: {exc}")
            self._session_dir = None
            return False

        self._video_path = self._session_dir / "video.mp4"
        self._jsonl_path = self._session_dir / "data.jsonl"

        encoder = self._probe_and_start_ffmpeg(self._video_path)
        if encoder is None:
            log("record", "no H.264 encoder available — cannot record")
            self._cleanup_failed_open()
            return False

        self._encoder_used = encoder

        try:
            self._jsonl_file = open(
                self._jsonl_path,
                "w",
                encoding="utf-8",
                buffering=1,
            )
        except Exception as exc:
            log("record", f"cannot open {self._jsonl_path}: {exc}")
            self._cleanup_failed_open()
            return False

        self._frame_index = 0
        self._start_unix = time.time()
        self._start_mono = time.monotonic()
        return True

    def _cleanup_failed_open(self) -> None:
        if self._ffmpeg is not None:
            try:
                if self._ffmpeg.stdin is not None:
                    self._ffmpeg.stdin.close()
            except Exception:
                pass
            try:
                self._ffmpeg.kill()
            except Exception:
                pass
            self._ffmpeg = None

        with self._jsonl_lock:
            if self._jsonl_file is not None:
                try:
                    self._jsonl_file.close()
                except Exception:
                    pass
                self._jsonl_file = None

        self._session_dir = None
        self._video_path = None
        self._jsonl_path = None
        self._encoder_used = None
        self._frame_index = 0
        self._start_unix = 0.0
        self._start_mono = 0.0

    def _probe_and_start_ffmpeg(self, video_path: Path) -> Optional[str]:
        for encoder in self._encoder_pref:
            cmd = self._build_ffmpeg_cmd(encoder, video_path)

            try:
                proc = subprocess.Popen(
                    cmd,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                )

                time.sleep(0.4)

                if proc.poll() is not None:
                    err = ""
                    try:
                        raw = proc.stderr.read() if proc.stderr is not None else b""
                        err = (raw or b"").decode("utf-8", errors="ignore")[:300]
                    except Exception:
                        pass

                    log(
                        "record",
                        f"encoder {encoder} unavailable: {err.strip() or 'exited early'}",
                    )
                    continue

                self._ffmpeg = proc
                log("record", f"encoder = {encoder}")
                return encoder

            except FileNotFoundError:
                log("record", "ffmpeg not found in PATH")
                return None

            except Exception as exc:
                log("record", f"encoder {encoder} start failed: {exc}")
                continue

        return None

    def _build_ffmpeg_cmd(self, encoder: str, video_path: Path) -> list:
        common_in = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "rawvideo",
            "-pix_fmt",
            "bgr24",
            "-s",
            f"{self._width}x{self._height}",
            "-r",
            str(self._fps),
            "-i",
            "pipe:0",
        ]

        if encoder == "h264_nvenc":
            enc = [
                "-c:v",
                "h264_nvenc",
                "-preset",
                "p4",
                "-b:v",
                self._video_bitrate,
                "-maxrate",
                self._video_bitrate,
                "-bufsize",
                "3000k",
                "-pix_fmt",
                "yuv420p",
            ]

        elif encoder == "h264_v4l2m2m":
            enc = [
                "-c:v",
                "h264_v4l2m2m",
                "-b:v",
                self._video_bitrate,
                "-pix_fmt",
                "yuv420p",
            ]

        else:
            enc = [
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-tune",
                "zerolatency",
                "-b:v",
                self._video_bitrate,
                "-pix_fmt",
                "yuv420p",
            ]

        out = [
            "-movflags",
            "+faststart",
            "-y",
            str(video_path),
        ]

        return common_in + enc + out

    def _write_session_metadata(self) -> None:
        if self._session_dir is None:
            return

        try:
            start_iso = datetime.fromtimestamp(self._start_unix).isoformat()
        except Exception:
            start_iso = None

        meta = {
            "session_dir":     str(self._session_dir),
            "start_unix":      self._start_unix,
            "start_iso":       start_iso,
            "fps":             self._fps,
            "frame_count":     self._frame_index,
            "duration_sec":    self._frame_index / self._fps if self._fps else 0,
            "encoder":         self._encoder_used,
            "width":           self._width,
            "height":          self._height,
            "video":           "video.mp4",
            "telemetry":       "data.jsonl",
            "camera":          self._camera_name,
        }

        try:
            with open(self._session_dir / "session.json", "w", encoding="utf-8") as f:
                json.dump(meta, f, indent=2)
        except Exception as exc:
            log("record", f"failed to write session.json: {exc}")

    def _tick_loop(self) -> None:
        interval = 1.0 / self._fps
        next_tick = time.monotonic()

        while not self._stop.is_set():
            sleep_for = next_tick - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)

            next_tick += interval

            now = time.monotonic()
            if next_tick < now - interval:
                next_tick = now + interval

            if self._robot_locked:
                continue

            try:
                ts, frame = self._cameras.read(self._camera_name)
            except Exception as exc:
                log("record", f"camera read failed: {exc}")
                continue

            if frame is None:
                continue

            self._write_frame(frame, ts)

    def _write_frame(self, frame: np.ndarray, capture_ts: Optional[float]) -> None:
        if frame.shape[1] != self._width or frame.shape[0] != self._height:
            try:
                import cv2
                frame = cv2.resize(
                    frame,
                    (self._width, self._height),
                    interpolation=cv2.INTER_AREA,
                )
            except Exception as exc:
                log("record", f"resize failed: {exc}")
                return

        if self._ffmpeg is not None and self._ffmpeg.stdin is not None:
            try:
                self._ffmpeg.stdin.write(frame.tobytes())
            except (BrokenPipeError, OSError) as exc:
                log("record", f"ffmpeg pipe lost: {exc} — recording thread stopping")
                self._active = False
                self._stop.set()
                return
            except Exception as exc:
                log("record", f"ffmpeg write failed: {exc}")
                self._active = False
                self._stop.set()
                return

        row = self._build_row(capture_ts)

        try:
            with self._jsonl_lock:
                if self._jsonl_file is not None:
                    self._jsonl_file.write(json.dumps(row) + "\n")
        except Exception as exc:
            log("record", f"jsonl write failed: {exc}")

        self._frame_index += 1

    def _build_row(self, capture_ts: Optional[float]) -> dict:
        idx = self._frame_index
        now_unix = time.time()
        rel_t = round(now_unix - self._start_unix, 4) if self._start_unix else 0.0

        lin_x = ang_z = 0.0
        locked = braking = False

        if self._motion_state is not None:
            try:
                lin_x, ang_z, locked, braking = self._motion_state()
            except Exception:
                pass

        # try:
        #     imu_d = self._imu_get() if self._imu_get is not None else {}
        # except Exception:
        #     imu_d = {}

        try:
            gps_d = self._gps_get() if self._gps_get is not None else {}
        except Exception:
            gps_d = {}

        return {
            "frame_index":      idx,
            # "ts_unix":          round(now_unix, 4),
            # "ts_capture":       round(capture_ts, 4) if capture_ts else None,
            "relative_time":    rel_t,

            "linear_velocity":  lin_x,
            "angular_velocity": ang_z,
            # "robot_locked":     bool(locked),
            # "braking":          bool(braking),

            # "accelerometer_x":  imu_d.get("accelerometer_x"),
            # "accelerometer_y":  imu_d.get("accelerometer_y"),
            # "accelerometer_z":  imu_d.get("accelerometer_z"),
            # "gyroscope_x":      imu_d.get("gyroscope_x"),
            # "gyroscope_y":      imu_d.get("gyroscope_y"),
            # "gyroscope_z":      imu_d.get("gyroscope_z"),
            # "magnetometer_x":   imu_d.get("magnetometer_x"),
            # "magnetometer_y":   imu_d.get("magnetometer_y"),
            # "magnetometer_z":   imu_d.get("magnetometer_z"),
            # "roll":             imu_d.get("roll"),
            # "pitch":            imu_d.get("pitch"),
            # "yaw":              imu_d.get("yaw"),

            "gps_latitude":        gps_d.get("gps_latitude"),
            "gps_longitude":       gps_d.get("gps_longitude"),
            # "gps_altitude":        gps_d.get("gps_altitude"),
            # "gps_fix":             gps_d.get("gps_fix"),
            # "gps_satellites":      gps_d.get("gps_satellites"),
            # "gps_hdop":            gps_d.get("gps_hdop"),
            # "gps_speed_kmh":       gps_d.get("gps_speed_kmh"),
            "orientation":         gps_d.get("orientation"),
            "gps_solution_status": gps_d.get("gps_solution_status"),
            "gps_position_type":   gps_d.get("gps_position_type"),
        }