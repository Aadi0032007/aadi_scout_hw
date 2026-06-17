# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations

"""
Session recorder — camera as H.264 MP4 + telemetry as JSONL.

One session = one folder under cache_dir:
    session_YYYYMMDD_HHMMSS/
        video.mp4
        data.jsonl
        session.json

Designed for:
    unlock -> recorder.start()
    lock   -> recorder.stop()

Two encoder backends are tried in order of `record_encoder_preference`:
    - "gst_nvenc" → GStreamer pipeline with nvv4l2h264enc (Jetson HW encoder)
    - "libx264"   → ffmpeg subprocess with libx264 (CPU fallback)

Whichever opens first is used. The MP4 + JSONL + session.json output is
identical either way. stop() finalises the active backend cleanly and is
safe to call twice or while already stopped.
"""

import json
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

import cv2
import numpy as np

from .common import log


# ── GStreamer python bindings (optional) ──────────────────────────────────────
# We probe gi at import time. If the bindings aren't there, _GstWriter will
# still raise cleanly on instantiation and SessionRecorder falls through to
# the ffmpeg backend.

try:
    import gi  # type: ignore
    gi.require_version("Gst", "1.0")
    from gi.repository import Gst, GLib  # type: ignore
    Gst.init(None)
    _GST_AVAILABLE = True
except Exception as _gst_import_exc:    # pragma: no cover
    Gst = None       # type: ignore
    GLib = None      # type: ignore
    _GST_AVAILABLE = False


# ── helpers ───────────────────────────────────────────────────────────────────

def _parse_bitrate_to_bps(s) -> int:
    """'1500k' → 1500000, '2M' → 2000000, '900000' → 900000."""
    t = str(s).strip().lower()
    if t.endswith("k"):
        return int(float(t[:-1]) * 1_000)
    if t.endswith("m"):
        return int(float(t[:-1]) * 1_000_000)
    return int(float(t))


# ══════════════════════════════════════════════════════════════════════════════
#  Video writer backends
# ══════════════════════════════════════════════════════════════════════════════
#
# Both backends accept BGR uint8 numpy frames and produce an H.264 MP4 at the
# configured path. They share this small interface:
#
#     name             : str — for logging / session metadata
#     write(frame, i)  : push frame i into the encoder; False if dead
#     close(timeout)   : finalize the file; safe to call once
#     is_alive()       : True if the backend is still accepting frames
# ══════════════════════════════════════════════════════════════════════════════


class _GstWriter:
    """
    GStreamer-based H.264 recorder using Jetson's NVENC engine.

    Pipeline:
        appsrc (BGR from Python)
          → videoconvert       (BGR → I420/whatever nvvidconv accepts)
          → nvvidconv          (CPU → NVMM, VIC colorspace to NV12)
          → nvv4l2h264enc      (NVENC hardware encode)
          → h264parse → mp4mux → filesink

    PTS is set explicitly per frame from frame_index/fps; we don't rely on
    appsrc's wallclock timestamping. That way pauses (e.g. robot_lock holding
    frames) don't create gaps in the recorded video.
    """

    name = "gst_nvenc"

    def __init__(
        self,
        video_path: Path,
        width: int,
        height: int,
        fps: int,
        bitrate_bps: int,
    ) -> None:
        if not _GST_AVAILABLE:
            raise RuntimeError("GStreamer python bindings (gi) not installed")

        gop = max(1, fps * 2)
        pipeline_str = (
            f"appsrc name=src is-live=true format=time do-timestamp=false "
            f"block=true max-bytes=0 ! "
            f"video/x-raw,format=BGR,width={width},height={height},"
            f"framerate={fps}/1 ! "
            f"videoconvert ! "
            f"nvvidconv ! "
            f"video/x-raw(memory:NVMM),format=NV12 ! "
            f"nvv4l2h264enc "
            f"bitrate={bitrate_bps} "
            f"iframeinterval={gop} "
            f"insert-sps-pps=true "
            f"maxperf-enable=true "
            f"control-rate=1 ! "
            f"h264parse ! mp4mux ! "
            f"filesink location={video_path} sync=false async=false"
        )

        try:
            self._pipeline = Gst.parse_launch(pipeline_str)
        except GLib.Error as exc:
            raise RuntimeError(f"parse_launch failed: {exc.message}")

        self._appsrc = self._pipeline.get_by_name("src")
        if self._appsrc is None:
            self._pipeline.set_state(Gst.State.NULL)
            raise RuntimeError("appsrc element 'src' missing from pipeline")

        self._bus = self._pipeline.get_bus()
        self._width  = width
        self._height = height
        self._fps    = max(1, fps)
        self._alive  = True

        ret = self._pipeline.set_state(Gst.State.PLAYING)
        if ret == Gst.StateChangeReturn.FAILURE:
            self._pipeline.set_state(Gst.State.NULL)
            raise RuntimeError("pipeline failed to enter PLAYING state")

        # Wait briefly for the pipeline to actually be PLAYING — this is where
        # nvv4l2h264enc would fail if NVENC weren't available on this system.
        _ret, state, _pending = self._pipeline.get_state(2 * Gst.SECOND)
        if state != Gst.State.PLAYING:
            err_msg = self._drain_error()
            self._pipeline.set_state(Gst.State.NULL)
            raise RuntimeError(
                f"pipeline did not reach PLAYING (state={state.value_nick}); "
                f"{err_msg or 'no error on bus'}"
            )

    def _drain_error(self) -> str:
        """Drain any pending ERROR message from the bus and return its text."""
        msg = self._bus.pop_filtered(Gst.MessageType.ERROR)
        if msg is None:
            return ""
        err, _debug = msg.parse_error()
        return err.message or ""

    def write(self, frame: np.ndarray, frame_index: int) -> bool:
        if not self._alive:
            return False

        if frame.shape[1] != self._width or frame.shape[0] != self._height:
            try:
                frame = cv2.resize(
                    frame, (self._width, self._height),
                    interpolation=cv2.INTER_AREA,
                )
            except Exception as exc:
                log("record", f"gst resize failed: {exc}")
                return False

        if not frame.flags["C_CONTIGUOUS"]:
            frame = np.ascontiguousarray(frame)

        # Non-blocking bus check — catch encoder errors between pushes
        msg = self._bus.pop_filtered(Gst.MessageType.ERROR | Gst.MessageType.EOS)
        if msg is not None:
            if msg.type == Gst.MessageType.ERROR:
                err, _ = msg.parse_error()
                log("record", f"gst pipeline error: {err.message}")
            else:
                log("record", "gst pipeline reached EOS unexpectedly")
            self._alive = False
            return False

        data = frame.tobytes()
        try:
            buf = Gst.Buffer.new_allocate(None, len(data), None)
            buf.fill(0, data)
            buf.pts      = (frame_index * Gst.SECOND) // self._fps
            buf.duration = Gst.SECOND // self._fps
            ret = self._appsrc.emit("push-buffer", buf)
        except Exception as exc:
            log("record", f"gst push-buffer threw: {exc}")
            self._alive = False
            return False

        if ret != Gst.FlowReturn.OK:
            log("record", f"gst appsrc returned {ret.value_nick}")
            self._alive = False
            return False
        return True

    def close(self, timeout: float = 10.0) -> None:
        if self._pipeline is None:
            return
        try:
            if self._alive:
                try:
                    self._appsrc.emit("end-of-stream")
                except Exception as exc:
                    log("record", f"gst EOS emit failed: {exc}")

                msg = self._bus.timed_pop_filtered(
                    int(timeout * Gst.SECOND),
                    Gst.MessageType.EOS | Gst.MessageType.ERROR,
                )
                if msg is None:
                    log("record", "gst finalize timeout — forcing pipeline NULL")
                elif msg.type == Gst.MessageType.ERROR:
                    err, _ = msg.parse_error()
                    log("record", f"gst close error: {err.message}")
        finally:
            try:
                self._pipeline.set_state(Gst.State.NULL)
            except Exception:
                pass
            self._pipeline = None
            self._alive = False

    def is_alive(self) -> bool:
        return self._alive


class _FfmpegWriter:
    """
    ffmpeg-subprocess H.264 recorder. Used as a CPU fallback (libx264).

    We deliberately don't list h264_v4l2m2m here: NVIDIA's L4T ffmpeg ships
    decoder integration but no working v4l2-m2m encoder on Jetson Orin, so it
    fails with "Could not find a valid device". For HW encode use _GstWriter.
    """

    def __init__(
        self,
        encoder: str,
        video_path: Path,
        width: int,
        height: int,
        fps: int,
        bitrate: str,
    ) -> None:
        self.name = encoder
        self._width  = width
        self._height = height
        self._proc: Optional[subprocess.Popen] = None
        self._alive = False

        cmd = self._build_cmd(encoder, video_path, width, height, fps, bitrate)

        try:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
        except FileNotFoundError:
            raise RuntimeError("ffmpeg not found in PATH")

        time.sleep(0.4)

        if proc.poll() is not None:
            err_text = ""
            try:
                raw = proc.stderr.read() if proc.stderr is not None else b""
                err_text = (raw or b"").decode("utf-8", errors="ignore")[:300]
            except Exception:
                pass
            raise RuntimeError(
                f"ffmpeg exited early: {err_text.strip() or 'no stderr output'}"
            )

        self._proc = proc
        self._alive = True

    @staticmethod
    def _build_cmd(
        encoder: str,
        video_path: Path,
        width: int,
        height: int,
        fps: int,
        bitrate: str,
    ) -> list:
        common_in = [
            "ffmpeg",
            "-hide_banner", "-loglevel", "error",
            "-f", "rawvideo",
            "-pix_fmt", "bgr24",
            "-s", f"{width}x{height}",
            "-r", str(fps),
            "-i", "pipe:0",
        ]

        if encoder == "libx264":
            enc = [
                "-c:v", "libx264",
                "-preset", "ultrafast",
                "-tune", "zerolatency",
                "-threads", "2",
                "-b:v", bitrate,
                "-g", str(fps * 2),
                "-pix_fmt", "yuv420p",
            ]
        elif encoder == "h264_v4l2m2m":
            # Kept for compatibility; will almost certainly fail to open on
            # Jetson L4T because NVIDIA's hw encoder isn't a v4l2-m2m device.
            enc = [
                "-c:v", "h264_v4l2m2m",
                "-b:v", bitrate,
                "-pix_fmt", "yuv420p",
                "-g", str(fps * 2),
                "-num_output_buffers", "32",
                "-num_capture_buffers", "16",
            ]
        else:
            raise ValueError(f"unsupported ffmpeg encoder: {encoder!r}")

        out = ["-movflags", "+faststart", "-y", str(video_path)]
        return common_in + enc + out

    def write(self, frame: np.ndarray, frame_index: int) -> bool:
        if not self._alive or self._proc is None or self._proc.stdin is None:
            return False

        if frame.shape[1] != self._width or frame.shape[0] != self._height:
            try:
                frame = cv2.resize(
                    frame, (self._width, self._height),
                    interpolation=cv2.INTER_AREA,
                )
            except Exception as exc:
                log("record", f"ffmpeg resize failed: {exc}")
                return False

        try:
            self._proc.stdin.write(frame.tobytes())
        except (BrokenPipeError, OSError) as exc:
            log("record", f"ffmpeg pipe lost: {exc}")
            self._alive = False
            return False
        except Exception as exc:
            log("record", f"ffmpeg write failed: {exc}")
            self._alive = False
            return False
        return True

    def close(self, timeout: float = 10.0) -> None:
        if self._proc is None:
            return
        try:
            if self._proc.stdin is not None:
                try:
                    self._proc.stdin.close()
                except Exception:
                    pass

            try:
                self._proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                log("record", "ffmpeg finalize timeout — killing process")
                try:
                    self._proc.kill()
                except Exception:
                    pass
                try:
                    self._proc.wait(timeout=2)
                except Exception:
                    pass
        except Exception as exc:
            log("record", f"ffmpeg stop error: {exc}")
            try:
                self._proc.kill()
            except Exception:
                pass
        finally:
            self._proc = None
            self._alive = False

    def is_alive(self) -> bool:
        return self._alive


# ══════════════════════════════════════════════════════════════════════════════
#  SessionRecorder
# ══════════════════════════════════════════════════════════════════════════════


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
        self._bitrate_bps    = _parse_bitrate_to_bps(video_bitrate)
        self._encoder_pref   = list(encoder_preference)
        self._motion_state   = motion_state_fn
        # self._imu_get        = imu_get_fn
        self._gps_get        = gps_get_fn

        self._session_dir:   Optional[Path] = None
        self._video_path:    Optional[Path] = None
        self._jsonl_path:    Optional[Path] = None
        self._writer:        Optional[object] = None   # _GstWriter | _FfmpegWriter
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

        log("record", f"▶  recording → {self._session_dir} ({self._encoder_used})")
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

        if self._writer is not None:
            try:
                self._writer.close(timeout=10.0)
            except Exception as exc:
                log("record", f"writer close error: {exc}")
            finally:
                self._writer = None

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

    # ── internals ────────────────────────────────────────────────────────────

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

        writer = self._probe_and_open_writer(self._video_path)
        if writer is None:
            log("record", "no working H.264 encoder available — cannot record")
            self._cleanup_failed_open()
            return False

        self._writer = writer
        self._encoder_used = writer.name

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

    def _probe_and_open_writer(self, video_path: Path):
        """Try each encoder in preference order; return the first that opens."""
        for encoder in self._encoder_pref:
            try:
                if encoder == "gst_nvenc":
                    writer = _GstWriter(
                        video_path=video_path,
                        width=self._width,
                        height=self._height,
                        fps=self._fps,
                        bitrate_bps=self._bitrate_bps,
                    )
                elif encoder in ("libx264", "h264_v4l2m2m"):
                    writer = _FfmpegWriter(
                        encoder=encoder,
                        video_path=video_path,
                        width=self._width,
                        height=self._height,
                        fps=self._fps,
                        bitrate=self._video_bitrate,
                    )
                else:
                    log("record", f"unknown encoder {encoder!r} — skipping")
                    continue
            except Exception as exc:
                log("record", f"encoder {encoder} unavailable: {exc}")
                continue

            log("record", f"encoder = {writer.name}")
            return writer

        return None

    def _cleanup_failed_open(self) -> None:
        if self._writer is not None:
            try:
                self._writer.close(timeout=2.0)
            except Exception:
                pass
            self._writer = None

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
        if self._writer is None:
            return

        ok = self._writer.write(frame, self._frame_index)
        if not ok:
            log("record", "writer no longer alive — stopping recording thread")
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

        if self._motion_state is not None:
            try:
                result = self._motion_state()
                # support both published_state() -> (lin, ang)
                # and state() -> (lin, ang, locked, braking)
                lin_x, ang_z = result[0], result[1]
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
            # ...

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