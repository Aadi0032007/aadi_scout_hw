# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations


"""
Multi-camera capture for stream + recorder.

Each camera runs in its own background thread, keeping a single latest frame
in a 1-slot buffer. read_latest() and read(name) are non-blocking and always
return immediately — either the freshest frame or (None, None) if the source
is down.

RTSP sources tolerate multiple concurrent clients (the camera does fan-out).
USB sources are exclusive — if one is already open by another process,
this capture will fail at startup and that camera is simply absent from
the collection. The orchestrator carries on with whichever cameras opened.

Auto-reconnect with exponential backoff on stream drop.

Frame bus fan-out
-----------------
If a CameraConfig has publish_frames=True, the capture thread also writes
each frame into a shared-memory region (see LAB/frame_bus.py). External
processes — e.g. an AI inference worker — attach to that region as readers
and never touch the V4L2 device themselves. The publisher is created
lazily on the first decoded frame so we use the camera's actual reported
shape, not the requested one (some V4L2 devices ignore CAP_PROP_FRAME_*).
"""

import os
import threading
import time
from typing import Optional

import cv2
import numpy as np

from .common import log
from .config import CameraConfig


class CameraCapture:
    """Background reader for a single RTSP URL or V4L2 device."""

    def __init__(self, cfg: CameraConfig) -> None:
        self.name = cfg.name
        self._cfg = cfg
        self._frame: Optional[np.ndarray] = None
        self._ts:    float = 0.0
        self._lock   = threading.Lock()
        self._stop   = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # Frame-bus publisher (created lazily on first frame, only if enabled)
        self._publish_enabled = bool(getattr(cfg, "publish_frames", False))
        self._publisher = None   # FrameBusPublisher | None

    # ── public API ────────────────────────────────────────────────────────────

    def start(self) -> bool:
        """Probe-open the source. Return True if reachable, False to skip."""
        cap = self._open_capture()
        ok = cap.isOpened()
        cap.release()
        if not ok:
            log("cameras", f"{self.name}: cannot open {self._cfg.source!r} — skipping")
            return False
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name=f"cam-{self.name}")
        self._thread.start()
        log(
            "cameras",
            f"{self.name}: started "
            f"{self._cfg.width}x{self._cfg.height}@{self._cfg.fps}fps "
            f"({'RTSP' if self._cfg.is_rtsp else 'V4L2'})"
            f"{' [hw]' if getattr(self._cfg, 'hw_decode', False) else ''}"
            f"{' [bus]' if self._publish_enabled else ''}",
        )
        return True

    def read_latest(self) -> tuple[Optional[float], Optional[np.ndarray]]:
        """Return (capture_timestamp, frame_copy). Non-blocking."""
        with self._lock:
            if self._frame is None:
                return None, None
            return self._ts, self._frame   # caller may copy if they need to mutate

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None
        with self._lock:
            self._frame = None
        if self._publisher is not None:
            try:
                self._publisher.close()
            except Exception as exc:
                log("cameras", f"{self.name}: publisher close error: {exc}")
            self._publisher = None

    # ── capture pipeline ──────────────────────────────────────────────────────

    def _open_capture(self) -> cv2.VideoCapture:
        cfg = self._cfg

        # ── Hardware-accelerated path: GStreamer with NVDEC/VIC ──────────────
        if getattr(cfg, "hw_decode", False):
            if cfg.is_rtsp:
                pipeline = self._build_gst_rtsp_pipeline(cfg)
            else:
                fmt = getattr(cfg, "pixel_format", "MJPG").upper()
                if fmt == "YUYV":
                    pipeline = self._build_gst_v4l2_yuyv_pipeline(cfg)
                else:
                    pipeline = self._build_gst_v4l2_mjpeg_pipeline(cfg)
            log("cameras", f"{self.name}: gst → {pipeline}")
            return cv2.VideoCapture(pipeline, cv2.CAP_GSTREAMER)
            # Note: appsink and the caps in the pipeline already enforce
            # resolution, framerate, and 1-frame buffering. Don't call
            # CAP_PROP_* on a GStreamer capture — those are V4L2-only.

        # ── Legacy CPU path ──────────────────────────────────────────────────
        if cfg.is_rtsp:
            # Set FFmpeg low-latency options transiently so we don't pollute env.
            prev = os.environ.get("OPENCV_FFMPEG_CAPTURE_OPTIONS")
            os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = (
                f"rtsp_transport;{cfg.rtsp_transport}"
                "|fflags;nobuffer|flags;low_delay|max_delay;0"
            )
            cap = cv2.VideoCapture(cfg.source, cv2.CAP_FFMPEG)
            if prev is None:
                os.environ.pop("OPENCV_FFMPEG_CAPTURE_OPTIONS", None)
            else:
                os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = prev
        else:
            cap = cv2.VideoCapture(cfg.source, cv2.CAP_V4L2)

        if cap.isOpened():
            cap.set(cv2.CAP_PROP_FRAME_WIDTH,  cfg.width)
            cap.set(cv2.CAP_PROP_FRAME_HEIGHT, cfg.height)
            cap.set(cv2.CAP_PROP_FPS,          cfg.fps)
            try:
                # 1-frame kernel buffer — never accumulate stale frames
                cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
            except Exception:
                pass
        return cap

    # ── GStreamer pipeline builders (Jetson hardware path) ───────────────────

    @staticmethod
    def _build_gst_rtsp_pipeline(cfg: CameraConfig) -> str:
        """
        RTSP H.264 → NVDEC → VIC resize → BGR for OpenCV appsink.

        latency=0 disables the rtspsrc jitter buffer (we want freshest frame).
        protocols pins TCP/UDP per config (matches rtsp_transport).
        nvvidconv caps force the output resolution via VIC.
        videoconvert at the tail is BGRx→BGR on CPU (~1ms at 640x480).
        appsink drop=true max-buffers=1 reproduces the 1-slot buffer semantics.
        """
        return (
            f"rtspsrc location={cfg.source} latency=0 protocols={cfg.rtsp_transport} ! "
            f"rtph264depay ! h264parse ! nvv4l2decoder ! "
            f"nvvidconv ! video/x-raw,format=BGRx,width={cfg.width},height={cfg.height} ! "
            f"videoconvert ! video/x-raw,format=BGR ! "
            f"appsink drop=true max-buffers=1 sync=false"
        )

    @staticmethod
    def _build_gst_v4l2_mjpeg_pipeline(cfg: CameraConfig) -> str:
        """
        USB camera (MJPEG) → hw JPEG decoder → VIC → BGR for OpenCV appsink.

        Requires the camera to advertise MJPG at the requested resolution
        AND framerate — cameras only allow discrete combos:
            v4l2-ctl --list-formats-ext -d <device>
        Requires gst-inspect-1.0 nvv4l2decoder to list the 'mjpeg' property.
        io-mode=2 selects DMABUF transfer from the V4L2 driver (lower CPU).
        """
        return (
            f"v4l2src device={cfg.source} io-mode=2 ! "
            f"image/jpeg,width={cfg.width},height={cfg.height},framerate={cfg.fps}/1 ! "
            f"nvv4l2decoder mjpeg=1 ! "
            f"nvvidconv ! video/x-raw,format=BGRx ! "
            f"videoconvert ! video/x-raw,format=BGR ! "
            f"appsink drop=true max-buffers=1 sync=false"
        )

    @staticmethod
    def _build_gst_v4l2_yuyv_pipeline(cfg: CameraConfig) -> str:
        """
        USB camera (raw YUYV) → VIC colorspace convert → BGR for OpenCV appsink.

        No decode involved (YUYV is uncompressed YUY2). The first nvvidconv
        pulls raw YUY2 from CPU and does YUY2→BGRx on the VIC hardware block.
        The final videoconvert is a cheap BGRx→BGR pack on CPU.

        As with MJPEG, width/height/fps must match a row in the camera's
        format table — cameras advertise discrete combinations only.
        """
        return (
            f"v4l2src device={cfg.source} io-mode=2 ! "
            f"video/x-raw,format=YUY2,width={cfg.width},height={cfg.height},framerate={cfg.fps}/1 ! "
            f"nvvidconv ! video/x-raw,format=BGRx ! "
            f"videoconvert ! video/x-raw,format=BGR ! "
            f"appsink drop=true max-buffers=1 sync=false"
        )

    def _ensure_publisher(self, frame: np.ndarray) -> None:
        """Lazily create the frame-bus publisher using the actual decoded frame shape."""
        if not self._publish_enabled or self._publisher is not None:
            return
        if frame.ndim != 3:
            log("cameras", f"{self.name}: bus disabled — unexpected frame.ndim={frame.ndim}")
            self._publish_enabled = False
            return
        try:
            from LAB.utils.frame_bus import FrameBusPublisher
            h, w, c = frame.shape
            self._publisher = FrameBusPublisher(self.name, height=h, width=w, channels=c)
            log("cameras",
                f"{self.name}: frame bus → /dev/shm/{self._publisher.name} ({w}x{h}x{c})")
        except Exception as exc:
            log("cameras", f"{self.name}: frame bus init failed: {exc}")
            self._publish_enabled = False

    def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            cap = self._open_capture()
            if not cap.isOpened():
                cap.release()
                self._stop.wait(timeout=backoff)
                backoff = min(backoff * 2, 10.0)
                continue
            backoff = 1.0

            while not self._stop.is_set():
                ok, frame = cap.read()
                if not ok or frame is None:
                    break   # stream ended → reconnect
                now = time.time()
                with self._lock:
                    self._frame = frame
                    self._ts = now

                # Fan-out to shared memory if enabled. This is one extra memcpy
                # per frame (~100 µs at 640x480x3) — negligible vs the 66 ms
                # frame budget at 15 fps. Any error here is non-fatal: it
                # disables the publisher but never blocks normal capture.
                if self._publish_enabled:
                    self._ensure_publisher(frame)
                    if self._publisher is not None:
                        try:
                            self._publisher.publish(frame, now)
                        except Exception as exc:
                            log("cameras", f"{self.name}: publish error: {exc}")
                            try: self._publisher.close()
                            except Exception: pass
                            self._publisher = None
                            self._publish_enabled = False

            cap.release()
            log("cameras", f"{self.name}: stream lost, reconnecting in {backoff:.1f}s")
            self._stop.wait(timeout=backoff)


class MultiCameraCapture:
    """Collection of named CameraCapture instances."""

    def __init__(self) -> None:
        self._cameras: dict[str, CameraCapture] = {}

    # ── construction ──────────────────────────────────────────────────────────

    @classmethod
    def from_configs(cls, configs: list) -> "MultiCameraCapture":
        mc = cls()
        for cfg in configs:
            mc._add(CameraCapture(cfg))
        return mc

    def _add(self, cam: CameraCapture) -> bool:
        if cam.start():
            self._cameras[cam.name] = cam
            return True
        return False

    # ── public access ─────────────────────────────────────────────────────────

    def names(self) -> list:
        return list(self._cameras.keys())

    def has(self, name: str) -> bool:
        return name in self._cameras

    def read(self, name: str) -> tuple[Optional[float], Optional[np.ndarray]]:
        """Return (timestamp, frame) for one camera. Public; safe to call freely."""
        cam = self._cameras.get(name)
        if cam is None:
            return None, None
        return cam.read_latest()

    def read_all(self) -> dict:
        """Return {name: (timestamp, frame)} for all cameras with available frames."""
        out: dict = {}
        for name, cam in self._cameras.items():
            ts, frame = cam.read_latest()
            if frame is not None and ts is not None:
                out[name] = (ts, frame)
        return out

    def stop_all(self) -> None:
        for cam in self._cameras.values():
            cam.stop()
        self._cameras.clear()