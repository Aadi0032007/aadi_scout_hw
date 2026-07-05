# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations

"""
cameras.py — REDESIGN.

Two responsibilities merged into one module:

    1. USB capture (single owner of /dev/videoN)
        - Reads BGR frames from V4L2
        - Publishes to /dev/shm/lab_<name> (frame bus) for AI + record + any
          external consumer
        - Also holds each frame in a 1-slot in-memory latest-frame slot
          for the in-process record.py to read directly and for the
          RTSP appsrc feeder to push into GStreamer

    2. In-process GstRtspServer (port 8556)
        - RTSP passthrough factory for every stream_only=True camera
          (rtspsrc → rtph264depay → h264parse → rtph264pay), plus optional
          audio branch identical to the standalone gst_rtsp.py
        - USB appsrc factory for the USB camera:
              appsrc → videoconvert → (x264enc | nvv4l2h264enc) → rtph264pay
          fed by a Python thread that reads the 1-slot latest-frame slot
          and drops if the encoder is behind. Capture thread is never
          blocked by streaming problems.

Isolation guarantee:
    Record.py and the AI frame bus reader are single-process reads of the
    1-slot / of shared memory. Neither is on the encoder's backpressure
    path. If MediaMTX stops pulling or x264enc stalls, the RTSP feeder's
    push_buffer will fail, the feeder drops the frame, and capture keeps
    running at full rate.

Watchdog:
    Same pattern as the standalone — count CLOSE_WAIT on the RTSP port,
    os._exit(1) if the threshold is crossed. systemd Restart=on-failure
    brings teleop back in seconds. In-place restart of the GstRtspServer
    subsystem is theoretically possible but the C-side cleanup is
    unreliable in practice; keeping the proven hard-restart pattern.
"""

import os
import threading
import time
from typing import Optional
from urllib.parse import quote

import cv2
import numpy as np

from .common import log
from .config import CameraConfig, LabConfig


# ══════════════════════════════════════════════════════════════════════════════
#  USB capture — sole owner of /dev/videoN
# ══════════════════════════════════════════════════════════════════════════════

class UsbCameraCapture:
    """Background reader for one V4L2 device.

    Writes each decoded frame to:
        - a 1-slot latest-frame slot (thread-safe, read by record.py and
          the RTSP appsrc feeder)
        - the frame bus (if publish_frames=True), for out-of-process AI

    Never blocks the capture loop on any consumer.
    """

    def __init__(self, cfg: CameraConfig) -> None:
        self.name  = cfg.name
        self._cfg  = cfg

        # 1-slot latest-frame slot — lossy on purpose
        self._frame:  Optional[np.ndarray] = None
        self._ts:     float = 0.0
        self._lock    = threading.Lock()
        self._stop    = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # Frame bus publisher (lazy, created on first frame)
        self._publish_enabled = bool(getattr(cfg, "publish_frames", False))
        self._publisher = None

    # ── public API ──────────────────────────────────────────────────────────

    def start(self) -> bool:
        cap = self._open_capture()
        ok = cap.isOpened()
        cap.release()
        if not ok:
            log("cameras", f"{self.name}: cannot open {self._cfg.source!r} — skipping")
            return False
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name=f"cam-{self.name}")
        self._thread.start()
        log("cameras",
            f"{self.name}: USB started "
            f"{self._cfg.width}x{self._cfg.height}@{self._cfg.fps}fps"
            f"{' [bus]' if self._publish_enabled else ''}")
        return True

    def read_latest(self) -> tuple[Optional[float], Optional[np.ndarray]]:
        """Return (timestamp, frame) from the 1-slot slot. Non-blocking.

        The returned frame is NOT copied — callers that intend to mutate it
        must copy first. record.py already copies before encoding; the
        appsrc feeder copies into a Gst.Buffer.
        """
        with self._lock:
            if self._frame is None:
                return None, None
            return self._ts, self._frame

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

    # ── capture loop ────────────────────────────────────────────────────────

    def _open_capture(self) -> cv2.VideoCapture:
        cfg = self._cfg
        if cfg.hw_decode:
            fmt = cfg.pixel_format.upper()
            if fmt == "YUYV":
                pipeline = (
                    f"v4l2src device={cfg.source} io-mode=2 ! "
                    f"video/x-raw,format=YUY2,width={cfg.width},height={cfg.height},"
                    f"framerate={cfg.fps}/1 ! "
                    f"nvvidconv ! video/x-raw,format=BGRx ! "
                    f"videoconvert ! video/x-raw,format=BGR ! "
                    f"appsink drop=true max-buffers=1 sync=false"
                )
            else:
                pipeline = (
                    f"v4l2src device={cfg.source} io-mode=2 ! "
                    f"image/jpeg,width={cfg.width},height={cfg.height},"
                    f"framerate={cfg.fps}/1 ! "
                    f"nvv4l2decoder mjpeg=1 ! "
                    f"nvvidconv ! video/x-raw,format=BGRx ! "
                    f"videoconvert ! video/x-raw,format=BGR ! "
                    f"appsink drop=true max-buffers=1 sync=false"
                )
            log("cameras", f"{self.name}: gst → {pipeline}")
            return cv2.VideoCapture(pipeline, cv2.CAP_GSTREAMER)

        cap = cv2.VideoCapture(cfg.source, cv2.CAP_V4L2)
        if cap.isOpened():
            cap.set(cv2.CAP_PROP_FRAME_WIDTH,  cfg.width)
            cap.set(cv2.CAP_PROP_FRAME_HEIGHT, cfg.height)
            cap.set(cv2.CAP_PROP_FPS,          cfg.fps)
            try:
                cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
            except Exception:
                pass
        return cap

    def _ensure_publisher(self, frame: np.ndarray) -> None:
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
                    break
                now = time.time()

                # 1-slot slot for in-process consumers (record + RTSP feeder)
                with self._lock:
                    self._frame = frame
                    self._ts = now

                # Frame bus for out-of-process consumers (AI worker)
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


# ══════════════════════════════════════════════════════════════════════════════
#  In-process GstRtspServer
# ══════════════════════════════════════════════════════════════════════════════

class RtspServer:
    """Runs GstRtspServer in a background GLib thread.

    Mounts:
        /<name> for each stream_only=True camera → rtsp passthrough
        /<usb_stream_mount> for the USB camera → appsrc + encoder
    """

    def __init__(
        self,
        cfg: LabConfig,
        usb_capture: Optional[UsbCameraCapture],
    ) -> None:
        self._cfg = cfg
        self._usb = usb_capture
        self._thread: Optional[threading.Thread] = None
        self._loop = None  # GLib.MainLoop
        self._server = None
        self._stop = threading.Event()

    # ── lifecycle ───────────────────────────────────────────────────────────

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True, name="rtsp-server")
        self._thread.start()
        threading.Thread(target=self._watchdog_loop, daemon=True, name="rtsp-watchdog").start()

    def stop(self) -> None:
        self._stop.set()
        if self._loop is not None:
            try:
                self._loop.quit()
            except Exception:
                pass

    # ── GLib main loop ──────────────────────────────────────────────────────

    def _run(self) -> None:
        try:
            import gi
            gi.require_version("Gst", "1.0")
            gi.require_version("GstRtspServer", "1.0")
            from gi.repository import GLib, Gst, GstRtspServer
        except Exception as exc:
            log("rtsp", f"gi/gstreamer import failed: {exc} — RTSP server disabled")
            return

        Gst.init(None)
        self._server = GstRtspServer.RTSPServer.new()
        self._server.props.address = self._cfg.gst_rtsp_bind
        self._server.props.service = str(self._cfg.gst_rtsp_port)
        mounts = self._server.get_mount_points()

        # ── RTSP passthrough mounts ────────────────────────────────────────
        for cam in self._cfg.cameras:
            if not cam.stream_only:
                continue
            factory = GstRtspServer.RTSPMediaFactory.new()
            factory.set_launch(self._rtsp_pipeline(cam))
            factory.set_shared(True)
            mounts.add_factory(f"/{cam.name}", factory)
            log("rtsp", f"mount /{cam.name} ← {cam.source} ({cam.rtsp_transport})")

        # ── USB appsrc mount ───────────────────────────────────────────────
        if self._usb is not None:
            factory = GstRtspServer.RTSPMediaFactory.new()
            factory.set_launch(self._usb_pipeline())
            factory.set_shared(True)
            factory.connect("media-configure", self._on_usb_media_configure)
            mount = f"/{self._cfg.usb_stream_mount}"
            mounts.add_factory(mount, factory)
            log("rtsp", f"mount {mount} ← USB {self._usb.name} "
                       f"({'nvv4l2h264enc' if self._cfg.gst_hw_encode else 'x264enc'})")

        self._server.attach(None)
        log("rtsp", f"listening on {self._cfg.gst_rtsp_bind}:{self._cfg.gst_rtsp_port}")

        # Session cleanup timer — GstRtspServer doesn't do this on its own
        GLib.timeout_add_seconds(20, self._cleanup_sessions)

        self._loop = GLib.MainLoop()
        try:
            self._loop.run()
        except Exception as exc:
            log("rtsp", f"main loop crashed: {exc}")

    def _cleanup_sessions(self) -> bool:
        try:
            self._server.get_session_pool().cleanup()
        except Exception:
            pass
        return True

    # ── Pipelines ───────────────────────────────────────────────────────────

    def _rtsp_pipeline(self, cam: CameraConfig) -> str:
        source = self._expand_secret(cam.source)
        latency = 0 if cam.rtsp_transport == "tcp" else 100
        video = (
            f'rtspsrc location="{source}" protocols={cam.rtsp_transport} '
            f'latency={latency} name=src '
            'src. ! queue ! application/x-rtp,media=video,encoding-name=H264 '
            '! rtph264depay ! h264parse config-interval=-1 '
            '! rtph264pay name=pay0 config-interval=-1 pt=96 '
        )
        if not cam.audio:
            return video
        return video + (
            'src. ! queue leaky=downstream max-size-buffers=0 max-size-bytes=0 '
            'max-size-time=500000000 '
            '! application/x-rtp,media=audio ! decodebin ! audioconvert ! audioresample '
            '! opusenc bitrate=16000 complexity=3 audio-type=voice bandwidth=wideband '
            '! rtpopuspay name=pay1 pt=97'
        )

    def _usb_pipeline(self) -> str:
        """Appsrc pipeline for USB streaming. Frames arrive as BGR from Python."""
        cfg = self._cfg
        usb = self._usb
        w, h, fps = usb._cfg.width, usb._cfg.height, usb._cfg.fps

        appsrc_caps = (
            f"video/x-raw,format=BGR,width={w},height={h},framerate={fps}/1"
        )

        if cfg.gst_hw_encode:
            # Jetson NVENC path — BGR → NV12 → NVMM → nvv4l2h264enc
            encoder = (
                "videoconvert ! video/x-raw,format=NV12 ! "
                "nvvidconv ! 'video/x-raw(memory:NVMM),format=NV12' ! "
                f"nvv4l2h264enc bitrate={cfg.usb_stream_bitrate_bps} "
                "preset-level=1 profile=0 insert-sps-pps=1 iframeinterval=30 "
            )
        else:
            # Software x264enc — same settings as the standalone
            encoder = (
                "videoconvert ! "
                "x264enc tune=zerolatency speed-preset=superfast "
                f"bitrate={cfg.usb_stream_bitrate_kbps} key-int-max=30 ! "
                "video/x-h264,profile=baseline "
            )

        return (
            f"appsrc name=usb_src is-live=true format=time do-timestamp=true block=false "
            f"! {appsrc_caps} "
            f"! {encoder} "
            "! h264parse config-interval=-1 "
            "! rtph264pay name=pay0 config-interval=-1 pt=96"
        )

    def _on_usb_media_configure(self, factory, media):
        """Wire up the appsrc feeder each time a new media pipeline is created.

        With shared=True the same pipeline serves all clients, so the feeder
        is typically created once per server lifetime (or after the pipeline
        is torn down and rebuilt on last-client-leaves).
        """
        pipeline = media.get_element()
        appsrc = pipeline.get_by_name("usb_src")
        if appsrc is None:
            log("rtsp", "USB appsrc not found in pipeline")
            return
        feeder = _UsbAppsrcFeeder(self._usb, appsrc)
        feeder.start()
        # Stop the feeder when the media is unprepared
        media.connect("unprepared", lambda m: feeder.stop())

    # ── Secret expansion (parity with standalone) ───────────────────────────

    def _expand_secret(self, url: str) -> str:
        if "<camera-password>" in url:
            pw = self._cfg.camera_password or os.environ.get("CAMERA_PASSWORD", "")
            if not pw:
                log("rtsp", "camera_password unset — RTSP URL contains unfilled placeholder")
            url = url.replace("<camera-password>", quote(pw, safe=""))
        return url

    # ── Watchdog (identical to standalone) ──────────────────────────────────

    def _watchdog_loop(self) -> None:
        port = self._cfg.gst_rtsp_port
        interval = self._cfg.rtsp_close_wait_interval_sec
        threshold = self._cfg.rtsp_close_wait_max
        while not self._stop.is_set():
            self._stop.wait(timeout=interval)
            if self._stop.is_set():
                break
            stuck = _count_close_wait(port)
            if stuck > threshold:
                log("rtsp",
                    f"{stuck} CLOSE_WAIT connections on :{port} — os._exit(1) for systemd restart")
                os._exit(1)


class _UsbAppsrcFeeder:
    """Reads the USB capture's 1-slot slot, pushes into an appsrc, drops if full.

    Runs in its own thread. Never blocks capture — appsrc is set to
    block=false in the pipeline, so a full internal queue causes push_buffer
    to return GST_FLOW_OK-but-drop or an error which we ignore.
    """

    def __init__(self, usb: UsbCameraCapture, appsrc) -> None:
        self._usb = usb
        self._appsrc = appsrc
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_pushed_ts: float = -1.0

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True, name="usb-feeder")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        try:
            self._appsrc.emit("end-of-stream")
        except Exception:
            pass
        if self._thread is not None:
            self._thread.join(timeout=1.0)
            self._thread = None

    def _run(self) -> None:
        try:
            from gi.repository import Gst
        except Exception:
            return

        # Pace by capture rate — read the same fps as configured
        target_period = 1.0 / max(1.0, float(self._usb._cfg.fps))

        while not self._stop.is_set():
            t0 = time.time()
            ts, frame = self._usb.read_latest()
            if frame is not None and ts != self._last_pushed_ts:
                try:
                    # Zero-copy wrap of the numpy buffer into a Gst.Buffer.
                    # We .tobytes() to hand ownership to GStreamer safely —
                    # a full 640x480x3 memcpy is ~1ms, negligible next to encode.
                    data = frame.tobytes()
                    buf = Gst.Buffer.new_wrapped(data)
                    ret = self._appsrc.emit("push-buffer", buf)
                    if ret != Gst.FlowReturn.OK:
                        # Encoder / downstream in trouble — drop and continue.
                        # Common values: FLUSHING (shutdown), ERROR (encoder crashed).
                        # Not fatal for capture; the RTSP subsystem will recover
                        # on next client reconnect (or the watchdog restarts us).
                        pass
                    self._last_pushed_ts = ts
                except Exception:
                    pass

            elapsed = time.time() - t0
            self._stop.wait(timeout=max(0.0, target_period - elapsed))


# ══════════════════════════════════════════════════════════════════════════════
#  CamerasManager — top-level entry point for teleop
# ══════════════════════════════════════════════════════════════════════════════

class CamerasManager:
    """One-stop object teleop constructs and calls start/stop on.

    Holds the USB capture (if configured) and the RTSP server.
    """

    def __init__(self, cfg: LabConfig) -> None:
        self._cfg = cfg
        self._usb: Optional[UsbCameraCapture] = None
        self._rtsp: Optional[RtspServer] = None

    def start(self) -> None:
        # Find and start the USB (publish_frames / non-stream_only) camera
        for cam in self._cfg.cameras:
            if cam.stream_only:
                continue
            if cam.is_rtsp:
                log("cameras", f"{cam.name}: RTSP but not stream_only — ignored")
                continue
            usb = UsbCameraCapture(cam)
            if usb.start():
                self._usb = usb
                break   # only one USB camera supported today

        # RTSP server always starts — it may host passthrough mounts even
        # without a USB camera
        self._rtsp = RtspServer(self._cfg, self._usb)
        self._rtsp.start()

    def stop(self) -> None:
        if self._rtsp is not None:
            self._rtsp.stop()
            self._rtsp = None
        if self._usb is not None:
            self._usb.stop()
            self._usb = None

    # ── record.py integration ───────────────────────────────────────────────

    def read(self, name: str) -> tuple[Optional[float], Optional[np.ndarray]]:
        """Return (ts, frame) for the local USB camera. `name` is accepted
        for API compatibility with the previous MultiCameraCapture but only
        the USB camera is available locally now."""
        if self._usb is None or name != self._usb.name:
            return None, None
        return self._usb.read_latest()

    def has(self, name: str) -> bool:
        return self._usb is not None and name == self._usb.name

    def names(self) -> list:
        return [self._usb.name] if self._usb is not None else []


# ══════════════════════════════════════════════════════════════════════════════
#  /proc/net/tcp CLOSE_WAIT counter (identical to standalone)
# ══════════════════════════════════════════════════════════════════════════════

def _count_close_wait(port: int) -> int:
    port_hex = f"{port:04X}"
    count = 0
    for proc_file in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            with open(proc_file) as f:
                next(f)
                for line in f:
                    fields = line.split()
                    local_port = fields[1].split(":")[1]
                    state = fields[3]
                    if local_port == port_hex and state == "08":
                        count += 1
        except FileNotFoundError:
            continue
        except Exception:
            continue
    return count