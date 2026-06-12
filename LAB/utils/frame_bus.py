# -*- coding: utf-8 -*-
"""
Frame bus — shared-memory fan-out for camera frames.

Teleop is the sole writer for each named bus; any number of external
processes can attach as readers without ever touching V4L2. The kernel
only lets one process own a /dev/videoN node, so this is how downstream
consumers (AI inference, debug tools, anything else) get frames without
fighting teleop for the camera.

Memory lives in /dev/shm, which is a tmpfs (pure RAM). A 640x480x3 BGR
frame is ~900 KB; a memcpy at that size is ~100 µs on a Jetson — about
0.15% of one 15 fps frame budget. Publishing is effectively free.

Layout (one shared-memory region per camera):
    offset  size  field        notes
    0       8     seq          uint64 — odd while writing, even when stable
    8       8     frame_idx    uint64 — monotonic publish counter
    16      8     timestamp    float64 — unix time of capture
    24      4     height       uint32
    28      4     width        uint32
    32      4     channels     uint32
    36      4     dtype_code   uint32 — 0 = uint8 (BGR)
    40     24     padding      align payload to 64 bytes
    64     ...    pixel bytes  height * width * channels

Concurrency:
    The writer uses a seqlock — bump seq to odd, write payload + header
    fields, bump seq to even. Readers sample seq before and after copying
    the payload; if both reads match and are even, the copy is consistent.
    Aligned 8-byte writes are atomic on x86_64 and aarch64, so no kernel
    primitives are involved.

Usage (writer, inside teleop):
    pub = FrameBusPublisher("ai_front", height=480, width=640, channels=3)
    pub.publish(frame_bgr, timestamp)
    pub.close()        # closes the region; unlinks /dev/shm/lab_ai_front

Usage (reader, in any other process):
    rdr = FrameBusReader("ai_front")
    ts, frame = rdr.read_latest()       # (None, None) if no frame yet
    rdr.close()
"""

from __future__ import annotations

import time
from multiprocessing import shared_memory
from typing import Optional

import numpy as np


# ── Layout constants ──────────────────────────────────────────────────────────

_SHM_PREFIX  = "lab_"     # /dev/shm/lab_<camera_name>
_HEADER_SIZE = 64         # bytes, payload starts at offset 64

# Field offsets within the header (in bytes)
_OFF_SEQ        = 0
_OFF_FRAME_IDX  = 8
_OFF_TIMESTAMP  = 16
_OFF_SHAPE      = 24   # 4 x uint32: height, width, channels, dtype_code


def _region_name(camera_name: str) -> str:
    return f"{_SHM_PREFIX}{camera_name}"


# ── Publisher ────────────────────────────────────────────────────────────────

class FrameBusPublisher:
    """Writer side. Owns a /dev/shm region for one camera."""

    def __init__(self, camera_name: str, height: int, width: int, channels: int = 3) -> None:
        self._name     = _region_name(camera_name)
        self._height   = int(height)
        self._width    = int(width)
        self._channels = int(channels)

        size = _HEADER_SIZE + self._height * self._width * self._channels

        # Clean up any stale region left behind by a previous crashed teleop.
        try:
            stale = shared_memory.SharedMemory(name=self._name, create=False)
            stale.close()
            stale.unlink()
        except FileNotFoundError:
            pass

        self._shm = shared_memory.SharedMemory(name=self._name, create=True, size=size)

        # Scalar views into the header — direct memory, no copies.
        buf = self._shm.buf
        self._seq       = np.ndarray((1,), dtype=np.uint64,  buffer=buf, offset=_OFF_SEQ)
        self._frame_idx = np.ndarray((1,), dtype=np.uint64,  buffer=buf, offset=_OFF_FRAME_IDX)
        self._timestamp = np.ndarray((1,), dtype=np.float64, buffer=buf, offset=_OFF_TIMESTAMP)
        self._shape     = np.ndarray((4,), dtype=np.uint32,  buffer=buf, offset=_OFF_SHAPE)

        # Payload view — the actual pixel array.
        self._payload = np.ndarray(
            (self._height, self._width, self._channels),
            dtype=np.uint8,
            buffer=buf,
            offset=_HEADER_SIZE,
        )

        # Initialize header. seq=0 signals "no frame published yet".
        self._seq[0]       = 0
        self._frame_idx[0] = 0
        self._timestamp[0] = 0.0
        self._shape[0]     = self._height
        self._shape[1]     = self._width
        self._shape[2]     = self._channels
        self._shape[3]     = 0   # dtype_code: 0 = uint8

        self._counter = 0   # local frame counter (cheaper than reading back)
        self._closed  = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def shape(self) -> tuple[int, int, int]:
        return (self._height, self._width, self._channels)

    def publish(self, frame: np.ndarray, timestamp: float) -> None:
        """Publish a frame. Caller must pass an array with the configured shape & dtype."""
        if self._closed:
            return
        if frame.shape != (self._height, self._width, self._channels):
            raise ValueError(
                f"frame shape {frame.shape} != region shape "
                f"({self._height}, {self._width}, {self._channels})"
            )
        if frame.dtype != np.uint8:
            raise ValueError(f"frame dtype {frame.dtype} != uint8")

        self._counter += 1

        # Seqlock: bump to odd → write payload + metadata → bump to even.
        # CPython's GIL plus aligned 8-byte stores on x86_64/aarch64 give us
        # enough ordering for readers to see a consistent snapshot.
        seq_odd = np.uint64(self._counter * 2 - 1)
        self._seq[0] = seq_odd

        # np.copyto is a straight memcpy into the existing buffer.
        np.copyto(self._payload, frame)
        self._frame_idx[0] = np.uint64(self._counter)
        self._timestamp[0] = float(timestamp)

        self._seq[0] = np.uint64(self._counter * 2)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True

        # Drop numpy views before closing the buffer — otherwise the BufferError
        # "memoryview has N exported buffers" fires on .close().
        self._seq       = None  # type: ignore[assignment]
        self._frame_idx = None  # type: ignore[assignment]
        self._timestamp = None  # type: ignore[assignment]
        self._shape     = None  # type: ignore[assignment]
        self._payload   = None  # type: ignore[assignment]

        try:
            self._shm.close()
        except Exception:
            pass
        try:
            self._shm.unlink()
        except Exception:
            pass


# ── Reader ───────────────────────────────────────────────────────────────────

class FrameBusReader:
    """Reader side. Attaches to a region written by teleop."""

    def __init__(self, camera_name: str) -> None:
        self._name    = _region_name(camera_name)
        self._shm: Optional[shared_memory.SharedMemory] = None
        self._seq       = None
        self._frame_idx = None
        self._timestamp = None
        self._shape     = None
        self._payload   = None
        self._last_idx  = 0

        # Try to attach now; if teleop hasn't started yet, read_latest()
        # will retry transparently.
        self._try_attach()

    # ── attach/detach ────────────────────────────────────────────────────────

    def _try_attach(self) -> bool:
        if self._shm is not None:
            return True
        try:
            self._shm = shared_memory.SharedMemory(name=self._name, create=False)
        except FileNotFoundError:
            return False

        buf = self._shm.buf
        self._seq       = np.ndarray((1,), dtype=np.uint64,  buffer=buf, offset=_OFF_SEQ)
        self._frame_idx = np.ndarray((1,), dtype=np.uint64,  buffer=buf, offset=_OFF_FRAME_IDX)
        self._timestamp = np.ndarray((1,), dtype=np.float64, buffer=buf, offset=_OFF_TIMESTAMP)
        self._shape     = np.ndarray((4,), dtype=np.uint32,  buffer=buf, offset=_OFF_SHAPE)
        return True

    def _ensure_payload(self) -> bool:
        """Set up the payload view once we know the writer's shape."""
        if self._payload is not None:
            return True
        if self._shape is None:
            return False
        h = int(self._shape[0])
        w = int(self._shape[1])
        c = int(self._shape[2])
        if h == 0 or w == 0 or c == 0:
            return False
        self._payload = np.ndarray(
            (h, w, c), dtype=np.uint8, buffer=self._shm.buf, offset=_HEADER_SIZE,
        )
        return True

    # ── public API ───────────────────────────────────────────────────────────

    @property
    def attached(self) -> bool:
        return self._shm is not None

    def shape(self) -> Optional[tuple[int, int, int]]:
        if self._shape is None:
            return None
        return (int(self._shape[0]), int(self._shape[1]), int(self._shape[2]))

    def read_latest(self, max_attempts: int = 4) -> tuple[Optional[float], Optional[np.ndarray]]:
        """Return (timestamp, frame_copy) or (None, None) if no fresh frame is available."""
        if not self._try_attach():
            return None, None
        if not self._ensure_payload():
            return None, None

        for _ in range(max_attempts):
            s1 = int(self._seq[0])
            if s1 == 0:
                return None, None             # writer has never published
            if s1 & 1:
                # Writer is mid-publish — yield briefly and retry.
                time.sleep(0.0002)
                continue

            ts    = float(self._timestamp[0])
            frame = self._payload.copy()       # snapshot out of shared memory
            s2 = int(self._seq[0])
            if s1 == s2:
                self._last_idx = int(self._frame_idx[0])
                return ts, frame
            # Torn read — writer raced us. Retry.

        return None, None

    def read_new(self, max_attempts: int = 4) -> tuple[Optional[float], Optional[np.ndarray]]:
        """Like read_latest but returns (None, None) if the frame hasn't advanced."""
        if not self._try_attach() or not self._ensure_payload():
            return None, None
        if int(self._frame_idx[0]) == self._last_idx:
            return None, None
        return self.read_latest(max_attempts=max_attempts)

    def close(self) -> None:
        self._seq       = None
        self._frame_idx = None
        self._timestamp = None
        self._shape     = None
        self._payload   = None
        if self._shm is not None:
            try:
                self._shm.close()
            except Exception:
                pass
            self._shm = None
        # Reader never calls unlink — only the writer owns the region's lifetime.