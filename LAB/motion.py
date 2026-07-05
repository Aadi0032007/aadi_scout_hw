# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations

"""
motion.py — REDESIGN delta.

Changes from previous version:
    + set_ai_enabled(bool) / is_ai_enabled() — flag driven by the gamepad's
      ai_request field in the motion UDP payload. Currently exposed only
      for telemetry; motion output is not gated on it yet. Add gating in
      _compute_output() when ready.

Everything else (UDP forward to Docker, watchdog, lidar gate, published_state)
is unchanged.
"""

import json
import socket
import threading
import time
from typing import Callable, Optional

from .common import log


class MotionController:
    def __init__(
        self,
        docker_host:      str   = "127.0.0.1",
        docker_port:      int   = 56000,
        publish_hz:       int   = 50,
        watchdog_sec:     float = 0.30,
        ang_z_scale:      float = 0.20,
        lidar_block_fn:   Optional[Callable[[float], bool]] = None,
    ) -> None:
        self._docker_host   = docker_host
        self._docker_port   = docker_port
        self._publish_hz    = max(1, publish_hz)
        self._watchdog      = watchdog_sec
        self._ang_z_scale   = ang_z_scale
        self._lidar_block_fn = lidar_block_fn

        self._lock          = threading.Lock()
        self._lin_x         = 0.0
        self._ang_z         = 0.0
        self._locked        = True
        self._braking       = False
        self._last_cmd_t    = 0.0
        self._ai_enabled    = False   # NEW — driven by gamepad ai_request field

        self._last_pub_lin: float = 0.0
        self._last_pub_ang: float = 0.0

        self._sock: Optional[socket.socket] = None
        self._stop          = threading.Event()
        self._pub_thread    = threading.Thread(
            target=self._publish_loop, daemon=True, name="motion-pub"
        )

    # ── lifecycle ───────────────────────────────────────────────────────────

    def start(self) -> None:
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._pub_thread.start()
            log(
                "motion",
                f"forwarding → udp://{self._docker_host}:{self._docker_port} "
                f"@ {self._publish_hz} Hz "
                f"(watchdog={self._watchdog*1000:.0f}ms, ang_scale={self._ang_z_scale})"
            )
        except Exception as exc:
            log("motion", f"start failed: {exc}")

    def stop(self) -> None:
        self._stop.set()
        try:
            self._pub_thread.join(timeout=1.0)
        except Exception:
            pass
        for _ in range(3):
            self._send_twist(0.0, 0.0)
            time.sleep(0.02)
        if self._sock is not None:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None

    # ── public API ──────────────────────────────────────────────────────────

    def command(self, lin_x: float, ang_z: float, locked: bool, braking: bool) -> None:
        with self._lock:
            self._lin_x      = float(lin_x)
            self._ang_z      = float(ang_z)
            self._locked     = bool(locked)
            self._braking    = bool(braking)
            self._last_cmd_t = time.monotonic()

    def set_ai_enabled(self, enabled: bool) -> None:
        """Called by teleop when gamepad packet contains ai_request=='enable'.

        Currently only tracked for telemetry. To gate motion, add a check in
        _compute_output() (e.g. if self._ai_enabled and origin != 'ai': ...).
        """
        with self._lock:
            if enabled != self._ai_enabled:
                self._ai_enabled = enabled
                log("motion", f"ai_enabled={enabled}")

    def is_ai_enabled(self) -> bool:
        with self._lock:
            return self._ai_enabled

    def state(self) -> tuple[float, float, bool, bool]:
        with self._lock:
            return self._lin_x, self._ang_z, self._locked, self._braking

    def published_state(self) -> tuple[float, float]:
        with self._lock:
            return self._last_pub_lin, self._last_pub_ang

    # ── publisher loop ──────────────────────────────────────────────────────

    def _publish_loop(self) -> None:
        interval = 1.0 / self._publish_hz
        while not self._stop.is_set():
            lin, ang = self._compute_output()
            self._send_twist(lin, ang)
            with self._lock:
                self._last_pub_lin = lin
                self._last_pub_ang = ang
            self._stop.wait(timeout=interval)

    def _compute_output(self) -> tuple[float, float]:
        with self._lock:
            now         = time.monotonic()
            watchdog_ok = (now - self._last_cmd_t) < self._watchdog
            locked      = self._locked
            braking     = self._braking
            lin_x       = self._lin_x
            ang_z       = self._ang_z * self._ang_z_scale

        if not watchdog_ok or locked or braking:
            return 0.0, 0.0

        if self._lidar_block_fn is not None:
            try:
                if self._lidar_block_fn(lin_x):
                    return 0.0, 0.0
            except Exception as exc:
                log("motion", f"lidar_block_fn error: {exc}")

        return lin_x, ang_z

    def _send_twist(self, lin: float, ang: float) -> None:
        if self._sock is None:
            return
        try:
            payload = json.dumps({"lin_x": lin, "ang_z": ang}).encode()
            self._sock.sendto(payload, (self._docker_host, self._docker_port))
        except Exception:
            pass