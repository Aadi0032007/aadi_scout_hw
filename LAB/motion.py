# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations

"""
motion.py — UDP forward to segway_ros1 Docker + human/AI source arbitration.

Changes vs previous version:
    - New runtime toggle for the lidar safety brake, driven by WS bubble_mode:
        motion.set_lidar_block_enabled(bool)   # runtime on/off
        motion.lidar_block_enabled() -> bool
      cfg.lidar_safety_brake is now the INITIAL value only.
    - Watchdog contract is documented: silence zeros velocity (brake in
      place); it does NOT latch robot_lock. Lock is owned by the WS channel
      in the new architecture — UDP silence just brakes.

Transport (unchanged):
    JSON {"lin_x", "ang_z"} → udp://docker_host:docker_port at publish_hz.

Arbitration (unchanged):
    Two command sources arbitrated internally with HARD HUMAN PRIORITY.
    - Human commands win during handback window opened by meaningful input.
    - AI honored only if set_ai_enabled(True) AND handback closed.
    - Human brake hard-latches AI off.
    - Human always authoritative for lock and brake.
    - Watchdog + lidar_block_fn apply to the selected source.
    - Lidar gate now respects _lidar_block_enabled (see set_lidar_block_enabled).

Public API (back-compatible):
    command(lin_x, ang_z, locked, braking, origin="human")
    set_ai_enabled(on: bool)
    ai_enabled() -> bool
    is_ai_enabled() -> bool
    human_in_control() -> bool
    state() -> (lin_x, ang_z, locked, braking)
    published_state() -> (lin_x, ang_z)
    set_lidar_block_enabled(on: bool)      # NEW
    lidar_block_enabled() -> bool          # NEW
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
        docker_host:         str   = "127.0.0.1",
        docker_port:         int   = 56000,
        publish_hz:          int   = 50,
        watchdog_sec:        float = 0.30,
        ang_z_scale:         float = 0.20,
        lidar_block_fn:      Optional[Callable[[float], bool]] = None,
        lidar_block_enabled: bool  = True,
        # ── human-priority arbiter knobs ─────────────────────────────────
        human_handback_sec:  float = 2.0,
        human_idle_deadband: float = 0.05,
        human_stale_timeout: float = 2.0,
    ) -> None:
        self._docker_host    = docker_host
        self._docker_port    = docker_port
        self._publish_hz     = max(1, publish_hz)
        self._watchdog       = watchdog_sec
        self._ang_z_scale    = ang_z_scale
        self._lidar_block_fn = lidar_block_fn

        # ── State (protected by _lock) ────────────────────────────────────
        self._lock = threading.Lock()

        # Per-origin latest command
        self._latest_human: tuple[float, float, bool, bool, float] = (
            0.0, 0.0, True, False, 0.0
        )
        self._latest_ai:    tuple[float, float, bool, bool, float] = (
            0.0, 0.0, False, False, 0.0
        )

        # AI control gate (latched off by default; flipped by explicit call).
        self._ai_enabled = False

        # Runtime lidar brake gate. Toggled at runtime by
        # motion.set_lidar_block_enabled() driven by WS bubble_mode.
        self._lidar_block_enabled = bool(lidar_block_enabled)

        # Handback window
        self._human_active_until  = 0.0
        self._human_handback_sec  = float(human_handback_sec)
        self._human_idle_db       = float(human_idle_deadband)
        self._human_stale_timeout = float(human_stale_timeout)

        # Last values actually sent to Docker — for the recorder.
        self._last_pub_lin: float = 0.0
        self._last_pub_ang: float = 0.0

        # UDP transport
        self._sock: Optional[socket.socket] = None
        self._stop = threading.Event()
        self._pub_thread = threading.Thread(
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
                f"(watchdog={self._watchdog*1000:.0f}ms, ang_scale={self._ang_z_scale}, "
                f"handback={self._human_handback_sec}s, idle_db={self._human_idle_db}, "
                f"human_stale={self._human_stale_timeout}s, "
                f"lidar_gate={self._lidar_block_enabled})"
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

    def command(
        self,
        lin_x:   float,
        ang_z:   float,
        locked:  bool,
        braking: bool,
        origin:  str = "human",
    ) -> None:
        now = time.monotonic()
        with self._lock:
            if origin == "ai":
                self._latest_ai = (float(lin_x), float(ang_z),
                                   bool(locked), bool(braking), now)
                return

            # Human path
            self._latest_human = (float(lin_x), float(ang_z),
                                  bool(locked), bool(braking), now)

            meaningful = (
                abs(lin_x) >= self._human_idle_db
                or abs(ang_z) >= self._human_idle_db
                or bool(braking)
            )
            if meaningful:
                self._human_active_until = now + self._human_handback_sec

            if braking:
                if self._ai_enabled:
                    log("motion", "AI disabled by human brake")
                self._ai_enabled = False

    def set_ai_enabled(self, on: bool) -> None:
        with self._lock:
            prev = self._ai_enabled
            self._ai_enabled = bool(on)
            if not on:
                self._latest_ai = (0.0, 0.0, False, False, 0.0)
            if prev != self._ai_enabled:
                log("motion", f"ai_enabled -> {self._ai_enabled}")

    def ai_enabled(self) -> bool:
        with self._lock:
            return self._ai_enabled

    def is_ai_enabled(self) -> bool:
        return self.ai_enabled()

    def set_lidar_block_enabled(self, on: bool) -> None:
        """Runtime toggle for the lidar safety brake (bubble_mode from browser).

        When off, the drivetrain no longer consults lidar_block_fn — motion
        proceeds regardless of proximity readings. Human brake and watchdog
        still apply. Off by default in cfg.lidar_safety_brake; the browser
        owns the runtime state.
        """
        with self._lock:
            prev = self._lidar_block_enabled
            self._lidar_block_enabled = bool(on)
            if prev != self._lidar_block_enabled:
                log("motion",
                    f"lidar_block_enabled -> {self._lidar_block_enabled}")

    def lidar_block_enabled(self) -> bool:
        with self._lock:
            return self._lidar_block_enabled

    def human_in_control(self) -> bool:
        with self._lock:
            return (time.monotonic() < self._human_active_until) or (not self._ai_enabled)

    def state(self) -> tuple[float, float, bool, bool]:
        with self._lock:
            now = time.monotonic()
            h_lin, h_ang, h_locked, h_brake, h_t = self._latest_human
            a_lin, a_ang, _a_lk, _a_br, a_t      = self._latest_ai

            handback_active  = now < self._human_active_until
            human_in_control = handback_active or (not self._ai_enabled)

            if human_in_control:
                src_lin, src_ang, src_t = h_lin, h_ang, h_t
            else:
                src_lin, src_ang, src_t = a_lin, a_ang, a_t

            if (now - src_t) >= self._watchdog:
                src_lin, src_ang = 0.0, 0.0

            return src_lin, src_ang, h_locked, h_brake

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
            now = time.monotonic()
            h_lin, h_ang, h_locked, h_brake, h_t = self._latest_human
            a_lin, a_ang, _a_lk, _a_br,  a_t    = self._latest_ai

            # Safety latch: no human packet for too long while AI is enabled
            if (self._ai_enabled and h_t > 0.0
                    and (now - h_t) > self._human_stale_timeout):
                log("motion",
                    f"AI auto-disabled: no human packet for {now - h_t:.1f}s "
                    f"(gamepad disconnect?)")
                self._ai_enabled = False
                self._latest_ai  = (0.0, 0.0, False, False, 0.0)

            ai_enabled      = self._ai_enabled
            handback_active = now < self._human_active_until
            human_in_control = handback_active or (not ai_enabled)

            if human_in_control:
                src_lin, src_ang, src_t = h_lin, h_ang, h_t
            else:
                src_lin, src_ang, src_t = a_lin, a_ang, a_t

            # Per-source watchdog — if the selected source hasn't published
            # within _watchdog seconds, zero. We do NOT fall back to the
            # other source: the selected source going stale is suspicious
            # and silence is the safe default. NOTE: this zeroes velocity
            # (i.e. "apply brake in place") — it does NOT latch robot_lock.
            # In the new architecture the WS channel owns robot_lock; UDP
            # silence just brakes so the operator can resume from browser or
            # after restarting their gamepad.
            watchdog_ok = (now - src_t) < self._watchdog

            # Human is authoritative for lock + brake regardless of who drives.
            locked  = h_locked
            braking = h_brake

            lin_x = src_lin
            ang_z = src_ang * self._ang_z_scale

            lidar_gate_on = self._lidar_block_enabled

        if not watchdog_ok or locked or braking:
            return 0.0, 0.0

        # Lidar safety gate — runtime-togglable via set_lidar_block_enabled.
        if self._lidar_block_fn is not None and lidar_gate_on:
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