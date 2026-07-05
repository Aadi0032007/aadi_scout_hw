# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations

"""
motion.py — UDP forward to segway_ros1 Docker + human/AI source arbitration.

Transport (unchanged from previous new version):
    JSON {"lin_x", "ang_z"} → udp://docker_host:docker_port at publish_hz.
    Docker-side keepalive republishes to /cmd_vel inside the container.

Arbitration (ported from the ROS2 version):

  Two command sources are arbitrated internally with HARD HUMAN PRIORITY:

  • Human commands always win during a "handback" window that opens on
    any meaningful human input and stays open for human_handback_sec
    (default 2.0s) past the last meaningful sample.
  • AI commands are only honored when BOTH:
        1. set_ai_enabled(True) was called (explicit human consent), AND
        2. the handback window is closed (human is idle).
  • Human BRAKE is a hard latch — it disables AI until an explicit
    set_ai_enabled(True) re-enables it.
  • Human is ALWAYS authoritative for lock and brake. AI cannot unlock
    and cannot un-brake.
  • Watchdog / lidar_block_fn apply to whichever source the arbiter
    picks. Lidar gates AI too — safety trumps source.

Public API (back-compatible; origin defaults to "human"):
    command(lin_x, ang_z, locked, braking, origin="human")
    set_ai_enabled(on: bool)
    ai_enabled() -> bool
    is_ai_enabled() -> bool          # alias, kept for existing callers
    human_in_control() -> bool
    state() -> (lin_x, ang_z, locked, braking)   # active source's pre-gate intent
    published_state() -> (lin_x, ang_z)          # last values actually sent

Recording note
--------------
Pass motion.published_state to the recorder, NOT motion.state.
published_state reflects whichever source the arbiter selected,
post-scale and post-gate — the same values that reached Docker → /cmd_vel.
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
        # ── human-priority arbiter knobs ─────────────────────────────────
        human_handback_sec:  float = 2.0,
        human_idle_deadband: float = 0.05,
        human_stale_timeout: float = 2.0,   # disable AI if no human packet for this long
    ) -> None:
        self._docker_host    = docker_host
        self._docker_port    = docker_port
        self._publish_hz     = max(1, publish_hz)
        self._watchdog       = watchdog_sec
        self._ang_z_scale    = ang_z_scale
        self._lidar_block_fn = lidar_block_fn

        # ── State (protected by _lock) ────────────────────────────────────
        self._lock = threading.Lock()

        # Per-origin latest command: (lin_x, ang_z, locked, braking, t_monotonic)
        # Human starts LOCKED (safety). AI starts inert AND gated off.
        self._latest_human: tuple[float, float, bool, bool, float] = (
            0.0, 0.0, True, False, 0.0
        )
        self._latest_ai:    tuple[float, float, bool, bool, float] = (
            0.0, 0.0, False, False, 0.0
        )

        # AI control gate. Latched off; flipped on only by an explicit
        # set_ai_enabled(True) call from teleop on the human's enable chord.
        # Flipped back off by human brake, human packet going stale, or an
        # explicit set_ai_enabled(False).
        self._ai_enabled = False

        # Handback window. While now < _human_active_until, human is "in
        # control" even if AI is publishing. Extended on every human
        # command above the idle deadband. After it expires AND AI is
        # enabled, AI resumes driving.
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
                f"human_stale={self._human_stale_timeout}s)"
            )
        except Exception as exc:
            log("motion", f"start failed: {exc}")

    def stop(self) -> None:
        self._stop.set()
        try:
            self._pub_thread.join(timeout=1.0)
        except Exception:
            pass
        # 3× zero for safety on shutdown
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
        """Update the latest command for one source.

        origin = "human" (default, back-compat) or "ai".

        Human path also:
          • extends the handback window on meaningful input,
          • latches AI off on brake.
        AI path only updates the latest-AI tuple — whether it actually
        drives Docker is decided each publish tick.
        """
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
                # HARD LATCH: human brake disables AI until explicitly re-enabled.
                if self._ai_enabled:
                    log("motion", "AI disabled by human brake")
                self._ai_enabled = False

    def set_ai_enabled(self, on: bool) -> None:
        """Toggle the AI control gate. Called by teleop on the human's
        explicit enable chord (and any explicit disable path)."""
        with self._lock:
            prev = self._ai_enabled
            self._ai_enabled = bool(on)
            if not on:
                # Wipe stale AI state so a fresh enable starts clean.
                self._latest_ai = (0.0, 0.0, False, False, 0.0)
            if prev != self._ai_enabled:
                log("motion", f"ai_enabled -> {self._ai_enabled}")

    def ai_enabled(self) -> bool:
        with self._lock:
            return self._ai_enabled

    # Alias — matches the pre-arbitration new-motion API so existing
    # telemetry callers (udp_telemetry, azure_telemetry) keep working.
    def is_ai_enabled(self) -> bool:
        return self.ai_enabled()

    def human_in_control(self) -> bool:
        """True iff human is currently driving (handback open or AI gated off)."""
        with self._lock:
            return (time.monotonic() < self._human_active_until) or (not self._ai_enabled)

    def state(self) -> tuple[float, float, bool, bool]:
        """Active source's pre-gate intent: (lin_x, ang_z, locked, braking).

        Returns whichever source the arbiter currently selects — so a UI
        overlay (speed badge, etc.) reflects whoever is actually driving.
        Lock and brake are always reported from the human side, since AI
        cannot unlock or un-brake.

        Respects the watchdog: if the selected source's last packet is
        older than watchdog_sec, returns 0 for lin/ang so the badge
        matches what _compute_output actually publishes.

        Do NOT pass this to the recorder. Use published_state().
        """
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
        """Last (lin_x, ang_z) actually sent to Docker — for the recorder.

        Reflects whichever source was selected by the arbiter, post
        ang_z_scale and post watchdog/lock/brake/lidar gates. Pass THIS
        to the recorder, not state().
        """
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
        """Pick a source (human vs AI), then apply universal gates.

        Returns (lin_x, ang_z) to publish this tick — ang_z already
        scaled by ang_z_scale. Lidar gate applies to both sources.
        """
        with self._lock:
            now = time.monotonic()
            h_lin, h_ang, h_locked, h_brake, h_t = self._latest_human
            a_lin, a_ang, _a_lk, _a_br,  a_t    = self._latest_ai

            # ── Safety latch: no human packet for too long while AI is enabled ──
            # Handles gamepad disconnect / network drop. Forces human to
            # explicitly re-chord to re-enable AI. h_t > 0 guards against
            # tripping at startup before any human packet has arrived.
            if (self._ai_enabled and h_t > 0.0
                    and (now - h_t) > self._human_stale_timeout):
                log("motion",
                    f"AI auto-disabled: no human packet for {now - h_t:.1f}s "
                    f"(gamepad disconnect?)")
                self._ai_enabled = False
                self._latest_ai  = (0.0, 0.0, False, False, 0.0)

            ai_enabled      = self._ai_enabled
            handback_active = now < self._human_active_until

            # Source selection. Human wins during handback OR when AI is gated off.
            human_in_control = handback_active or (not ai_enabled)

            if human_in_control:
                src_lin, src_ang, src_t = h_lin, h_ang, h_t
            else:
                src_lin, src_ang, src_t = a_lin, a_ang, a_t

            # Per-source watchdog — if the selected source hasn't published
            # within _watchdog seconds, zero. We do NOT fall back to the
            # other source: the selected source going stale is suspicious
            # and silence is the safe default.
            watchdog_ok = (now - src_t) < self._watchdog

            # Human is authoritative for lock + brake regardless of who drives.
            # AI cannot unlock and cannot un-brake.
            locked  = h_locked
            braking = h_brake

            lin_x = src_lin
            ang_z = src_ang * self._ang_z_scale

        if not watchdog_ok or locked or braking:
            return 0.0, 0.0

        # Lidar safety gate applies to both sources — safety trumps origin.
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