# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations

"""
motion.py — UDP forward to segway_ros1 Docker + human/AI source arbitration.

Changes vs previous version:
    - NEW: ang_z drift correction. A constant yaw bias is added to every
      ang_z command AFTER ang_z_scale, i.e. directly in /cmd_vel space, to
      cancel the drivetrain's steady drift:
          ang_out = src_ang * ang_z_scale + ang_z_drift_correction
        cfg.ang_z_drift_correction     initial value
      It is applied only on the live output path, so it never leaks into the
      recorded dataset: published_state_raw() returns the selected source's
      RAW ang_z (no scale, no correction) and that is what SessionRecorder
      stores. The correction is suppressed whenever the output is gated to
      zero (watchdog, lock, brake, lidar) — a braked robot must not yaw.
    - NEW: AI stream watchdog. ai_stale_timeout auto-disables AI when no
      origin="ai" packets are arriving, so enabling AI with lab_inference
      down hands control straight back to the human instead of leaving the
      robot braked on a dead AI slot. A grace window runs from the moment
      AI is enabled, so a just-started inference process is not cut off.
        motion.ai_stale_timeout()      -> float
        cfg.motion_ai_stale_sec        initial value
    - Runtime toggle for the lidar safety brake, driven by WS bubble_mode:
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
    ai_stream_age() -> float               # NEW
    ai_stale_timeout() -> float            # NEW
    human_in_control() -> bool
    state() -> (lin_x, ang_z, locked, braking)
    published_state() -> (lin_x, ang_z)            # scaled + drift-corrected
    published_state_raw() -> (lin_x, ang_z)        # NEW — raw, uncorrected
    set_lidar_block_enabled(on: bool)
    lidar_block_enabled() -> bool
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
        ang_z_drift_correction: float = 0.0,
        lidar_block_fn:      Optional[Callable[[float], bool]] = None,
        lidar_block_enabled: bool  = True,
        # ── human-priority arbiter knobs ─────────────────────────────────
        human_handback_sec:  float = 2.0,
        human_idle_deadband: float = 0.05,
        human_stale_timeout: float = 2.0,
        # ── AI stream watchdog ───────────────────────────────────────────
        ai_stale_timeout:    float = 1.0,
    ) -> None:
        self._docker_host    = docker_host
        self._docker_port    = docker_port
        self._publish_hz     = max(1, publish_hz)
        self._watchdog       = watchdog_sec
        self._ang_z_scale    = ang_z_scale
        self._ang_z_drift    = float(ang_z_drift_correction)
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
        # Monotonic time AI was last switched ON. Starts the grace window for
        # the AI stream watchdog; 0.0 while AI is off.
        self._ai_enabled_at = 0.0
        self._ai_stale_timeout = float(ai_stale_timeout)

        # Runtime lidar brake gate. Toggled at runtime by
        # motion.set_lidar_block_enabled() driven by WS bubble_mode.
        self._lidar_block_enabled = bool(lidar_block_enabled)

        # Handback window
        self._human_active_until  = 0.0
        self._human_handback_sec  = float(human_handback_sec)
        self._human_idle_db       = float(human_idle_deadband)
        self._human_stale_timeout = float(human_stale_timeout)

        # Last values actually sent to Docker — for telemetry.
        self._last_pub_lin: float = 0.0
        self._last_pub_ang: float = 0.0
        # Same publish tick, but the selected source's RAW ang_z: no
        # ang_z_scale, no drift correction. This is what the recorder stores,
        # so the dataset stays in the command space the policy is trained on.
        self._last_pub_ang_raw: float = 0.0

        # HUMAN/AI handover logging — None until the first decision is made.
        self._last_human_in_control: Optional[bool] = None

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
                f"ang_drift={self._ang_z_drift:+.3f}, "
                f"handback={self._human_handback_sec}s, idle_db={self._human_idle_db}, "
                f"human_stale={self._human_stale_timeout}s, "
                f"ai_stale={self._ai_stale_timeout}s, "
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
                self._ai_enabled_at = 0.0

    def set_ai_enabled(self, on: bool) -> None:
        with self._lock:
            # The brake is an emergency stop and it hard-latches AI off. A
            # latched brake re-asserts that on every incoming packet, so
            # enabling here would flip on and straight back off ~20 ms later
            # and look like the A button did nothing. Refuse, and say why.
            if on and self._latest_human[3]:
                log("motion",
                    "AI enable REFUSED — emergency brake is engaged. "
                    "Release the brake, then press A again.")
                return

            prev = self._ai_enabled
            self._ai_enabled = bool(on)
            if on:
                # A standing cruise value is still a human command, so the
                # handback window never closes and AI cannot take over. That
                # is intended, but silent — call it out at enable time.
                h_lin, h_ang = self._latest_human[0], self._latest_human[1]
                if (abs(h_lin) >= self._human_idle_db
                        or abs(h_ang) >= self._human_idle_db):
                    log("motion",
                        f"AI enabled, but the human is still commanding "
                        f"lin_x={h_lin:+.2f} ang_z={h_ang:+.2f} — AI takes over "
                        f"once that returns to neutral")
                # Start the AI-stream grace window. The watchdog in
                # _compute_output measures from whichever is later — this
                # moment, or the last origin="ai" packet — so a freshly
                # launched lab_inference gets time to be noticed while a
                # dead stream is still caught within ai_stale_timeout.
                self._ai_enabled_at = time.monotonic()
            else:
                self._ai_enabled_at = 0.0
                self._latest_ai = (0.0, 0.0, False, False, 0.0)
            if prev != self._ai_enabled:
                log("motion", f"ai_enabled -> {self._ai_enabled}")

    def ai_enabled(self) -> bool:
        with self._lock:
            return self._ai_enabled

    def is_ai_enabled(self) -> bool:
        return self.ai_enabled()

    def ai_stream_age(self) -> float:
        """Seconds since the last origin="ai" packet. inf if there never was one."""
        with self._lock:
            a_t = self._latest_ai[4]
        if a_t <= 0.0:
            return float("inf")
        return max(0.0, time.monotonic() - a_t)

    def ai_stale_timeout(self) -> float:
        return self._ai_stale_timeout

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
        """Last (lin_x, ang_z) actually sent to Docker — scaled + drift-corrected."""
        with self._lock:
            return self._last_pub_lin, self._last_pub_ang

    def published_state_raw(self) -> tuple[float, float]:
        """Same publish tick as published_state(), but ang_z in RAW command space.

        No ang_z_scale, no ang_z_drift_correction — this is the value the
        selected source asked for. Gating still applies: when the output was
        zeroed (watchdog, lock, brake, lidar) both fields read 0.0, because
        the robot did not move and the dataset must reflect that.

        Use this for recording; use published_state() for telemetry about what
        the drivetrain actually received.
        """
        with self._lock:
            return self._last_pub_lin, self._last_pub_ang_raw

    # ── publisher loop ──────────────────────────────────────────────────────

    def _publish_loop(self) -> None:
        interval = 1.0 / self._publish_hz
        while not self._stop.is_set():
            lin, ang, ang_raw = self._compute_output()
            self._send_twist(lin, ang)
            with self._lock:
                self._last_pub_lin     = lin
                self._last_pub_ang     = ang
                self._last_pub_ang_raw = ang_raw
            self._stop.wait(timeout=interval)

    def _compute_output(self) -> tuple[float, float, float]:
        """Return (lin_x, ang_z_out, ang_z_raw).

        ang_z_out is what goes on the wire: src_ang * ang_z_scale + drift.
        ang_z_raw is the same tick's src_ang untouched — for the recorder.
        Both are 0.0 together whenever the output is gated.
        """
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
                self._ai_enabled_at = 0.0
                self._latest_ai  = (0.0, 0.0, False, False, 0.0)

            # AI stream watchdog: lab_inference never started, died, or its
            # packets stopped. Without this the arbiter would select an AI
            # slot whose timestamp is stale, the per-source watchdog below
            # would zero it, and the robot would sit braked with no obvious
            # cause. Handing control back to the human is both safer and far
            # easier to diagnose. Measured from the later of "AI switched on"
            # and "last AI packet" so a just-launched inference is not cut off.
            ai_ref_t = max(a_t, self._ai_enabled_at)
            if (self._ai_enabled and self._ai_stale_timeout > 0.0
                    and ai_ref_t > 0.0
                    and (now - ai_ref_t) > self._ai_stale_timeout):
                log("motion",
                    f"AI auto-disabled: no origin=ai packet for "
                    f"{now - ai_ref_t:.1f}s (lab_inference not running?)")
                self._ai_enabled = False
                self._ai_enabled_at = 0.0
                self._latest_ai = (0.0, 0.0, False, False, 0.0)
                a_lin, a_ang, a_t = 0.0, 0.0, 0.0

            ai_enabled      = self._ai_enabled
            handback_active = now < self._human_active_until
            human_in_control = handback_active or (not ai_enabled)

            # Handover logging. Only interesting while AI is in play, and only
            # on a change — this runs at publish_hz.
            if human_in_control != self._last_human_in_control:
                if ai_enabled or self._last_human_in_control is False:
                    log("motion",
                        f"control → {'HUMAN' if human_in_control else 'AI'}"
                        + (" (handback window open)"
                           if human_in_control and ai_enabled else ""))
                self._last_human_in_control = human_in_control

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
            # Drift correction lands AFTER the scale, so it is a plain offset
            # in /cmd_vel space. It is added unconditionally — including at
            # src_ang == 0.0, which is the case it exists for: "commanded
            # straight, veers anyway".
            ang_raw = src_ang
            ang_z   = src_ang * self._ang_z_scale + self._ang_z_drift

            lidar_gate_on = self._lidar_block_enabled

        # Every gated path below returns a hard zero, drift correction
        # included — a braked, locked or watchdog-silenced robot must sit
        # still, not yaw at the bias rate.
        if not watchdog_ok or locked or braking:
            return 0.0, 0.0, 0.0

        # Lidar safety gate — runtime-togglable via set_lidar_block_enabled.
        if self._lidar_block_fn is not None and lidar_gate_on:
            try:
                if self._lidar_block_fn(lin_x):
                    return 0.0, 0.0, 0.0
            except Exception as exc:
                log("motion", f"lidar_block_fn error: {exc}")

        return lin_x, ang_z, ang_raw

    def _send_twist(self, lin: float, ang: float) -> None:
        if self._sock is None:
            return
        try:
            payload = json.dumps({"lin_x": lin, "ang_z": ang}).encode()
            self._sock.sendto(payload, (self._docker_host, self._docker_port))
        except Exception:
            pass