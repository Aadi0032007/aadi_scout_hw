# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations


"""
PTZ camera control via ONVIF — direction commands + home capture/return.

UDP "head" field drives continuous pan/tilt. The camera auto-stops if no
new command arrives within ptz_stop_after_sec. Position is dead-reckoned
by integrating commanded velocity × dt — accurate enough for "look around,
then return roughly to where I started," not for precision pointing.

Triggers that capture the current position as "home":
    - First-ever unlock (call set_unlock_state(True) when robot unlocks)
    - capture_home() called externally (e.g. on A+B combo or speed-cycle)

Return-to-home:
    - goto_home() drives back toward stored origin until inside deadband
    - intended for the gamepad's lights-ON button (button=8)

PTZ motion is intentionally independent of drivetrain robot_lock so the
operator can still look around while the robot is locked.
"""

import threading
import time
from typing import Optional

from .common import log


class PtzController:
    def __init__(
        self,
        ip:               str,
        port:             int,
        user:             str,
        password:         str,
        pan_speed:        float,
        tilt_speed:       float,
        loop_hz:          float,
        deadband_sec:     float,
        stop_after_sec:   float,
        return_deadband:  float = 0.02,
    ) -> None:
        self._ip            = ip
        self._port          = port
        self._user          = user
        self._password      = password
        self._pan_speed     = pan_speed
        self._tilt_speed    = tilt_speed
        self._loop_period   = 1.0 / max(1.0, loop_hz)
        self._deadband_sec  = deadband_sec
        self._stop_after    = stop_after_sec
        self._return_dead   = return_deadband

        # ONVIF handles
        self._ptz     = None
        self._token   = None

        # State (protected by _lock)
        self._lock    = threading.Lock()
        self._desired = "center"    # last commanded direction
        self._last_cmd_t = 0.0
        self._last_applied: Optional[str] = None
        self._last_send_t = 0.0

        # Dead-reckoning position
        self._pan_pos     = 0.0
        self._tilt_pos    = 0.0
        self._origin_pan  = 0.0
        self._origin_tilt = 0.0
        self._origin_captured = False
        self._returning_pan   = False
        self._returning_tilt  = False
        self._last_tick = time.monotonic()

        # PTZ has its own independent lock state. Drivetrain lock does NOT affect it.
        self._ptz_unlocked = True

        # Lifecycle
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="ptz")

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        try:
            from onvif import ONVIFCamera   # type: ignore
            cam = ONVIFCamera(self._ip, self._port, self._user, self._password)
            self._ptz = cam.create_ptz_service()
            media = cam.create_media_service()
            profiles = media.GetProfiles()
            if not profiles:
                raise RuntimeError("no ONVIF profiles found")
            self._token = profiles[0].token
            self._send_stop()
            self._thread.start()
            log("ptz", f"connected {self._ip}:{self._port}")
        except Exception as exc:
            log("ptz", f"disabled — {exc}")
            self._ptz = None

    def stop(self) -> None:
        self._stop.set()
        try:
            self._thread.join(timeout=2.0)
        except Exception:
            pass
        if self._ptz is not None:
            try:
                self._send_stop()
            except Exception:
                pass

    # ── public API ────────────────────────────────────────────────────────────

    def command(self, head: str) -> None:
        """Set desired direction. One of: left, right, up, down, center."""
        head = (head or "center").strip().lower()
        if head not in ("left", "right", "up", "down", "center"):
            head = "center"

        with self._lock:
            if not self._ptz_unlocked:
                self._desired = "center"
            else:
                self._desired = head
            self._last_cmd_t = time.monotonic()

    def set_ptz_unlock_state(self, unlocked: bool) -> None:
        """Independent PTZ-only lock. Capture home on first unlock."""
        with self._lock:
            was_unlocked = self._ptz_unlocked
            self._ptz_unlocked = unlocked
            if unlocked and (not was_unlocked or not self._origin_captured):
                self._origin_pan      = self._pan_pos
                self._origin_tilt     = self._tilt_pos
                self._origin_captured = True
                self._returning_pan   = False
                self._returning_tilt  = False
                self._desired         = "center"
                log("ptz", "unlocked — home captured at current position")
            if not unlocked and was_unlocked:
                self._desired = "center"
                self._returning_pan  = False
                self._returning_tilt = False
                log("ptz", "locked — motion paused")

    def capture_home(self) -> None:
        """Mark current position as home. Called on A+B combo, speed-cycle, etc."""
        with self._lock:
            self._origin_pan      = self._pan_pos
            self._origin_tilt     = self._tilt_pos
            self._origin_captured = True
            self._returning_pan   = False
            self._returning_tilt  = False
            self._desired         = "center"
        log("ptz", "home captured")

    def goto_home(self) -> None:
        """Start driving back toward stored home position."""
        with self._lock:
            if not self._origin_captured:
                log("ptz", "goto_home ignored — no home stored")
                return
            self._returning_pan  = abs(self._pan_pos  - self._origin_pan)  > self._return_dead
            self._returning_tilt = abs(self._tilt_pos - self._origin_tilt) > self._return_dead
            self._desired = "center"
            if self._returning_pan or self._returning_tilt:
                log("ptz", "returning to home")
            else:
                log("ptz", "already at home")

    # ── internal control loop ─────────────────────────────────────────────────

    def _direction_to_velocity(self, direction: str) -> tuple[float, float]:
        if direction == "left":  return (-self._pan_speed, 0.0)
        if direction == "right": return ( self._pan_speed, 0.0)
        if direction == "up":    return (0.0,  self._tilt_speed)
        if direction == "down":  return (0.0, -self._tilt_speed)
        return (0.0, 0.0)

    def _loop(self) -> None:
        while not self._stop.is_set():
            now = time.monotonic()
            dt = max(0.0, now - self._last_tick)
            self._last_tick = now

            with self._lock:
                ptz_unlocked    = self._ptz_unlocked
                desired         = self._desired
                last_cmd_t      = self._last_cmd_t
                returning_pan   = self._returning_pan
                returning_tilt  = self._returning_tilt
                pan_delta       = self._pan_pos  - self._origin_pan
                tilt_delta      = self._tilt_pos - self._origin_tilt
                origin_captured = self._origin_captured

                # Stop returning if we're within deadband
                if returning_pan and abs(pan_delta) <= self._return_dead:
                    self._pan_pos       = self._origin_pan
                    self._returning_pan = False
                    returning_pan       = False
                    pan_delta           = 0.0
                if returning_tilt and abs(tilt_delta) <= self._return_dead:
                    self._tilt_pos       = self._origin_tilt
                    self._returning_tilt = False
                    returning_tilt       = False
                    tilt_delta           = 0.0

            # Decide the velocity command for this tick
            cmd_pan = 0.0
            cmd_tilt = 0.0
            mode = "center"

            if not ptz_unlocked:
                mode = "center"
            elif origin_captured and returning_pan:
                cmd_pan = -self._pan_speed if pan_delta > 0 else self._pan_speed
                mode = "return_pan"
            elif origin_captured and returning_tilt:
                cmd_tilt = -self._tilt_speed if tilt_delta > 0 else self._tilt_speed
                mode = "return_tilt"
            else:
                # Operator-driven motion stops if commands stop arriving
                if (now - last_cmd_t) > self._stop_after:
                    mode = "center"
                else:
                    cmd_pan, cmd_tilt = self._direction_to_velocity(desired)
                    mode = desired

            self._send_with_deadband(mode, cmd_pan, cmd_tilt, dt)
            self._stop.wait(timeout=self._loop_period)

    def _send_with_deadband(self, mode: str, pan: float, tilt: float, dt: float) -> None:
        """Apply ONVIF command, respecting per-mode deadband to avoid hammering the camera."""
        if self._ptz is None:
            return

        now = time.monotonic()
        try:
            if mode != self._last_applied:
                self._send_stop()
                self._last_applied = mode
                self._last_send_t = now

            if mode == "center":
                return

            # Don't re-issue ContinuousMove within deadband
            if (now - self._last_send_t) < self._deadband_sec:
                # Still integrate position estimate
                self._integrate_position(pan, tilt, dt)
                return

            self._send_continuous_move(pan, tilt)
            self._last_send_t = now
            self._integrate_position(pan, tilt, dt)
        except Exception as exc:
            log("ptz", f"send error: {exc}")

    def _integrate_position(self, pan: float, tilt: float, dt: float) -> None:
        if dt <= 0.0:
            return
        with self._lock:
            self._pan_pos  += pan  * dt
            self._tilt_pos += tilt * dt

    # ── ONVIF primitives ──────────────────────────────────────────────────────

    def _send_continuous_move(self, pan: float, tilt: float) -> None:
        req = self._ptz.create_type("ContinuousMove")
        req.ProfileToken = self._token
        req.Velocity = {
            "PanTilt": {"x": float(pan), "y": float(tilt)},
            "Zoom":    {"x": 0.0},
        }
        self._ptz.ContinuousMove(req)

    def _send_stop(self) -> None:
        if self._ptz is None or self._token is None:
            return
        req = self._ptz.create_type("Stop")
        req.ProfileToken = self._token
        req.PanTilt = True
        req.Zoom    = True
        self._ptz.Stop(req)