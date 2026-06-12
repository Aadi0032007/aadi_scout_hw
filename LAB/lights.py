# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations

"""
Lights: 4-channel USB HID relay (vendor 0x16c0, product 0x05df).

Channel map (Elephant robot):
    1 = headlights
    2 = strobe
    3 = halo left  + tail left
    4 = halo right + tail right

Three independent animation states that don't fight each other:
    - steady (headlights, strobe, halos as parking lights)
    - turn signal (left or right halo blink, headlights/strobe untouched)
    - all-blink (all 4 channels blink together — used for talk events
      and the all-lights-ON combo)

robot_lock=true forces everything off and locks out further commands.
HID auto-reconnects on USB transient errors.
"""

import threading
import time
from typing import Optional

from .common import log, truthy


# ── Hardware ──────────────────────────────────────────────────────────────────

VENDOR_ID  = 0x16c0
PRODUCT_ID = 0x05df

CH_HEADLIGHTS = 1
CH_STROBE     = 2
CH_HALO_LEFT  = 3
CH_HALO_RIGHT = 4

PARKING_RELAYS = (CH_HALO_LEFT, CH_HALO_RIGHT)


# ── Controller ────────────────────────────────────────────────────────────────

class LightsController:
    def __init__(
        self,
        blink_period_sec:        float,
        signal_timeout_sec:      float,
        talk_default_duration:   float,
        all_lights_cooldown_sec: float,
        all_lights_blink_sec:    float,
    ) -> None:
        self._blink_half       = max(0.05, blink_period_sec / 2.0)
        self._signal_timeout   = signal_timeout_sec
        self._talk_default     = talk_default_duration
        self._combo_cooldown   = all_lights_cooldown_sec
        self._combo_blink_sec  = all_lights_blink_sec

        # HID device
        self._dev = None
        self._dev_lock = threading.Lock()
        self._last_reconnect_log = 0.0

        # State (protected by _state_lock)
        self._state_lock     = threading.Lock()
        self._headlights_on  = False
        self._strobe_on      = False
        self._parking_on     = False
        self._left_until:  float = 0.0
        self._right_until: float = 0.0
        self._robot_locked   = False
        self._last_combo_at  = 0.0

        # All-blink (talk events, all-lights-on combo)
        self._all_blink_until: float = 0.0
        self._all_blink_then_on: bool = False   # after blink, leave them steady on?

        # Lifecycle
        self._stop = threading.Event()
        self._blink_thread = threading.Thread(target=self._blink_loop, daemon=True, name="lights-blink")

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        self._open_hid()
        self._apply_all_steady_locked = self._apply_steady   # alias for clarity
        self.all_off()
        self._blink_thread.start()
        log("lights", "ready")

    def stop(self) -> None:
        self._stop.set()
        try:
            self._blink_thread.join(timeout=2.0)
        except Exception:
            pass
        self.all_off()
        with self._dev_lock:
            if self._dev is not None:
                try:
                    self._dev.close()
                except Exception:
                    pass
                self._dev = None

    # ── public API (called by orchestrator) ───────────────────────────────────

    def set_robot_lock(self, locked: bool) -> None:
        """When True, force all lights off and ignore subsequent commands."""
        with self._state_lock:
            changed = locked != self._robot_locked
            self._robot_locked = locked
            if locked:
                self._headlights_on = False
                self._strobe_on = False
                self._parking_on = False
                self._left_until = 0.0
                self._right_until = 0.0
                self._all_blink_until = 0.0
                self._all_blink_then_on = False
        if changed:
            log("lights", f"robot_lock={'ON' if locked else 'OFF'}")
        if locked:
            self._apply_all_off()

    def command(self, pkt: dict) -> None:
        """Dispatch one parsed packet. Called for every relevant event."""
        with self._state_lock:
            if self._robot_locked:
                return

        event = (pkt.get("event") or "").strip().lower()

        if event == "lights":
            self._handle_lights_event(pkt)
        elif event == "signals":
            self._handle_signals_event(pkt)
        elif event == "talk":
            self._handle_talk_event(pkt)

    def all_off(self) -> None:
        with self._state_lock:
            self._headlights_on = False
            self._strobe_on = False
            self._parking_on = False
            self._left_until = 0.0
            self._right_until = 0.0
            self._all_blink_until = 0.0
            self._all_blink_then_on = False
        self._apply_all_off()

    # ── event handlers ────────────────────────────────────────────────────────

    def _handle_lights_event(self, pkt: dict) -> None:
        headlights = truthy(pkt.get("headlights")) if "headlights" in pkt else None
        parklights = truthy(pkt.get("parklights")) if "parklights" in pkt else None
        strobe     = truthy(pkt.get("strobe"))     if "strobe"     in pkt else None

        # All-lights-ON combo: blink 5s then leave everything on.
        if headlights is True and parklights is True and strobe is True:
            now = time.monotonic()
            with self._state_lock:
                if (now - self._last_combo_at) < self._combo_cooldown:
                    return   # absorb the 10× repeat from the gamepad sender
                self._last_combo_at = now
                self._all_blink_until    = now + self._combo_blink_sec
                self._all_blink_then_on  = True
            log("lights", f"ALL-ON combo: blink {self._combo_blink_sec}s then steady")
            return

        with self._state_lock:
            if headlights is not None:
                self._headlights_on = headlights
            if parklights is not None:
                self._parking_on = parklights
            if strobe is not None:
                self._strobe_on = strobe
        self._apply_steady()

    def _handle_signals_event(self, pkt: dict) -> None:
        left  = truthy(pkt.get("left",  False))
        right = truthy(pkt.get("right", False))
        now = time.monotonic()
        with self._state_lock:
            self._left_until  = (now + self._signal_timeout) if left  else 0.0
            self._right_until = (now + self._signal_timeout) if right else 0.0
        log("lights", f"signals: left={left} right={right}")

    def _handle_talk_event(self, pkt: dict) -> None:
        try:
            duration = float(pkt.get("duration", self._talk_default))
        except (TypeError, ValueError):
            duration = self._talk_default
        duration = max(0.5, min(30.0, duration))
        now = time.monotonic()
        with self._state_lock:
            self._all_blink_until   = now + duration
            self._all_blink_then_on = False   # talk blink fades to current steady state
        log("lights", f"talk blink {duration:.1f}s")

    # ── relay write with reconnect ────────────────────────────────────────────

    def _open_hid(self) -> None:
        try:
            import hid   # type: ignore
            with self._dev_lock:
                self._dev = hid.Device(vid=VENDOR_ID, pid=PRODUCT_ID)
            log("lights", f"HID opened vid=0x{VENDOR_ID:04x} pid=0x{PRODUCT_ID:04x}")
        except ImportError:
            log("lights", "hidapi not installed — pip install hid")
        except Exception as exc:
            log("lights", f"HID open failed: {exc}")
            self._dev = None

    def _write_relay(self, channel: int, on: bool) -> None:
        cmd = 0xFF if on else 0xFD
        payload = bytes([0x00, cmd, channel & 0xFF])

        with self._dev_lock:
            for attempt in (1, 2):
                if self._dev is None:
                    self._reopen_hid_locked(attempt)
                    if self._dev is None:
                        return
                try:
                    self._dev.write(payload)
                    return
                except Exception as exc:
                    msg = str(exc).lower()
                    transient = (
                        "no such device" in msg
                        or "device disconnected" in msg
                        or "i/o error" in msg
                        or "broken pipe" in msg
                    )
                    try:
                        self._dev.close()
                    except Exception:
                        pass
                    self._dev = None
                    if not transient or attempt == 2:
                        return

    def _reopen_hid_locked(self, attempt: int) -> None:
        now = time.time()
        if now - self._last_reconnect_log >= 1.0:
            log("lights", f"HID reconnect attempt {attempt}")
            self._last_reconnect_log = now
        try:
            import hid   # type: ignore
            self._dev = hid.Device(vid=VENDOR_ID, pid=PRODUCT_ID)
        except Exception:
            self._dev = None

    # ── relay state application ───────────────────────────────────────────────

    def _apply_steady(self) -> None:
        """Apply current headlights/strobe/parking flags. Only halos may be overridden
        by an active turn signal or all-blink — the blink loop handles that."""
        with self._state_lock:
            hl = self._headlights_on
            st = self._strobe_on
            pk = self._parking_on
            left_blink_active  = self._left_until  > time.monotonic()
            right_blink_active = self._right_until > time.monotonic()
            all_blink_active   = self._all_blink_until > time.monotonic()

        self._write_relay(CH_HEADLIGHTS, hl)
        self._write_relay(CH_STROBE,     st)

        # Halos: if a signal or all-blink is running, the blink loop owns them.
        # Otherwise apply parking state.
        if not (left_blink_active or right_blink_active or all_blink_active):
            self._write_relay(CH_HALO_LEFT,  pk)
            self._write_relay(CH_HALO_RIGHT, pk)

    def _apply_all_off(self) -> None:
        for ch in (CH_HEADLIGHTS, CH_STROBE, CH_HALO_LEFT, CH_HALO_RIGHT):
            self._write_relay(ch, False)

    def _apply_all_on(self) -> None:
        for ch in (CH_HEADLIGHTS, CH_STROBE, CH_HALO_LEFT, CH_HALO_RIGHT):
            self._write_relay(ch, True)

    # ── blink loop ────────────────────────────────────────────────────────────

    def _blink_loop(self) -> None:
        """One thread drives every blinking animation. Decides what to do each tick
        based on current state. Precedence: all-blink > turn signal > steady halos."""
        phase = False
        while not self._stop.is_set():
            now = time.monotonic()
            with self._state_lock:
                left_active  = now < self._left_until
                right_active = now < self._right_until
                all_active   = now < self._all_blink_until
                all_then_on  = self._all_blink_then_on
                hl = self._headlights_on
                st = self._strobe_on
                pk = self._parking_on

                # Edge: all-blink just ended
                if not all_active and self._all_blink_until != 0.0:
                    self._all_blink_until = 0.0
                    if all_then_on:
                        # Latch all four lights to ON in steady state
                        self._headlights_on = True
                        self._strobe_on     = True
                        self._parking_on    = True
                        hl, st, pk = True, True, True
                        self._all_blink_then_on = False
                    just_ended_all_blink = True
                else:
                    just_ended_all_blink = False

            phase = not phase

            if all_active:
                # All four channels blink together
                self._write_relay(CH_HEADLIGHTS, phase)
                self._write_relay(CH_STROBE,     phase)
                self._write_relay(CH_HALO_LEFT,  phase)
                self._write_relay(CH_HALO_RIGHT, phase)
            else:
                if just_ended_all_blink:
                    self._apply_steady()

                # Turn signals own the halos when active
                if left_active or right_active:
                    self._write_relay(CH_HEADLIGHTS, hl)
                    self._write_relay(CH_STROBE,     st)
                    self._write_relay(CH_HALO_LEFT,  left_active  and phase)
                    self._write_relay(CH_HALO_RIGHT, right_active and phase)
                else:
                    # No animation. Just keep steady state asserted (cheap and idempotent).
                    self._write_relay(CH_HEADLIGHTS, hl)
                    self._write_relay(CH_STROBE,     st)
                    self._write_relay(CH_HALO_LEFT,  pk)
                    self._write_relay(CH_HALO_RIGHT, pk)

            self._stop.wait(timeout=self._blink_half)