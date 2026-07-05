# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations

"""
lights.py — REDESIGN v2 (corrected channel mapping).

Relay wiring (dcttech HID relay, VID 0x16c0 PID 0x05df):

    channel 1  →  right taillight + right halo
    channel 2  →  left  taillight + left  halo
    channel 3  →  headlights (with strobe wiring — treated as one on/off)
    channel 4  →  christmas lights (NOT WIRED IN THIS BUILD)

Behavior:

    Lights ON button
        - if already ON  → blink all 3 channels for 5 seconds, then leave on
        - if OFF         → all 3 channels steady on
    Lights OFF button
        - all 3 channels off
    Left  turn signal → channel 2 blinks at duty cycle blink_period_sec
                        for signal_timeout_sec (default 5s)
    Right turn signal → channel 1 blinks at duty cycle blink_period_sec
                        for signal_timeout_sec (default 5s)
    Talk event → all 3 channels blink for the talk duration, then restore
                 pre-talk state (matches the old talk-blink UX)

Wire schema (TCP event channel, unchanged from previous version):

    {"type":"lights",    "data":{"headlights":bool,"parklights":bool,"strobe":bool}}
        - all three True   → interpreted as "lights ON"
        - all three False  → interpreted as "lights OFF"
        - anything else    → ignored (gamepad never sends other combos)
    {"type":"indicator", "data":{"side":"left"|"right"|"center"}}
    {"type":"talk",      "data":{"text":str,"duration":float}}

robot_lock=True forces everything off and locks out further commands.
HID auto-reconnects on transient USB errors.

Note on strobe: the wiring shares channel 3 with headlights, so strobe is
not independently controllable. The field is accepted but only participates
in the "all three True" ON detection.
"""

import os
import threading
import time
from typing import Optional

from .common import log, truthy


# ── Hardware ─────────────────────────────────────────────────────────────────

VENDOR_ID  = 0x16c0
PRODUCT_ID = 0x05df

CH_TAIL_HALO_RIGHT = 1
CH_TAIL_HALO_LEFT  = 2
CH_HEADLIGHTS      = 3
# channel 4 (christmas lights) is intentionally not wired — leave the relay
# alone. Reintroduce a constant and add to ALL_ON_CHANNELS when you decide
# how it should behave.

# All channels that participate in the "lights ON" / all-blink group
ALL_ON_CHANNELS = (CH_HEADLIGHTS, CH_TAIL_HALO_LEFT, CH_TAIL_HALO_RIGHT)


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

        # HID device — self._dev is an int fd (os.open) or None.
        # No hidapi dependency; same raw-write approach as test_relay.py.
        self._dev:      Optional[int] = None
        self._dev_path: Optional[str] = None
        self._dev_lock = threading.Lock()
        self._last_reconnect_log = 0.0

        # State (protected by _state_lock)
        self._state_lock     = threading.Lock()
        self._lights_on      = False   # single flag: headlights + tails + halos
        self._left_until:  float = 0.0
        self._right_until: float = 0.0
        self._robot_locked   = False
        self._last_combo_at  = 0.0

        # All-blink (both talk and "on-when-already-on" combo use this)
        self._all_blink_until: float = 0.0
        self._all_blink_then_on: bool = False   # after blink, latch lights_on?

        # Lifecycle
        self._stop = threading.Event()
        self._blink_thread = threading.Thread(target=self._blink_loop, daemon=True, name="lights-blink")

    # ── lifecycle ───────────────────────────────────────────────────────────

    def start(self) -> None:
        self._open_hid()
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
                    os.close(self._dev)
                except OSError:
                    pass
                self._dev = None

    # ── public dispatch (new envelope) ──────────────────────────────────────

    def set_robot_lock(self, locked: bool) -> None:
        with self._state_lock:
            changed = locked != self._robot_locked
            self._robot_locked = locked
            if locked:
                self._lights_on = False
                self._left_until = 0.0
                self._right_until = 0.0
                self._all_blink_until = 0.0
                self._all_blink_then_on = False
        if changed:
            log("lights", f"robot_lock={'ON' if locked else 'OFF'}")
        if locked:
            self._apply_all_off()

    def command(self, envelope: dict) -> None:
        """Dispatch one parsed envelope {seq, t, type, data}."""
        with self._state_lock:
            if self._robot_locked:
                return

        type_ = (envelope.get("type") or "").strip().lower()
        data  = envelope.get("data") or {}

        if type_ == "lights":
            self._handle_lights(data)
        elif type_ == "indicator":
            self._handle_indicator(data)
        elif type_ == "talk":
            self._handle_talk(data)

    def all_off(self) -> None:
        with self._state_lock:
            self._lights_on = False
            self._left_until = 0.0
            self._right_until = 0.0
            self._all_blink_until = 0.0
            self._all_blink_then_on = False
        self._apply_all_off()

    # ── event handlers ──────────────────────────────────────────────────────

    def _handle_lights(self, data: dict) -> None:
        """The gamepad sends all three booleans together:
            (True, True, True)    → lights ON
            (False, False, False) → lights OFF
        Anything else is ignored.
        """
        hl = truthy(data.get("headlights"))
        pk = truthy(data.get("parklights"))
        st = truthy(data.get("strobe"))

        all_on  = hl and pk and st
        all_off = (not hl) and (not pk) and (not st)

        if all_on:
            now = time.monotonic()
            with self._state_lock:
                if (now - self._last_combo_at) < self._combo_cooldown:
                    return   # absorb 10× repeat from gamepad
                self._last_combo_at = now
                already_on = self._lights_on
                if already_on:
                    # ON pressed while already on → blink combo, then stay on
                    self._all_blink_until   = now + self._combo_blink_sec
                    self._all_blink_then_on = True
                else:
                    # OFF → ON: steady on immediately, no blink
                    self._lights_on = True
                    self._all_blink_until   = 0.0
                    self._all_blink_then_on = False
            if already_on:
                log("lights", f"ON-while-ON combo: blink {self._combo_blink_sec}s then steady")
            else:
                log("lights", "lights ON")
                self._apply_steady()
            return

        if all_off:
            with self._state_lock:
                self._lights_on = False
                self._all_blink_until = 0.0
                self._all_blink_then_on = False
            log("lights", "lights OFF")
            self._apply_all_off()
            return

        # Any other combination is not defined by the wire spec; drop it.
        log("lights", f"lights envelope ignored (partial): hl={hl} pk={pk} st={st}")

    def _handle_indicator(self, data: dict) -> None:
        """New envelope has a single `side` field: left | right | center."""
        side = (data.get("side") or "center").strip().lower()
        now = time.monotonic()
        with self._state_lock:
            if side == "left":
                self._left_until  = now + self._signal_timeout
                self._right_until = 0.0
            elif side == "right":
                self._left_until  = 0.0
                self._right_until = now + self._signal_timeout
            else:
                self._left_until  = 0.0
                self._right_until = 0.0
        log("lights", f"indicator: side={side}")

    def _handle_talk(self, data: dict) -> None:
        try:
            duration = float(data.get("duration", self._talk_default))
        except (TypeError, ValueError):
            duration = self._talk_default
        duration = max(0.5, min(30.0, duration))
        now = time.monotonic()
        with self._state_lock:
            self._all_blink_until   = now + duration
            self._all_blink_then_on = False   # restore pre-talk state after
        log("lights", f"talk blink {duration:.1f}s")

    # ── relay write with reconnect (unchanged) ──────────────────────────────

    def _open_hid(self) -> None:
        """Find the dcttech relay's /dev/hidraw* node by scanning sysfs.

        No hidapi/pyusb dependency — this uses the same raw file I/O the
        test_relay.py bench tool uses (os.open + os.write). Report format
        is 3 bytes: [report_id, command, channel].
        """
        path = self._find_hidraw_node()
        if path is None:
            log("lights",
                f"no /dev/hidraw* device matched vid=0x{VENDOR_ID:04x} "
                f"pid=0x{PRODUCT_ID:04x} — is the relay plugged in?")
            with self._dev_lock:
                self._dev = None
            return
        try:
            fd = os.open(path, os.O_RDWR)
        except PermissionError as exc:
            log("lights",
                f"HID open PermissionError on {path}: {exc} "
                f"(need udev rule + plugdev group)")
            with self._dev_lock:
                self._dev = None
            return
        except OSError as exc:
            log("lights", f"HID open failed on {path}: {exc}")
            with self._dev_lock:
                self._dev = None
            return

        with self._dev_lock:
            self._dev = fd
            self._dev_path = path
        log("lights", f"HID opened {path} (vid=0x{VENDOR_ID:04x} pid=0x{PRODUCT_ID:04x})")

    @staticmethod
    def _find_hidraw_node() -> Optional[str]:
        """Scan /sys/class/hidraw/*/device/uevent for the matching VID:PID.

        `HID_ID=0003:000016C0:000005DF` uniquely identifies the dcttech relay.
        Preferred over a hardcoded /dev/hidraw2 — the number changes with
        USB enumeration order.
        """
        vid_hex = f"{VENDOR_ID:04X}"
        pid_hex = f"{PRODUCT_ID:04X}"
        try:
            names = sorted(os.listdir("/sys/class/hidraw"))
        except OSError:
            return None
        for name in names:
            uevent_path = f"/sys/class/hidraw/{name}/device/uevent"
            try:
                with open(uevent_path, "r") as f:
                    body = f.read()
            except OSError:
                continue
            if vid_hex in body.upper() and pid_hex in body.upper():
                return f"/dev/{name}"
        return None

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
                    os.write(self._dev, payload)
                    return
                except OSError as exc:
                    msg = str(exc).lower()
                    transient = (
                        "no such device" in msg or "device disconnected" in msg
                        or "i/o error" in msg or "broken pipe" in msg
                        or "no such file" in msg
                    )
                    try:
                        os.close(self._dev)
                    except OSError:
                        pass
                    self._dev = None
                    if not transient or attempt == 2:
                        return

    def _reopen_hid_locked(self, attempt: int) -> None:
        """Called while self._dev_lock is held. Try to reopen the hidraw node."""
        now = time.time()
        if now - self._last_reconnect_log >= 1.0:
            log("lights", f"HID reconnect attempt {attempt}")
            self._last_reconnect_log = now
        path = self._find_hidraw_node()
        if path is None:
            self._dev = None
            return
        try:
            self._dev = os.open(path, os.O_RDWR)
            self._dev_path = path
        except OSError:
            self._dev = None

    # ── relay state application ─────────────────────────────────────────────

    def _apply_steady(self) -> None:
        """Assert the current steady state on channels 1–3.

        Halos/tails are shared between "lights on" (steady) and turn signal
        (blinking). While a signal is active, the blink loop owns that side's
        channel; this method leaves it alone. Same for all-blink.
        """
        with self._state_lock:
            on = self._lights_on
            left_active  = self._left_until  > time.monotonic()
            right_active = self._right_until > time.monotonic()
            all_active   = self._all_blink_until > time.monotonic()

        self._write_relay(CH_HEADLIGHTS, on)

        # Left tail/halo: signal wins over steady
        if not (left_active or all_active):
            self._write_relay(CH_TAIL_HALO_LEFT, on)
        # Right tail/halo: signal wins over steady
        if not (right_active or all_active):
            self._write_relay(CH_TAIL_HALO_RIGHT, on)

    def _apply_all_off(self) -> None:
        for ch in ALL_ON_CHANNELS:
            self._write_relay(ch, False)

    # ── blink loop ──────────────────────────────────────────────────────────

    def _blink_loop(self) -> None:
        """One thread drives every blinking animation.

        Precedence per channel:
            all-blink (talk / on-when-on combo) > turn signal > steady state

        Duty cycle is symmetric: on for blink_half, off for blink_half.
        """
        phase = False
        while not self._stop.is_set():
            now = time.monotonic()

            with self._state_lock:
                left_active  = now < self._left_until
                right_active = now < self._right_until
                all_active   = now < self._all_blink_until
                all_then_on  = self._all_blink_then_on
                on           = self._lights_on

                # Edge: all-blink just ended
                if not all_active and self._all_blink_until != 0.0:
                    self._all_blink_until = 0.0
                    if all_then_on:
                        self._lights_on = True
                        on = True
                        self._all_blink_then_on = False
                    just_ended_all_blink = True
                else:
                    just_ended_all_blink = False

            phase = not phase

            if all_active:
                # All 3 channels blink together
                for ch in ALL_ON_CHANNELS:
                    self._write_relay(ch, phase)
            else:
                if just_ended_all_blink:
                    self._apply_steady()

                # Headlights always follow the steady flag
                self._write_relay(CH_HEADLIGHTS, on)

                # Left channel: signal blink wins, else follow steady
                if left_active:
                    self._write_relay(CH_TAIL_HALO_LEFT, phase)
                else:
                    self._write_relay(CH_TAIL_HALO_LEFT, on)

                # Right channel: signal blink wins, else follow steady
                if right_active:
                    self._write_relay(CH_TAIL_HALO_RIGHT, phase)
                else:
                    self._write_relay(CH_TAIL_HALO_RIGHT, on)

            self._stop.wait(timeout=self._blink_half)