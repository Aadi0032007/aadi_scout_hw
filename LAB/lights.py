# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations

"""
lights.py — REDESIGN v4 (xmas + xwalk-until-off + auto-on-unlock).

Relay wiring (dcttech HID relay, VID 0x16c0 PID 0x05df):

    channel 1  →  right taillight + right halo
    channel 2  →  left  taillight + left  halo
    channel 3  →  headlights (with strobe wiring — one on/off)
    channel 4  →  xmas lights

Behavior:

    UNLOCK EDGE (robot goes unlocked)
        - channels 1, 2, 3 latch STEADY ON automatically
        - channel 4 (xmas) stays off
        - Same as pressing lights-ON, but automatic.

    LOCK EDGE (robot goes locked)
        - all channels off
        - all pending signals/blinks cancelled

    Turn signals (from indicator envelope)
        - Left  → channel 2 blinks for signal_timeout_sec, self-expires
        - Right → channel 1 blinks for signal_timeout_sec, self-expires
        - Flick same side again while active → cancel
        - Suppressed while xwalk-blink is running

    Xwalk (from xwalk envelope) — CHANGED
        - on=True  → all 4 channels (1, 2, 3, 4) blink together and STAY
                     blinking until explicitly turned off
        - on=False → cancel immediately, restore prior steady state

    Talk (from talk envelope)
        - Channels 1, 2, 3 blink for the talk duration
        - Then restore prior state (channels 1,2,3 back to steady-on-if-unlocked)
        - Xmas (channel 4) NOT part of talk-blink — talk is a smaller subset

    Lights ON/OFF envelope (from browser high_visibility)
        - Only meaningful while LOCKED — since unlock already turns 1,2,3 on
        - While unlocked: lights follow lock state, high_visibility ignored
          (prevents accidentally killing safety lights while driving)

Wire schema (TCP/WS event envelope):

    {"type":"lights",    "data":{"headlights":bool,"parklights":bool,"strobe":bool}}
    {"type":"indicator", "data":{"side":"left"|"right"|"center"}}
    {"type":"talk",      "data":{"text":str,"duration":float}}
    {"type":"xwalk",     "data":{"on":bool}}

Precedence per channel (highest wins):
    1. robot_locked            → all off
    2. xwalk-blink             → all 4 blink (includes xmas), persists until off
    3. Turn signal (ch 1 or 2) → that side blinks
    4. Talk-blink              → channels 1,2,3 blink
    5. Steady lights_on flag   → channels 1,2,3 on/off (auto from lock state)

Note on strobe: shares channel 3 wiring with headlights. Not independently
controllable. Field is accepted but only participates in the all-three-True
detection for high_visibility.

Note on xmas: channel 4 is ONLY used for xwalk. Not part of talk-blink,
not part of unlock-auto-on, not part of high_visibility toggle.
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
CH_XMAS            = 4      # used by xwalk only

# Channels that participate in "lights on" (unlock-auto-on) and talk-blink.
# Xmas is intentionally excluded — it's xwalk-exclusive.
LIGHTS_ON_CHANNELS = (CH_HEADLIGHTS, CH_TAIL_HALO_LEFT, CH_TAIL_HALO_RIGHT)

# Channels that blink together on xwalk — ALL four including xmas.
XWALK_CHANNELS = (CH_HEADLIGHTS, CH_TAIL_HALO_LEFT, CH_TAIL_HALO_RIGHT, CH_XMAS)

# All channels ever written to (used only for all_off shutdown).
ALL_CHANNELS = (CH_HEADLIGHTS, CH_TAIL_HALO_LEFT, CH_TAIL_HALO_RIGHT, CH_XMAS)


class LightsController:
    def __init__(
        self,
        blink_period_sec:        float,
        signal_timeout_sec:      float,
        talk_default_duration:   float,
        all_lights_cooldown_sec: float,
        all_lights_blink_sec:    float,
        xwalk_duration_sec:      float = 10.0,   # kept for API compat, unused
    ) -> None:
        self._blink_half       = max(0.05, blink_period_sec / 2.0)
        self._signal_timeout   = signal_timeout_sec
        self._talk_default     = talk_default_duration
        self._combo_cooldown   = all_lights_cooldown_sec
        self._combo_blink_sec  = all_lights_blink_sec
        # xwalk_duration_sec kept as a constructor arg for backwards compat
        # with teleop wiring, but xwalk is no longer time-bounded.

        # HID device — self._dev is an int fd (os.open) or None.
        self._dev:      Optional[int] = None
        self._dev_path: Optional[str] = None
        self._dev_lock = threading.Lock()
        self._last_reconnect_log = 0.0

        # State (protected by _state_lock)
        self._state_lock     = threading.Lock()
        self._lights_on      = False   # channels 1,2,3 steady on (driven by unlock)
        self._left_until:  float = 0.0
        self._right_until: float = 0.0
        self._robot_locked   = True    # start locked
        self._last_combo_at  = 0.0

        # Talk-blink (3-channel: 1, 2, 3). Time-bounded. Restores prior state
        # when done.
        self._talk_blink_until:   float = 0.0

        # Xwalk-blink (4-channel: 1, 2, 3, 4). CHANGED: latched boolean, not
        # a deadline. Stays True until an explicit {"on": False} arrives (or
        # until the robot locks, which clears everything).
        self._xwalk_active: bool = False

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

    # ── public dispatch ─────────────────────────────────────────────────────

    def set_robot_lock(self, locked: bool) -> None:
        """
        UNLOCK → channels 1,2,3 latch steady on automatically.
        LOCK   → all channels off, all pending signals/blinks cancelled.
        """
        with self._state_lock:
            changed = locked != self._robot_locked
            self._robot_locked = locked
            if locked:
                # Full reset: everything off, no pending blinks or signals.
                self._lights_on = False
                self._left_until = 0.0
                self._right_until = 0.0
                self._talk_blink_until = 0.0
                self._xwalk_active = False
            else:
                # Unlock → auto-on channels 1,2,3 (steady). Xmas stays off.
                self._lights_on = True
                # Clear anything stale so the next signal / xwalk starts clean.
                self._left_until = 0.0
                self._right_until = 0.0
                self._talk_blink_until = 0.0
                self._xwalk_active = False
        if changed:
            log("lights",
                f"robot_lock={'ON' if locked else 'OFF'}"
                + (" — auto-on ch1,2,3" if not locked else ""))
        if locked:
            self._apply_all_off()
        else:
            self._apply_steady()

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
        elif type_ == "xwalk":
            self._handle_xwalk(data)

    def all_off(self) -> None:
        with self._state_lock:
            self._lights_on = False
            self._left_until = 0.0
            self._right_until = 0.0
            self._talk_blink_until = 0.0
            self._xwalk_active = False
        self._apply_all_off()

    # ── event handlers ──────────────────────────────────────────────────────

    def _handle_lights(self, data: dict) -> None:
        """High_visibility envelope from browser (or gamepad lights buttons).

        Since unlock now auto-turns-on channels 1,2,3, this envelope is
        effectively cosmetic while unlocked — steady lights are already on.
        We deliberately ignore it here to prevent the operator from
        accidentally killing safety lights while driving.

        Envelope is only dispatched when unlocked (command() blocks it while
        locked), so both "all True" and "all False" become no-ops.
        """
        hl = truthy(data.get("headlights"))
        pk = truthy(data.get("parklights"))
        st = truthy(data.get("strobe"))
        log("lights",
            f"lights envelope ignored (unlock owns lights): "
            f"hl={hl} pk={pk} st={st}")

    def _handle_indicator(self, data: dict) -> None:
        """Single `side` field: left | right | center."""
        side = (data.get("side") or "center").strip().lower()
        now = time.monotonic()
        with self._state_lock:
            if side == "left":
                if self._left_until > now:
                    self._left_until = 0.0
                    log("lights", "indicator: left cancelled")
                else:
                    self._left_until = now + self._signal_timeout
                    log("lights", f"indicator: left ON ({self._signal_timeout:.0f}s)")
                self._right_until = 0.0
            elif side == "right":
                if self._right_until > now:
                    self._right_until = 0.0
                    log("lights", "indicator: right cancelled")
                else:
                    self._right_until = now + self._signal_timeout
                    log("lights", f"indicator: right ON ({self._signal_timeout:.0f}s)")
                self._left_until = 0.0
            # else: side == "center" → no-op, let active signal self-expire

    def _handle_xwalk(self, data: dict) -> None:
        """Xwalk envelope from browser.

        {"data":{"on":True}}  → all 4 channels blink together, and STAY
                                blinking until an explicit off arrives.
        {"data":{"on":False}} → cancel immediately, restore steady state.
        """
        on = truthy(data.get("on"))
        with self._state_lock:
            if on:
                if not self._xwalk_active:
                    self._xwalk_active = True
                    log("lights", "xwalk: 4-channel blink ON (persists until off)")
                # else: already blinking — nothing to do
            else:
                if self._xwalk_active:
                    self._xwalk_active = False
                    log("lights", "xwalk: cancelled")
                # else: already off — nothing to do

    def _handle_talk(self, data: dict) -> None:
        try:
            duration = float(data.get("duration", self._talk_default))
        except (TypeError, ValueError):
            duration = self._talk_default
        duration = max(0.5, min(30.0, duration))
        now = time.monotonic()
        with self._state_lock:
            # 3-channel blink (headlights + tails/halos). Xmas not included.
            self._talk_blink_until = now + duration
        log("lights", f"talk blink {duration:.1f}s (ch 1,2,3)")

    # ── relay write with reconnect ──────────────────────────────────────────

    def _open_hid(self) -> None:
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
        """Assert the current steady state on channels 1–3."""
        with self._state_lock:
            on             = self._lights_on
            now = time.monotonic()
            left_active    = self._left_until       > now
            right_active   = self._right_until      > now
            talk_active    = self._talk_blink_until > now
            xwalk_active   = self._xwalk_active

        # Headlights (ch3): steady unless a blink owns it.
        if not (talk_active or xwalk_active):
            self._write_relay(CH_HEADLIGHTS, on)

        # Left tail/halo (ch2): signal > xwalk/talk blink > steady
        if not (left_active or talk_active or xwalk_active):
            self._write_relay(CH_TAIL_HALO_LEFT, on)

        # Right tail/halo (ch1): signal > xwalk/talk blink > steady
        if not (right_active or talk_active or xwalk_active):
            self._write_relay(CH_TAIL_HALO_RIGHT, on)

    def _apply_all_off(self) -> None:
        """Drive every relay channel low, including xmas."""
        for ch in ALL_CHANNELS:
            self._write_relay(ch, False)

    # ── blink loop ──────────────────────────────────────────────────────────

    def _blink_loop(self) -> None:
        """One thread drives every blinking animation.

        Precedence per channel (highest wins):
            1. robot_locked            → nothing here runs (all_off elsewhere)
            2. xwalk_active (latched)  → all 4 channels blink together
            3. Turn signal (ch1 or 2) → that side blinks
            4. Talk-blink              → channels 1, 2, 3 blink (xmas untouched)
            5. Steady lights_on flag   → channels 1, 2, 3 on/off

        Xmas (ch4) is xwalk-only: it's ON iff xwalk is active. When xwalk
        ends we explicitly drive xmas off. All other paths leave it alone.

        Duty cycle is symmetric: on for blink_half, off for blink_half.
        """
        phase = False
        prev_xwalk_active = False
        prev_talk_active  = False

        while not self._stop.is_set():
            now = time.monotonic()

            with self._state_lock:
                left_active   = now < self._left_until
                right_active  = now < self._right_until
                talk_active   = now < self._talk_blink_until
                xwalk_active  = self._xwalk_active
                on            = self._lights_on

                # Talk edge: talk was time-bounded; clear the deadline once
                # it elapses so future logic doesn't keep checking.
                just_ended_talk = False
                if not talk_active and self._talk_blink_until != 0.0:
                    self._talk_blink_until = 0.0
                    just_ended_talk = True

            # Xwalk edge (latched → False transition):
            just_ended_xwalk = prev_xwalk_active and not xwalk_active
            prev_xwalk_active = xwalk_active
            prev_talk_active  = talk_active

            phase = not phase

            # ── XWALK: all 4 channels blink together (highest precedence) ──
            if xwalk_active:
                for ch in XWALK_CHANNELS:
                    self._write_relay(ch, phase)
                # Skip everything else this tick.
                self._stop.wait(timeout=self._blink_half)
                continue

            # xwalk just ended → make sure xmas is off; then fall through to
            # restore steady state on channels 1-3.
            if just_ended_xwalk:
                self._write_relay(CH_XMAS, False)
                self._apply_steady()

            # talk just ended → restore steady on channels 1-3.
            if just_ended_talk:
                self._apply_steady()

            # ── TALK: channels 1, 2, 3 blink; xmas untouched ────────────────
            if talk_active:
                for ch in LIGHTS_ON_CHANNELS:
                    self._write_relay(ch, phase)
                self._stop.wait(timeout=self._blink_half)
                continue

            # ── Steady + turn signals ───────────────────────────────────────
            # Headlights always follow the steady flag.
            self._write_relay(CH_HEADLIGHTS, on)

            # Left channel: signal blink wins, else follow steady.
            if left_active:
                self._write_relay(CH_TAIL_HALO_LEFT, phase)
            else:
                self._write_relay(CH_TAIL_HALO_LEFT, on)

            # Right channel: signal blink wins, else follow steady.
            if right_active:
                self._write_relay(CH_TAIL_HALO_RIGHT, phase)
            else:
                self._write_relay(CH_TAIL_HALO_RIGHT, on)

            self._stop.wait(timeout=self._blink_half)