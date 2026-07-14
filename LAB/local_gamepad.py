# -*- coding: utf-8 -*-
"""
Created on Sun Jul  5 07:09:45 2026
Updated: ABXY feature parity + buffered A/B lock disambiguation + 8BitDo reconnect

@author: Aadi
"""
from __future__ import annotations

"""
local_gamepad.py — REDESIGN v3.

Local dongle handler. Same driving behavior as the pilot gamepad
(same profiles, same axis mappings, same state machines) but calls
teleop's in-process dispatchers instead of going over the wire.

Wire equivalence:
    - Motion packets: trimmed 12-field schema (same as pilot's UDP
      payload) plus "_local": True for the source arbiter.
    - Event packets: unified TCP envelope {seq, t, type, data}.
    - Both handed to teleop's dispatchers directly (on_motion, on_events).
      No sockets involved.

Behaviors:
    Driving — identical to pilot bridge:
        - Steering:  deadzone → expo → gain → yaw limit
        - Cruise ±:  buttons cycle CRUISE_LEVELS into lin_x
        - Both cruise buttons: brake, zero yaw, reset cruise
        - Turn signals: axis 3 threshold → indicator event
        - Sound axis 4: neg-tap speech, neg double-tap music, pos speech
        - Lift triggers: -255..-50 / 0 / +50..+255
        - AI-enable chord: both lift triggers > 0.95 → ai_request="enable"
        - Head direction: hat pad or right stick → head field
        - Lights ON/OFF buttons → all three lights fields together

    Local-only lock/speed (WS not in the loop):
        - A → B within 2s while locked   → UNLOCK
        - A → B within 2s while unlocked → cycle speed level 1→2→3→1
        - B → A within 2s                → LOCK
        - A and B are BUFFERED. If the paired button comes within 2s,
          the sequence fires and standalone toggles are SUPPRESSED.
          If it doesn't come, the toggle fires when the buffer expires.

    ABXY as browser-feature toggles:
        - A (buffered 2s) → toggle AI mode          (event: ai_mode)
        - B (buffered 2s) → toggle bubble (lidar)   (event: bubble)
        - X (immediate)   → toggle xwalk            (event: xwalk)
        - Y (immediate)   → toggle yield            (event: yield)
"""

import math
import os
import platform
import subprocess
import threading
import time
from typing import Callable, Optional

from .common import log


# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG (parity with pilot)
# ══════════════════════════════════════════════════════════════════════════════

SEND_HZ = 50
AXIS_ACTION_THRESHOLD_COUNTS = 30000

AXIS4_NEG_MESSAGE           = "Hellow how are you today?"
AXIS4_POS_MESSAGE           = "please let me go!"
TALK_DURATION_SEC           = 7.0
AXIS4_MULTI_TAP_WINDOW_SEC  = 1.0
MUSIC_TRACK_ID              = 1
MUSIC_TALK_DURATION_SEC     = 60.0
AUDIO_FULL_VOLUME_PCT       = 100

# Steering behavior
MAX_YAW_MOVING   = 2.0
MAX_YAW_INPLACE = 3.5
STEER_DEADZONE  = 0.1
STEER_EXPO      = 0.8
STEER_GAIN      = 1.0

# Pedal / cruise
BRAKE_THRESHOLD = 0.2
CRUISE_LEVELS = [-1.0, -0.6, -0.4, -0.2, -0.1, -0.05,
                 0.0, 0.05, 0.1, 0.2, 0.4, 0.6, 1.0]
PEDAL_DEADBAND = 0.05

# Lift
LIFT_MIN_CMD        = 50
LIFT_MAX_CMD        = 255
LIFT_AXIS_DEADBAND  = 0.02

# AI-enable chord (lift-trigger squeeze)
AI_ENABLE_PRESS_THRESHOLD = 0.95
AI_REQUEST_REPEAT_PACKETS = 5

# Lock sequence + A/B toggle buffer window (same value — they use one clock)
LOCK_SEQUENCE_TIMEOUT = 2.0
MAX_SPEED_INITIAL     = 1.0
SWAP_XY_BUTTONS       = False

# Joystick reconnect
JOYSTICK_RETRY_SEC = 1.0

# 8BitDo Ultimate 2 XInput wake/recovery.
# The USB dongle can remain enumerated as /dev/input/js0 even when the
# physical controller has powered off or gone idle. On Jetson, the dongle
# may need an Xbox LED/start OUT packet before it resumes interrupt reports.
WAKE_8BITDO_SCRIPT = "/home/revolabs/Revobots/Segway/CAN/wake_8bitdo_xinput.py"
REWAKE_MIN_INTERVAL_SEC = 5.0
JOYSTICK_SOFT_REINIT_SEC = 3.0


# ── Gamepad mappings (identical to pilot) ────────────────────────────────────

GAMEPAD_MAPPINGS = {
    "8bitdo_ultimate_wireless_pc": {
        "axis_steer":     0,
        "axis_sound":     3,
        "axis_signal":    4,
        "axis_head_lr":   6,
        "axis_head_ud":   7,
        "axis_lift_pos":  4,
        "axis_lift_neg":  5,
        "btn_a": 0, "btn_b": 1, "btn_x": 3, "btn_y": 4,
        "btn_cruise_down": 6, "btn_cruise_up": 7,
        "btn_lights_on":  11, "btn_lights_off": 10,
    },
    "8bitdo_ultimate2_wireless": {
        "axis_steer":     0,
        "axis_signal":    3,
        "axis_sound":     4,
        "axis_head_lr":   6,
        "axis_head_ud":   7,
        "axis_lift_pos":  5,
        "axis_lift_neg":  2,
        "btn_a": 0, "btn_b": 1, "btn_x": 2, "btn_y": 3,
        "btn_cruise_down": 4, "btn_cruise_up": 5,
        "btn_lights_on":  7, "btn_lights_off": 6,
    },
    "8bitdo_ultimate2_wireless_windows": {
        "axis_steer":     0,
        "axis_signal":    2,
        "axis_sound":     3,
        "axis_head_lr":   6,
        "axis_head_ud":   7,
        "axis_lift_pos":  5,
        "axis_lift_neg":  4,
        "btn_a": 0, "btn_b": 1, "btn_x": 2, "btn_y": 3,
        "btn_cruise_down": 4, "btn_cruise_up": 5,
        "btn_lights_on":  7, "btn_lights_off": 6,
    },
}

DEFAULT_MAPPING_KEY = "8bitdo_ultimate_wireless_pc"


# ══════════════════════════════════════════════════════════════════════════════
#  Utility functions
# ══════════════════════════════════════════════════════════════════════════════

def _clamp(x, lo, hi):
    return max(lo, min(hi, x))


def _apply_deadzone(x, dz):
    if abs(x) <= dz:
        return 0.0
    return math.copysign((abs(x) - dz) / (1.0 - dz), x)


def _expo_curve(x, expo):
    return (1.0 - expo) * x + expo * (x ** 3)


def _lift_axis_to_cmd(axis_value):
    press = _clamp((axis_value + 1.0) * 0.5, 0.0, 1.0)
    if press <= LIFT_AXIS_DEADBAND:
        return 0
    return int(round(LIFT_MIN_CMD + press * (LIFT_MAX_CMD - LIFT_MIN_CMD)))


# ══════════════════════════════════════════════════════════════════════════════
#  LocalGamepad
# ══════════════════════════════════════════════════════════════════════════════

class LocalGamepad:
    """Reads a locally-attached gamepad and drives teleop dispatchers directly."""

    def __init__(
        self,
        on_motion:           Callable[[dict, tuple, int], None],
        on_events:           Callable[[dict, tuple, int], None],
        initial_robot_lock:  bool = True,
        priority_value:      int  = 100,
    ) -> None:
        self._on_motion = on_motion
        self._on_events = on_events
        self._robot_lock = bool(initial_robot_lock)
        self._priority   = int(priority_value)

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._local_event_seq = 0

        # Last time we ran wake_8bitdo_xinput.py. Used to avoid hammering
        # USB/xpad while the controller is off or reconnecting.
        self._last_8bitdo_wake_t = 0.0

    # ── lifecycle ───────────────────────────────────────────────────────────

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="local-gamepad"
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    # ── envelope emission ───────────────────────────────────────────────────

    def _emit_envelope(self, type_: str, data: dict) -> None:
        """Emit one event envelope in the same shape TCP produces."""
        self._local_event_seq += 1
        pkt = {
            "seq":    self._local_event_seq,
            "t":      time.time(),
            "type":   type_,
            "data":   data,
        }
        try:
            self._on_events(pkt, ("local", 0), -1)
        except Exception as exc:
            log("local_gp", f"events dispatch error: {exc}")

    # ── pygame init helpers ─────────────────────────────────────────────────

    def _init_pygame(self):
        """Lazy pygame import + init. Kept inside the worker thread so a
        missing pygame doesn't crash teleop startup, and to avoid touching
        SDL from the main thread.
        """
        try:
            import pygame
        except ImportError:
            log("local_gp", "pygame not installed — local gamepad disabled")
            return None
        try:
            if not os.environ.get("DISPLAY") and not os.environ.get("WAYLAND_DISPLAY"):
                os.environ.setdefault("SDL_VIDEODRIVER", "dummy")
                log("local_gp", "no display — using SDL_VIDEODRIVER=dummy for joystick")
            os.environ.setdefault("SDL_AUDIODRIVER", "dummy")
            pygame.init()
            pygame.joystick.init()
        except Exception as exc:
            log("local_gp", f"pygame init failed: {exc}")
            return None
        return pygame


    def _wake_8bitdo(self, reason: str = "") -> None:
        """Kick the 8BitDo XInput dongle so it starts sending reports again.

        The dongle can stay visible to pygame while the physical controller
        is asleep/off. Running the wake helper sends the OUT packet this
        Jetson/xpad setup needs, then reattaches the kernel xpad driver.
        """
        now = time.time()
        if now - self._last_8bitdo_wake_t < REWAKE_MIN_INTERVAL_SEC:
            return
        if not os.path.exists(WAKE_8BITDO_SCRIPT):
            return

        self._last_8bitdo_wake_t = now
        suffix = f" ({reason})" if reason else ""
        log("local_gp", f"waking 8BitDo XInput stream{suffix}")
        try:
            subprocess.run(
                ["/usr/bin/python3", WAKE_8BITDO_SCRIPT],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=8,
                check=False,
            )
        except Exception as exc:
            log("local_gp", f"8BitDo wake failed: {exc}")

    def _wait_for_joystick(self, pygame_mod, retry_sec=JOYSTICK_RETRY_SEC):
        logged_waiting = False
        while not self._stop.is_set():
            self._wake_8bitdo("before joystick scan")
            pygame_mod.joystick.quit()
            pygame_mod.joystick.init()
            count = pygame_mod.joystick.get_count()
            if count > 0:
                try:
                    js = pygame_mod.joystick.Joystick(0)
                    js.init()
                except pygame_mod.error as exc:
                    log("local_gp", f"joystick init failed: {exc}")
                    self._stop.wait(timeout=retry_sec)
                    continue
                log("local_gp",
                    f"joystick found: {js.get_name()} "
                    f"(axes={js.get_numaxes()} btns={js.get_numbuttons()} "
                    f"hats={js.get_numhats()})")
                return js
            if not logged_waiting:
                log("local_gp",
                    f"no joystick detected — retrying every {retry_sec:.1f}s "
                    "(check dongle, joydev module, 'input' group)")
                logged_waiting = True
            self._stop.wait(timeout=retry_sec)
        return None

    def _get_mapping(self, js):
        name = (js.get_name() or "").strip().lower()
        num_buttons = js.get_numbuttons()
        current_os = platform.system()
        if "ultimate 2 wireless" in name and current_os == "Linux":
            key = "8bitdo_ultimate2_wireless"
        elif "ultimate wireless controller for pc" in name and current_os == "Linux":
            key = "8bitdo_ultimate_wireless_pc"
        elif "ultimate 2 wireless" in name and current_os == "Windows":
            key = "8bitdo_ultimate2_wireless_windows"
        elif num_buttons <= 12:
            key = "8bitdo_ultimate2_wireless"
        else:
            key = DEFAULT_MAPPING_KEY
        log("local_gp", f"mapping='{key}' for '{js.get_name()}'")
        return GAMEPAD_MAPPINGS[key]

    # ── input reads (safe against varying axis counts) ──────────────────────

    @staticmethod
    def _read_button(js, idx):
        if idx is None or idx < 0:
            return 0
        return js.get_button(idx) if js.get_numbuttons() > idx else 0

    def _read_axis(self, js, idx, pygame_mod):
        if js.get_numaxes() <= idx:
            return 0.0
        try:
            v = js.get_axis(idx)
        except pygame_mod.error:
            return 0.0
        if abs(v) > 1.5:
            v = v / 32767.0
        return _clamp(v, -1.0, 1.0)

    def _read_axis_counts(self, js, idx, pygame_mod):
        return int(round(self._read_axis(js, idx, pygame_mod) * 32767.0))

    # ── main worker loop ────────────────────────────────────────────────────

    def _run(self) -> None:
        pygame_mod = self._init_pygame()
        if pygame_mod is None:
            return

        js = self._wait_for_joystick(pygame_mod)
        if js is None:
            return
        gp = self._get_mapping(js)
        last_soft_reinit_t = time.time()

        # ── State ───────────────────────────────────────────────────────────
        seq = 0
        period = 1.0 / SEND_HZ
        next_t = time.time()

        cruise_zero_idx = CRUISE_LEVELS.index(0.0)
        cruise_level_idx = cruise_zero_idx
        cruise_speed = 0.0

        max_speed = MAX_SPEED_INITIAL
        speed_level = 1   # 1=slow, 2=medium, 3=fast

        # Feature toggle state — mirrors what browser tracks
        ai_on     = False
        bubble_on = False
        xwalk_on  = False
        yield_on  = False

        # A/B toggle buffer:
        #   *_buffered_until = time.time() deadline. If deadline expires
        #   without the paired button, fire the standalone toggle. If the
        #   paired button arrives before the deadline, sequence fires and
        #   the buffer is cleared (toggle suppressed).
        a_buffered_until: Optional[float] = None
        b_buffered_until: Optional[float] = None

        prev_a = prev_b = prev_y = prev_x = 0
        prev_cruise_up = prev_cruise_down = 0
        prev_lights_on = prev_lights_off = 0
        axis3_state = "center"
        axis4_state = "center"
        axis4_neg_taps: list = []
        axis4_pending_speech_deadline: Optional[float] = None

        prev_ai_chord = False
        ai_request_counter = 0

        while not self._stop.is_set():
            # ── tick pacing ─────────────────────────────────────────────
            now = time.time()
            if now < next_t:
                self._stop.wait(timeout=next_t - now)
                if self._stop.is_set():
                    break
            next_t += period

            # ── joystick health ─────────────────────────────────────────
            if pygame_mod.joystick.get_count() == 0:
                log("local_gp", "joystick disconnected, re-waking/waiting…")
                self._wake_8bitdo("joystick count is zero")
                js = self._wait_for_joystick(pygame_mod)
                if js is None:
                    return
                gp = self._get_mapping(js)
                next_t = time.time()
                last_soft_reinit_t = time.time()
                continue

            try:
                pygame_mod.event.pump()
            except pygame_mod.error:
                log("local_gp", "event pump error, re-waking/re-detecting")
                self._wake_8bitdo("after pygame event error")
                js = self._wait_for_joystick(pygame_mod)
                if js is None:
                    return
                gp = self._get_mapping(js)
                next_t = time.time()
                last_soft_reinit_t = time.time()
                continue

            # The 8BitDo USB dongle can remain visible as one joystick while
            # the physical controller has gone to sleep. In that case the
            # get_count()==0 path never fires. Periodically re-wake/reinit
            # while the robot is LOCKED so the pad can come back without a
            # full teleop restart. We avoid doing this while unlocked to keep
            # driving control stable.
            if self._robot_lock and (time.time() - last_soft_reinit_t > JOYSTICK_SOFT_REINIT_SEC):
                last_soft_reinit_t = time.time()
                try:
                    self._wake_8bitdo("periodic while locked")
                    pygame_mod.joystick.quit()
                    pygame_mod.joystick.init()
                    if pygame_mod.joystick.get_count() > 0:
                        js = pygame_mod.joystick.Joystick(0)
                        js.init()
                        gp = self._get_mapping(js)
                        next_t = time.time()
                        continue
                except Exception as exc:
                    log("local_gp", f"periodic joystick reinit failed: {exc}")

            # ── inputs ──────────────────────────────────────────────────
            raw_steer   = -self._read_axis(js, gp["axis_steer"], pygame_mod)
            head_lr     = self._read_axis(js, gp["axis_head_lr"], pygame_mod)
            head_ud     = self._read_axis(js, gp["axis_head_ud"], pygame_mod)
            signal_axis = self._read_axis_counts(js, gp["axis_signal"], pygame_mod)
            sound_axis  = self._read_axis_counts(js, gp["axis_sound"], pygame_mod)

            # Fall back to hat pad for head direction if right stick idle
            if abs(head_lr) < 0.01 and abs(head_ud) < 0.01 and js.get_numhats() > 0:
                hx, hy = js.get_hat(0)
                head_lr = float(hx)
                head_ud = float(-hy)

            # ── Axis 3 → indicator event ────────────────────────────────
            if signal_axis < -AXIS_ACTION_THRESHOLD_COUNTS:
                new_axis3 = "left"
            elif signal_axis > AXIS_ACTION_THRESHOLD_COUNTS:
                new_axis3 = "right"
            else:
                new_axis3 = "center"

            if new_axis3 != axis3_state:
                self._emit_envelope("indicator", {"side": new_axis3})
                axis3_state = new_axis3

            # ── Axis 4 → audio / talk / music ───────────────────────────
            if sound_axis < -AXIS_ACTION_THRESHOLD_COUNTS:
                new_axis4 = "neg"
            elif sound_axis > AXIS_ACTION_THRESHOLD_COUNTS:
                new_axis4 = "pos"
            else:
                new_axis4 = "center"

            axis4_now = time.time()

            # Deferred single-tap-neg speech, fires once the tap window closes
            if (axis4_pending_speech_deadline is not None
                    and axis4_now >= axis4_pending_speech_deadline):
                self._emit_envelope("audio", {"volume_pct": AUDIO_FULL_VOLUME_PCT})
                self._emit_envelope("talk",  {"text": AXIS4_NEG_MESSAGE,
                                              "duration": TALK_DURATION_SEC})
                axis4_pending_speech_deadline = None
                axis4_neg_taps = []

            if new_axis4 != axis4_state:
                if new_axis4 == "neg":
                    axis4_neg_taps = [t for t in axis4_neg_taps
                                      if axis4_now - t <= AXIS4_MULTI_TAP_WINDOW_SEC]
                    axis4_neg_taps.append(axis4_now)
                    if len(axis4_neg_taps) >= 2:
                        self._emit_envelope("music", {"action": "play",
                                                      "track": MUSIC_TRACK_ID})
                        self._emit_envelope("talk",  {"text": "",
                                                      "duration": MUSIC_TALK_DURATION_SEC})
                        axis4_pending_speech_deadline = None
                        axis4_neg_taps = []
                    else:
                        axis4_pending_speech_deadline = (
                            axis4_now + AXIS4_MULTI_TAP_WINDOW_SEC
                        )
                elif new_axis4 == "pos":
                    self._emit_envelope("audio", {"volume_pct": AUDIO_FULL_VOLUME_PCT})
                    self._emit_envelope("talk",  {"text": AXIS4_POS_MESSAGE,
                                                  "duration": TALK_DURATION_SEC})
                axis4_state = new_axis4

            # ── buttons ─────────────────────────────────────────────────
            a_pressed = self._read_button(js, gp["btn_a"])
            b_pressed = self._read_button(js, gp["btn_b"])
            y_pressed = self._read_button(js, gp["btn_y"])
            x_pressed = self._read_button(js, gp["btn_x"])
            cruise_up = self._read_button(js, gp["btn_cruise_up"])
            cruise_down = self._read_button(js, gp["btn_cruise_down"])
            lights_on_pressed  = self._read_button(js, gp["btn_lights_on"])
            lights_off_pressed = self._read_button(js, gp["btn_lights_off"])

            lift_pos_axis = self._read_axis(js, gp["axis_lift_pos"], pygame_mod)
            lift_neg_axis = self._read_axis(js, gp["axis_lift_neg"], pygame_mod)
            lift_pos_cmd = _lift_axis_to_cmd(lift_pos_axis)
            lift_neg_cmd = _lift_axis_to_cmd(lift_neg_axis)
            if lift_pos_cmd > lift_neg_cmd:
                lift = lift_pos_cmd
            elif lift_neg_cmd > lift_pos_cmd:
                lift = -lift_neg_cmd
            else:
                lift = 0

            # ── AI-enable chord (lift triggers, NOT the A toggle) ───────
            press_pos = _clamp((lift_pos_axis + 1.0) * 0.5, 0.0, 1.0)
            press_neg = _clamp((lift_neg_axis + 1.0) * 0.5, 0.0, 1.0)
            ai_chord = (press_pos > AI_ENABLE_PRESS_THRESHOLD
                        and press_neg > AI_ENABLE_PRESS_THRESHOLD)
            if ai_chord and not prev_ai_chord:
                ai_request_counter = AI_REQUEST_REPEAT_PACKETS
                log("local_gp",
                    f"AI enable chord → ai_request x{AI_REQUEST_REPEAT_PACKETS}")
            prev_ai_chord = ai_chord

            if SWAP_XY_BUTTONS:
                y_pressed, x_pressed = x_pressed, y_pressed

            now_t = time.time()
            a_edge = a_pressed and not prev_a
            b_edge = b_pressed and not prev_b
            x_edge = x_pressed and not prev_x
            y_edge = y_pressed and not prev_y
            lights_on_edge  = lights_on_pressed  and not prev_lights_on
            lights_off_edge = lights_off_pressed and not prev_lights_off

            # ── Lights buttons (all three fields together) ──────────────
            if lights_on_edge:
                self._emit_envelope("lights", {
                    "headlights": True, "parklights": True, "strobe": True,
                })
            if lights_off_edge:
                self._emit_envelope("lights", {
                    "headlights": False, "parklights": False, "strobe": False,
                })

            # ── X / Y toggles fire IMMEDIATELY (never part of sequences) ─
            if x_edge:
                xwalk_on = not xwalk_on
                log("local_gp", f"X → xwalk toggle: {xwalk_on}")
                self._emit_envelope("xwalk", {"on": xwalk_on})

            if y_edge:
                yield_on = not yield_on
                log("local_gp", f"Y → yield toggle: {yield_on}")
                self._emit_envelope("yield", {"on": yield_on})

            # ── A / B buffered sequence + toggle resolution ─────────────
            # Sequence detection happens first. If A→B or B→A is completed,
            # clear both buffers and DO NOT fire the standalone toggle.
            sequence_fired = False

            if a_edge:
                # A pressed. Does a recent B mean this is a B→A lock sequence?
                if b_buffered_until is not None and now_t <= b_buffered_until:
                    # B→A within window → LOCK
                    self._robot_lock = True
                    log("local_gp", "robot LOCKED (B→A)")
                    b_buffered_until = None
                    a_buffered_until = None
                    sequence_fired = True
                else:
                    # Buffer A — either an A→B sequence follows, or A→AI toggle
                    a_buffered_until = now_t + LOCK_SEQUENCE_TIMEOUT

            if b_edge and not sequence_fired:
                # B pressed. Does a recent A mean this is an A→B sequence?
                if a_buffered_until is not None and now_t <= a_buffered_until:
                    # A→B within window → UNLOCK or cycle speed
                    if self._robot_lock:
                        self._robot_lock = False
                        max_speed = 1.0
                        speed_level = 1
                        log("local_gp", "robot UNLOCKED (A→B)")
                    else:
                        speed_level = 1 if speed_level >= 3 else speed_level + 1
                        max_speed = float(speed_level)
                        log("local_gp", f"speed level → {speed_level}")
                    a_buffered_until = None
                    b_buffered_until = None
                    sequence_fired = True
                else:
                    # Buffer B — either a B→A sequence follows, or B→bubble toggle
                    b_buffered_until = now_t + LOCK_SEQUENCE_TIMEOUT

            # Buffer expiration → fire the standalone toggles
            if a_buffered_until is not None and now_t > a_buffered_until:
                ai_on = not ai_on
                log("local_gp", f"A → AI toggle: {ai_on}")
                self._emit_envelope("ai_mode", {"on": ai_on})
                a_buffered_until = None

            if b_buffered_until is not None and now_t > b_buffered_until:
                bubble_on = not bubble_on
                log("local_gp", f"B → bubble toggle: {bubble_on}")
                self._emit_envelope("bubble", {"on": bubble_on})
                b_buffered_until = None

            prev_a, prev_b = a_pressed, b_pressed
            prev_y, prev_x = y_pressed, x_pressed
            prev_lights_on  = lights_on_pressed
            prev_lights_off = lights_off_pressed

            # ── Cruise / lin_x computation ──────────────────────────────
            both_cruise_pressed = bool(cruise_up and cruise_down)
            pedal_signed = 0.0
            brake = 1.0 if both_cruise_pressed else 0.0
            brake_active = (brake > BRAKE_THRESHOLD) or both_cruise_pressed

            pedal_speed = pedal_signed * max_speed
            if abs(pedal_speed) <= PEDAL_DEADBAND:
                pedal_speed = 0.0

            if both_cruise_pressed:
                cruise_level_idx = cruise_zero_idx
            else:
                if cruise_up and not prev_cruise_up:
                    cruise_level_idx = min(cruise_level_idx + 1,
                                           len(CRUISE_LEVELS) - 1)
                if cruise_down and not prev_cruise_down:
                    cruise_level_idx = max(cruise_level_idx - 1, 0)

            cruise_speed = CRUISE_LEVELS[cruise_level_idx]
            prev_cruise_up, prev_cruise_down = cruise_up, cruise_down

            cruise_abs_max = min(1.0, max_speed)
            cruise_speed = _clamp(cruise_speed, -cruise_abs_max, cruise_abs_max)

            if brake_active:
                lin_x = 0.0
                cruise_speed = 0.0
                cruise_level_idx = cruise_zero_idx
            elif abs(pedal_speed) > 0.0:
                lin_x = pedal_speed
                cruise_speed = 0.0
                cruise_level_idx = cruise_zero_idx
            else:
                lin_x = cruise_speed

            # ── Head direction ──────────────────────────────────────────
            head = "center"
            axis_head_threshold = 0.5
            if head_lr < -axis_head_threshold:
                head = "left"
            elif head_lr > axis_head_threshold:
                head = "right"
            elif head_ud < -axis_head_threshold:
                head = "up"
            elif head_ud > axis_head_threshold:
                head = "down"

            # ── Steering → ang_z ────────────────────────────────────────
            s = _apply_deadzone(raw_steer, STEER_DEADZONE)
            s = _expo_curve(s, STEER_EXPO)
            s *= STEER_GAIN
            s = _clamp(s, -1.0, 1.0)

            speed_frac = min(abs(lin_x) / max_speed, 1.0) if max_speed > 0 else 0.0
            yaw_limit = (MAX_YAW_INPLACE * (1.0 - speed_frac)
                         + MAX_YAW_MOVING * speed_frac)
            ang_z = _clamp(s * yaw_limit, -yaw_limit, yaw_limit)
            if both_cruise_pressed:
                ang_z = 0.0

            # ── Motion payload (trimmed schema, same as pilot) ──────────
            speed_label = {1: "slow", 2: "medium", 3: "fast"}.get(speed_level, "slow")
            pkt = {
                "seq":         seq,
                "t":           time.time(),
                "lin_x":       round(lin_x, 4),
                "ang_z":       round(ang_z, 4),
                "brake":       round(brake, 3),
                "robot_lock":  self._robot_lock,
                "head":        head,
                "speed":       speed_label,
                "lift":        lift,
                "priority":    self._priority,
                "origin":      "human",
                "ai_request":  "enable" if ai_request_counter > 0 else None,
                "_local":      True,
            }
            if ai_request_counter > 0:
                ai_request_counter -= 1
            seq += 1

            try:
                self._on_motion(pkt, ("local", 0), -1)
            except Exception as exc:
                log("local_gp", f"motion dispatch error: {exc}")

        log("local_gp", "stopped")