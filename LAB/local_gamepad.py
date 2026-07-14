# -*- coding: utf-8 -*-
"""
Local gamepad — evdev-based local controller.

Behavior:
    Motion — unchanged, mirrors pilot_bridge:
        - LB / RB          → cruise level down / up through LIN_X_LEVELS
        - LB + RB          → brake, reset cruise to zero
        - Left stick X     → ang_z (inverted so stick right = right turn)
        - D-pad            → head direction (PTZ)

    ABXY buttons — match browser features:
        - A                → toggle AI mode         (buffered 2s to disambiguate)
        - B                → toggle bubble (lidar)   (buffered 2s)
        - X                → toggle xwalk           (fires immediately)
        - Y                → toggle yield           (fires immediately)

    Local-only lock/speed sequences (WS not in the loop):
        - A → B  (within 2s) while locked   → unlock
        - A → B  (within 2s) while unlocked → cycle speed level 1→2→3→1
        - B → A  (within 2s)                → lock

        A and B are BUFFERED. If the paired button follows within
        LOCK_SEQUENCE_TIMEOUT, we treat the pair as a sequence and DO NOT
        fire the standalone A/B toggle. If it doesn't come, the toggle
        fires when the buffer expires.

    Speech, music, signals, lights, lift — unchanged from previous version.
"""

from __future__ import annotations

import math
import select
import threading
import time
from typing import Callable, Optional

from .common import log


# ── Tunables ─────────────────────────────────────────────────────────────────

SEND_HZ = 50
JOYSTICK_RETRY_SEC = 1.0
LOCAL_IDLE_LOCKED_DISARM_SEC = 1.5
LOCAL_IDLE_UNLOCKED_DISARM_SEC = 60.0

AXIS_ACTION_THRESHOLD = 0.75
HEAD_THRESHOLD = 0.5
STEER_DEADZONE = 0.1
STEER_EXPO = 0.8
STEER_GAIN = 1.0

MAX_YAW_MOVING = 2.0
MAX_YAW_INPLACE = 3.5
MAX_SPEED_INITIAL = 1.0

CRUISE_LEVELS = [-1.0, -0.6, -0.4, -0.2, -0.1, -0.05, 0.0, 0.05, 0.1, 0.2, 0.4, 0.6, 1.0]
PEDAL_DEADBAND = 0.05

LOCK_SEQUENCE_TIMEOUT = 2.0     # also used as the A/B toggle buffer window

LIFT_MIN_CMD = 50
LIFT_MAX_CMD = 255
LIFT_AXIS_DEADBAND = 0.02

AXIS4_NEG_MESSAGE = "Hellow how are you today?"
AXIS4_POS_MESSAGE = "please let me go!"
TALK_DURATION_SEC = 7.0
AXIS4_MULTI_TAP_WINDOW_SEC = 1.0
MUSIC_TRACK_ID = 1
MUSIC_TALK_DURATION_SEC = 60.0
AUDIO_FULL_VOLUME_PCT = 100

SWAP_XY_BUTTONS = False


# ── Helpers ──────────────────────────────────────────────────────────────────

def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _deadzone(x: float, dz: float) -> float:
    if abs(x) <= dz:
        return 0.0
    return math.copysign((abs(x) - dz) / (1.0 - dz), x)


def _expo(x: float, expo: float) -> float:
    return (1.0 - expo) * x + expo * (x ** 3)


def _lift_axis_to_cmd(axis_value: float) -> int:
    press = _clamp((axis_value + 1.0) * 0.5, 0.0, 1.0)
    if press <= LIFT_AXIS_DEADBAND:
        return 0
    return int(round(LIFT_MIN_CMD + press * (LIFT_MAX_CMD - LIFT_MIN_CMD)))


def _axis_norm(value: int, info) -> float:
    if info is None:
        if 0 <= value <= 255:
            return _clamp((value - 128) / 127.0, -1.0, 1.0)
        return _clamp(value / 32767.0, -1.0, 1.0)
    mn = info.min
    mx = info.max
    if mx == mn:
        return 0.0
    mid = (mx + mn) * 0.5
    half = (mx - mn) * 0.5
    return _clamp((value - mid) / half, -1.0, 1.0)


# ── Main class ────────────────────────────────────────────────────────────────

class LocalGamepad(threading.Thread):
    def __init__(
        self,
        on_motion: Callable[[dict, tuple, int], None],
        on_events: Callable[[dict, tuple, int], None],
        on_tts:    Optional[Callable[[dict, tuple, int], None]] = None,
        initial_robot_lock: bool = True,
        priority_value:     int  = 100,
    ) -> None:
        super().__init__(daemon=True, name="local-gamepad-evdev")
        self._on_motion = on_motion
        self._on_events = on_events
        self._on_tts = on_tts
        self._priority = priority_value
        self._init_lock = bool(initial_robot_lock)
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        try:
            from evdev import InputDevice, list_devices, ecodes
        except ImportError:
            log("local_gp", "evdev not installed — local gamepad disabled: pip install evdev")
            return

        logged_waiting = False
        while not self._stop.is_set():
            dev = self._find_device(InputDevice, list_devices, ecodes)
            if dev is None:
                if not logged_waiting:
                    log("local_gp", "no evdev gamepad found — will retry every 1.0s")
                    logged_waiting = True
                self._stop.wait(timeout=JOYSTICK_RETRY_SEC)
                continue
            logged_waiting = False
            try:
                self._run_device(dev, ecodes)
            except OSError:
                log("local_gp", "local evdev gamepad disconnected")
            except Exception as exc:
                log("local_gp", f"local evdev gamepad error: {exc}")
            self._stop.wait(timeout=JOYSTICK_RETRY_SEC)

    # ── Device loop ──────────────────────────────────────────────────────────

    def _run_device(self, dev, ecodes) -> None:
        log("local_gp", f"evdev connected: {dev.path} name={dev.name!r}")
        try:
            dev.grab()
            grabbed = True
            log("local_gp", "evdev device grabbed")
        except Exception:
            grabbed = False
            log("local_gp", "evdev grab unavailable — continuing without grab")

        caps = dev.capabilities(absinfo=True)
        abs_info = {}
        for item in caps.get(ecodes.EV_ABS, []):
            if isinstance(item, tuple):
                abs_info[item[0]] = item[1]
            else:
                abs_info[item] = None

        # ── State ───────────────────────────────────────────────────────────
        seq = 0
        speech_seq = 0
        period = 1.0 / SEND_HZ
        next_send_t = time.monotonic()

        cruise_zero_idx = CRUISE_LEVELS.index(0.0)
        cruise_level_idx = cruise_zero_idx

        camera_modes = ["floor", "orbital", "ai_front", "ai_back"]
        camera_index = 1
        camera_mode = camera_modes[camera_index]

        robot_lock = self._init_lock
        lock_user_engaged = False

        max_speed = MAX_SPEED_INITIAL
        speed_level = 1

        # Feature toggles (local mirror of what browser tracks) — start off.
        ai_on = False
        bubble_on = False
        xwalk_on = False
        yield_on = False

        # A/B toggle buffer — see docstring at top of file.
        # Holds ("A"|"B", press_time_monotonic) for the most recent press
        # of each button that hasn't yet been consumed by a sequence or
        # timed out to fire as a standalone toggle.
        a_buffered_until: Optional[float] = None
        b_buffered_until: Optional[float] = None

        axis = {
            "steer": 0.0, "head_lr": 0.0, "head_ud": 0.0,
            "signal": 0.0, "sound": 0.0,
            "lift_pos": -1.0, "lift_neg": -1.0,
        }
        btn = {
            "a": 0, "b": 0, "x": 0, "y": 0,
            "cruise_up": 0, "cruise_down": 0,
            "lights_on": 0, "lights_off": 0,
        }
        prev_btn = dict(btn)

        axis_signal_state = "center"
        axis_sound_state = "center"
        axis4_neg_taps: list[float] = []
        axis4_pending_speech_deadline: Optional[float] = None

        local_engaged = False
        last_active_t = 0.0

        try:
            while not self._stop.is_set():
                now = time.monotonic()
                timeout = max(0.0, min(0.05, next_send_t - now))
                r, _, _ = select.select([dev.fd], [], [], timeout)

                if r:
                    for event in dev.read():
                        if event.type == ecodes.EV_ABS:
                            self._handle_abs_event(event, ecodes, axis, abs_info)
                        elif event.type == ecodes.EV_KEY:
                            self._handle_key_event(event, ecodes, btn)

                now_wall = time.time()
                now_m = time.monotonic()

                # ── Deferred axis4 negative-tap speech ────────────────────
                if (axis4_pending_speech_deadline is not None
                        and now_wall >= axis4_pending_speech_deadline):
                    self._emit_event({"event": "audio", "volume_pct": AUDIO_FULL_VOLUME_PCT})
                    self._emit_event({"event": "talk", "duration": TALK_DURATION_SEC})
                    self._emit_tts(AXIS4_NEG_MESSAGE, speech_seq)
                    speech_seq += 1
                    axis4_pending_speech_deadline = None
                    axis4_neg_taps = []
                    last_active_t = now_m
                    local_engaged = True

                # ── Button aliasing (SWAP_XY_BUTTONS is compile-time) ────
                a = btn["a"]; b = btn["b"]; x = btn["x"]; y = btn["y"]
                cu = btn["cruise_up"]; cd = btn["cruise_down"]
                lon = btn["lights_on"]; loff = btn["lights_off"]
                if SWAP_XY_BUTTONS:
                    y, x = x, y

                # ── Edges ─────────────────────────────────────────────────
                a_edge  = a  and not prev_btn["a"]
                b_edge  = b  and not prev_btn["b"]
                x_edge  = x  and not prev_btn["x"]
                y_edge  = y  and not prev_btn["y"]
                cu_edge = cu and not prev_btn["cruise_up"]
                cd_edge = cd and not prev_btn["cruise_down"]
                lon_edge  = lon  and not prev_btn["lights_on"]
                loff_edge = loff and not prev_btn["lights_off"]

                any_button_edge = bool(
                    a_edge or b_edge or x_edge or y_edge
                    or cu_edge or cd_edge or lon_edge or loff_edge
                )
                if any_button_edge:
                    last_active_t = now_m
                    local_engaged = True

                # ── Lights ON/OFF buttons — unchanged ─────────────────────
                if lon_edge:
                    self._emit_event({
                        "event": "lights",
                        "headlights": True, "parklights": True, "strobe": True,
                    })
                    log("local_gp", "lights ON")
                if loff_edge:
                    self._emit_event({
                        "event": "lights",
                        "headlights": False, "parklights": False, "strobe": False,
                    })
                    log("local_gp", "lights OFF")

                # ── A / B buffering + lock sequence resolution ────────────
                #
                # When A is pressed: check if B was recently pressed within
                # LOCK_SEQUENCE_TIMEOUT. If so, this is a B→A sequence (lock).
                # Consume both buffers, do NOT fire either standalone toggle.
                # Otherwise, buffer A: it might become part of an A→B sequence,
                # or it might time out and fire the standalone AI toggle.
                #
                # Symmetric for B.

                sequence_fired = False

                if a_edge:
                    if b_buffered_until is not None and now_m <= b_buffered_until:
                        # B→A within window → LOCK
                        robot_lock = True
                        lock_user_engaged = True
                        log("local_gp", "robot LOCKED (B→A)")
                        b_buffered_until = None
                        a_buffered_until = None
                        sequence_fired = True
                    else:
                        a_buffered_until = now_m + LOCK_SEQUENCE_TIMEOUT

                if b_edge and not sequence_fired:
                    if a_buffered_until is not None and now_m <= a_buffered_until:
                        # A→B within window → UNLOCK or cycle speed
                        if robot_lock:
                            robot_lock = False
                            max_speed = 1.0
                            speed_level = 1
                            log("local_gp", "robot UNLOCKED (A→B)")
                        else:
                            speed_level = 1 if speed_level >= 3 else speed_level + 1
                            max_speed = float(speed_level)
                            log("local_gp", f"speed cycle → {speed_level}")
                        lock_user_engaged = True
                        a_buffered_until = None
                        b_buffered_until = None
                        sequence_fired = True
                    else:
                        b_buffered_until = now_m + LOCK_SEQUENCE_TIMEOUT

                # Buffer expiration → fire the standalone toggles.
                if a_buffered_until is not None and now_m > a_buffered_until:
                    ai_on = not ai_on
                    log("local_gp", f"A → AI toggle: {ai_on}")
                    self._emit_event({"event": "ai_mode", "on": ai_on})
                    a_buffered_until = None
                    local_engaged = True

                if b_buffered_until is not None and now_m > b_buffered_until:
                    bubble_on = not bubble_on
                    log("local_gp", f"B → bubble toggle: {bubble_on}")
                    self._emit_event({"event": "bubble", "on": bubble_on})
                    b_buffered_until = None
                    local_engaged = True

                # ── X and Y — fire immediately (never part of sequences) ──
                if x_edge:
                    xwalk_on = not xwalk_on
                    log("local_gp", f"X → xwalk toggle: {xwalk_on}")
                    self._emit_event({"event": "xwalk", "on": xwalk_on})

                if y_edge:
                    yield_on = not yield_on
                    log("local_gp", f"Y → yield toggle: {yield_on}")
                    self._emit_event({"event": "yield", "on": yield_on})

                # ── Turn signals (axis) ───────────────────────────────────
                new_signal = "center"
                if axis["signal"] < -AXIS_ACTION_THRESHOLD:
                    new_signal = "left"
                elif axis["signal"] > AXIS_ACTION_THRESHOLD:
                    new_signal = "right"
                if new_signal != axis_signal_state:
                    if new_signal == "left":
                        self._emit_event({"event": "signals", "left": True, "right": False})
                        last_active_t = now_m; local_engaged = True
                    elif new_signal == "right":
                        self._emit_event({"event": "signals", "left": False, "right": True})
                        last_active_t = now_m; local_engaged = True
                    axis_signal_state = new_signal

                # ── Sound axis (speech / music) ───────────────────────────
                new_sound = "center"
                if axis["sound"] < -AXIS_ACTION_THRESHOLD:
                    new_sound = "neg"
                elif axis["sound"] > AXIS_ACTION_THRESHOLD:
                    new_sound = "pos"
                if new_sound != axis_sound_state:
                    if new_sound == "neg":
                        axis4_neg_taps = [t for t in axis4_neg_taps
                                          if now_wall - t <= AXIS4_MULTI_TAP_WINDOW_SEC]
                        axis4_neg_taps.append(now_wall)
                        if len(axis4_neg_taps) >= 2:
                            self._emit_event({"event": "music", "action": "play", "track": MUSIC_TRACK_ID})
                            self._emit_event({"event": "talk", "duration": MUSIC_TALK_DURATION_SEC})
                            axis4_pending_speech_deadline = None
                            axis4_neg_taps = []
                        else:
                            axis4_pending_speech_deadline = now_wall + AXIS4_MULTI_TAP_WINDOW_SEC
                        last_active_t = now_m; local_engaged = True
                    elif new_sound == "pos":
                        self._emit_event({"event": "audio", "volume_pct": AUDIO_FULL_VOLUME_PCT})
                        self._emit_event({"event": "talk", "duration": TALK_DURATION_SEC})
                        self._emit_tts(AXIS4_POS_MESSAGE, speech_seq)
                        speech_seq += 1
                        last_active_t = now_m; local_engaged = True
                    axis_sound_state = new_sound

                # ── Cruise ────────────────────────────────────────────────
                both_cruise = bool(cu and cd)
                if both_cruise:
                    cruise_level_idx = cruise_zero_idx
                    last_active_t = now_m; local_engaged = True
                else:
                    if cu_edge:
                        cruise_level_idx = min(cruise_level_idx + 1, len(CRUISE_LEVELS) - 1)
                    if cd_edge:
                        cruise_level_idx = max(cruise_level_idx - 1, 0)
                if cu_edge or cd_edge:
                    last_active_t = now_m; local_engaged = True

                # ── Lift ──────────────────────────────────────────────────
                lp = _lift_axis_to_cmd(axis["lift_pos"])
                ln = _lift_axis_to_cmd(axis["lift_neg"])
                if lp > ln:
                    lift = lp
                elif ln > lp:
                    lift = -ln
                else:
                    lift = 0
                if lift != 0:
                    last_active_t = now_m; local_engaged = True

                # ── Head ──────────────────────────────────────────────────
                head = "center"
                if axis["head_lr"] < -HEAD_THRESHOLD:
                    head = "left"
                elif axis["head_lr"] > HEAD_THRESHOLD:
                    head = "right"
                elif axis["head_ud"] < -HEAD_THRESHOLD:
                    head = "up"
                elif axis["head_ud"] > HEAD_THRESHOLD:
                    head = "down"
                if head != "center":
                    last_active_t = now_m; local_engaged = True

                # ── Steering + motion ─────────────────────────────────────
                raw_steer = -axis["steer"]
                s = _deadzone(raw_steer, STEER_DEADZONE)
                s = _expo(s, STEER_EXPO)
                s *= STEER_GAIN
                s = _clamp(s, -1.0, 1.0)

                cruise_speed = CRUISE_LEVELS[cruise_level_idx]
                cruise_abs_max = min(1.0, max_speed)
                cruise_speed = _clamp(cruise_speed, -cruise_abs_max, cruise_abs_max)
                if both_cruise:
                    lin_x = 0.0
                    cruise_speed = 0.0
                    cruise_level_idx = cruise_zero_idx
                else:
                    lin_x = cruise_speed
                if abs(lin_x) > 0.001:
                    last_active_t = now_m; local_engaged = True

                speed_frac = min(abs(lin_x) / max_speed, 1.0) if max_speed > 0 else 0.0
                yaw_limit = MAX_YAW_INPLACE * (1.0 - speed_frac) + MAX_YAW_MOVING * speed_frac
                ang_z = _clamp(s * yaw_limit, -yaw_limit, yaw_limit)
                if both_cruise:
                    ang_z = 0.0
                if abs(ang_z) > 0.001:
                    last_active_t = now_m; local_engaged = True

                # ── Button-ish field for telemetry (last pressed) ─────────
                current_button = 0
                if a: current_button = 1
                elif b: current_button = 2
                elif x: current_button = 3
                elif y: current_button = 4
                elif cd: current_button = 5
                elif cu: current_button = 6
                elif loff: current_button = 7
                elif lon: current_button = 8

                speed_label = {1: "slow", 2: "medium", 3: "fast"}.get(speed_level, "slow")

                # ── Idle disarm ───────────────────────────────────────────
                idle_for = now_m - last_active_t if last_active_t > 0 else 999.0
                if robot_lock:
                    if idle_for > LOCAL_IDLE_LOCKED_DISARM_SEC:
                        local_engaged = False
                else:
                    if idle_for > LOCAL_IDLE_UNLOCKED_DISARM_SEC:
                        local_engaged = False

                # ── Motion packet ─────────────────────────────────────────
                if now_m >= next_send_t:
                    next_send_t = now_m + period
                    if local_engaged:
                        payload = {
                            "seq": seq,
                            "t": time.time(),
                            "lin_x": round(lin_x, 4),
                            "ang_z": round(ang_z, 4),
                            "accel": 0.0,
                            "brake": 1.0 if both_cruise else 0.0,
                            "cruise": round(cruise_speed, 3),
                            "fwd": True,
                            "camera": camera_mode,
                            "head": head,
                            "speed": speed_label,
                            "lift": lift,
                            "priority": self._priority,
                            "button": current_button,
                            "_local": True,
                        }
                        if lock_user_engaged:
                            payload["robot_lock"] = robot_lock
                        try:
                            self._on_motion(payload, ("local", 0), -1)
                        except Exception as exc:
                            log("local_gp", f"motion dispatch error: {exc}")
                    seq += 1

                prev_btn["a"] = a
                prev_btn["b"] = b
                prev_btn["x"] = x
                prev_btn["y"] = y
                prev_btn["cruise_up"] = cu
                prev_btn["cruise_down"] = cd
                prev_btn["lights_on"] = lon
                prev_btn["lights_off"] = loff

        finally:
            if grabbed:
                try: dev.ungrab()
                except Exception: pass
            try: dev.close()
            except Exception: pass

    # ── Event mapping ────────────────────────────────────────────────────────

    def _handle_abs_event(self, event, ecodes, axis: dict, abs_info: dict) -> None:
        code = event.code
        val = _axis_norm(event.value, abs_info.get(code))
        if code == getattr(ecodes, "ABS_X", -1):
            axis["steer"] = val
        elif code in (getattr(ecodes, "ABS_Z", -1),
                      getattr(ecodes, "ABS_GAS", -1)):
            axis["signal"] = val
            axis["lift_neg"] = val
        elif code in (getattr(ecodes, "ABS_RZ", -1),
                      getattr(ecodes, "ABS_BRAKE", -1)):
            axis["sound"] = val
            axis["lift_pos"] = val
        elif code in (getattr(ecodes, "ABS_RX", -1),
                      getattr(ecodes, "ABS_HAT0X", -1),
                      getattr(ecodes, "ABS_HAT1X", -1)):
            axis["head_lr"] = val
        elif code in (getattr(ecodes, "ABS_RY", -1),
                      getattr(ecodes, "ABS_HAT0Y", -1),
                      getattr(ecodes, "ABS_HAT1Y", -1)):
            axis["head_ud"] = val

    def _handle_key_event(self, event, ecodes, btn: dict) -> None:
        pressed = 1 if event.value else 0
        code = event.code
        if code in (getattr(ecodes, "BTN_SOUTH", -1),
                    getattr(ecodes, "BTN_A", -1), 304):
            btn["a"] = pressed
        elif code in (getattr(ecodes, "BTN_EAST", -1),
                      getattr(ecodes, "BTN_B", -1), 305):
            btn["b"] = pressed
        elif code in (getattr(ecodes, "BTN_WEST", -1),
                      getattr(ecodes, "BTN_X", -1), 307):
            btn["x"] = pressed
        elif code in (getattr(ecodes, "BTN_NORTH", -1),
                      getattr(ecodes, "BTN_Y", -1), 308):
            btn["y"] = pressed
        elif code in (getattr(ecodes, "BTN_TL", -1), 310):
            btn["cruise_down"] = pressed
        elif code in (getattr(ecodes, "BTN_TR", -1), 311):
            btn["cruise_up"] = pressed
        elif code in (getattr(ecodes, "BTN_SELECT", -1),
                      getattr(ecodes, "BTN_THUMBL", -1), 314, 317):
            btn["lights_off"] = pressed
        elif code in (getattr(ecodes, "BTN_START", -1),
                      getattr(ecodes, "BTN_THUMBR", -1), 315, 318):
            btn["lights_on"] = pressed

    # ── Device discovery ──────────────────────────────────────────────────────

    def _find_device(self, InputDevice, list_devices, ecodes):
        """Pick a device with gamepad button codes + at least one abs axis."""
        gamepad_keys = {
            getattr(ecodes, "BTN_SOUTH", -1),
            getattr(ecodes, "BTN_EAST", -1),
            getattr(ecodes, "BTN_WEST", -1),
            getattr(ecodes, "BTN_NORTH", -1),
            getattr(ecodes, "BTN_A", -1),
            getattr(ecodes, "BTN_B", -1),
            getattr(ecodes, "BTN_JOYSTICK", -1),
            getattr(ecodes, "BTN_GAMEPAD", -1),
        } - {-1}

        candidates = []
        for path in list_devices():
            try:
                dev = InputDevice(path)
            except Exception:
                continue
            try:
                caps = dev.capabilities()
            except Exception:
                continue
            keys = set(caps.get(ecodes.EV_KEY, []))
            absaxes = set(caps.get(ecodes.EV_ABS, []))
            if not (keys & gamepad_keys):
                continue
            if not absaxes:
                continue
            name = (dev.name or "").lower()
            score = 100 if "8bitdo" in name else 10
            candidates.append((score, path, dev.name))

        if not candidates:
            return None
        candidates.sort(reverse=True)
        score, path, name = candidates[0]
        try:
            dev = InputDevice(path)
            log("local_gp", f"selected evdev device score={score}: {path} {name!r}")
            return dev
        except Exception:
            return None

    # ── Dispatcher helpers ────────────────────────────────────────────────────

    def _emit_event(self, pkt: dict) -> None:
        pkt["_local"] = True
        try:
            self._on_events(pkt, ("local", 0), -1)
        except Exception as exc:
            log("local_gp", f"events dispatch error: {exc}")

    def _emit_tts(self, text: str, seq: int) -> None:
        if self._on_tts is None:
            self._emit_event({"event": "tts", "text": text, "duration": TALK_DURATION_SEC})
            return
        pkt = {
            "type": "stt",
            "seq": seq,
            "ts": time.time(),
            "text": text,
            "_local": True,
        }
        try:
            self._on_tts(pkt, ("local", 0), -1)
        except Exception as exc:
            log("local_gp", f"tts dispatch error: {exc}")