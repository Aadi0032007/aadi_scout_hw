# -*- coding: utf-8 -*-
"""
Local gamepad — evdev-based local controller.

Drop-in replacement for LAB.local_gamepad.LocalGamepad.

Why evdev:
    pygame can see the 8BitDo USB receiver even when the physical controller
    is OFF, and may still poll default/noisy states. evdev is event-driven:
    if no real input event arrives, we do not take local control.

Behavior:
    - No local input event  -> no local packet
    - A -> B sequence       -> local unlock, emit robot_lock=False
    - B -> A sequence       -> local lock, emit robot_lock=True, then idle out
    - While unlocked        -> emit motion at SEND_HZ using latest state
    - Lights, signals, TTS, music events are preserved
    - Packet schema matches remote/operator packets
"""

from __future__ import annotations

import math
import platform
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

LOCK_SEQUENCE_TIMEOUT = 2.0

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
    """
    axis_value is normalized [-1..+1].
    Rest may be -1 for triggers, or 0 for sticks depending on driver.
    We map only strong positive values to lift command.
    """
    press = _clamp((axis_value + 1.0) * 0.5, 0.0, 1.0)
    if press <= LIFT_AXIS_DEADBAND:
        return 0
    return int(round(LIFT_MIN_CMD + press * (LIFT_MAX_CMD - LIFT_MIN_CMD)))


def _axis_norm(value: int, info) -> float:
    """
    Normalize evdev abs value to [-1, +1] using device absinfo.
    """
    if info is None:
        # Fallback for common 0..255 style axes.
        if 0 <= value <= 255:
            return _clamp((value - 128) / 127.0, -1.0, 1.0)
        # Fallback for signed int16 style axes.
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
    """
    Same constructor/signature as the pygame LocalGamepad.

    teleop.py can keep:

        from LAB.local_gamepad import LocalGamepad

    and:

        local = LocalGamepad(
            on_motion=on_motion_packet,
            on_events=on_events_packet,
            on_tts=on_tts_packet,
            initial_robot_lock=True,
            priority_value=cfg.local_dongle_priority,
        )
    """

    def __init__(
        self,
        on_motion: Callable[[dict, tuple, int], None],
        on_events: Callable[[dict, tuple, int], None],
        on_tts:    Callable[[dict, tuple, int], None],
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

    # ── Thread main ──────────────────────────────────────────────────────────

    def run(self) -> None:
        try:
            from evdev import InputDevice, list_devices, ecodes
        except ImportError:
            log("local_gp", "evdev not installed — local gamepad disabled: pip install evdev")
            return

        while not self._stop.is_set():
            dev = self._find_device(InputDevice, list_devices, ecodes)

            if dev is None:
                # log("local_gp", "no evdev gamepad found — waiting…")
                self._stop.wait(timeout=JOYSTICK_RETRY_SEC)
                continue

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

        # Try to grab device so events do not leak elsewhere.
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
            # item can be (code, AbsInfo) depending on evdev version.
            if isinstance(item, tuple):
                abs_info[item[0]] = item[1]
            else:
                abs_info[item] = None

        # State mirrors operator/local pygame logic.
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

        lock_seq: list[tuple[str, float]] = []
        seq_start_state: Optional[str] = None
        sequence_active = False

        axis = {
            "steer": 0.0,
            "head_lr": 0.0,
            "head_ud": 0.0,
            "signal": 0.0,
            "sound": 0.0,
            "lift_pos": -1.0,
            "lift_neg": -1.0,
        }

        btn = {
            "a": 0,
            "b": 0,
            "x": 0,
            "y": 0,
            "cruise_up": 0,
            "cruise_down": 0,
            "lights_on": 0,
            "lights_off": 0,
        }

        prev_btn = dict(btn)

        axis_signal_state = "center"
        axis_sound_state = "center"
        axis4_neg_taps: list[float] = []
        axis4_pending_speech_deadline: Optional[float] = None

        local_engaged = False
        last_active_t = 0.0

        # Main event + periodic emit loop.
        try:
            while not self._stop.is_set():
                now = time.monotonic()

                # Wait for either an evdev event or next packet tick.
                timeout = max(0.0, min(0.05, next_send_t - now))
                r, _, _ = select.select([dev.fd], [], [], timeout)

                event_happened = False

                if r:
                    for event in dev.read():
                        event_happened = True

                        if event.type == ecodes.EV_ABS:
                            self._handle_abs_event(event, ecodes, axis, abs_info)

                        elif event.type == ecodes.EV_KEY:
                            self._handle_key_event(event, ecodes, btn)

                now_wall = time.time()

                # Handle deferred speech after event processing.
                if (
                    axis4_pending_speech_deadline is not None
                    and now_wall >= axis4_pending_speech_deadline
                ):
                    self._emit_event({"event": "audio", "volume_pct": AUDIO_FULL_VOLUME_PCT})
                    self._emit_event({"event": "talk", "duration": TALK_DURATION_SEC})
                    self._emit_tts(AXIS4_NEG_MESSAGE, speech_seq)
                    speech_seq += 1
                    axis4_pending_speech_deadline = None
                    axis4_neg_taps = []
                    last_active_t = time.monotonic()
                    local_engaged = True

                # Button edges.
                a = btn["a"]
                b = btn["b"]
                x = btn["x"]
                y = btn["y"]
                cu = btn["cruise_up"]
                cd = btn["cruise_down"]
                lon = btn["lights_on"]
                loff = btn["lights_off"]

                if SWAP_XY_BUTTONS:
                    y, x = x, y

                a_edge = a and not prev_btn["a"]
                b_edge = b and not prev_btn["b"]
                x_edge = x and not prev_btn["x"]
                y_edge = y and not prev_btn["y"]
                cu_edge = cu and not prev_btn["cruise_up"]
                cd_edge = cd and not prev_btn["cruise_down"]
                lon_edge = lon and not prev_btn["lights_on"]
                loff_edge = loff and not prev_btn["lights_off"]

                any_button_edge = bool(
                    a_edge or b_edge or x_edge or y_edge
                    or cu_edge or cd_edge or lon_edge or loff_edge
                )

                if any_button_edge:
                    last_active_t = time.monotonic()
                    local_engaged = True

                # Lights buttons.
                if lon_edge:
                    self._emit_event({
                        "event": "lights",
                        "headlights": True,
                        "parklights": True,
                        "strobe": True,
                    })
                    log("local_gp", "lights ON")

                if loff_edge:
                    self._emit_event({
                        "event": "lights",
                        "headlights": False,
                        "parklights": False,
                        "strobe": False,
                    })
                    log("local_gp", "lights OFF")

                # Signals axis.
                new_signal = "center"
                if axis["signal"] < -AXIS_ACTION_THRESHOLD:
                    new_signal = "left"
                elif axis["signal"] > AXIS_ACTION_THRESHOLD:
                    new_signal = "right"

                if new_signal != axis_signal_state:
                    if new_signal == "left":
                        self._emit_event({"event": "signals", "left": True, "right": False})
                        last_active_t = time.monotonic()
                        local_engaged = True
                    elif new_signal == "right":
                        self._emit_event({"event": "signals", "left": False, "right": True})
                        last_active_t = time.monotonic()
                        local_engaged = True
                    axis_signal_state = new_signal

                # Sound axis.
                new_sound = "center"
                if axis["sound"] < -AXIS_ACTION_THRESHOLD:
                    new_sound = "neg"
                elif axis["sound"] > AXIS_ACTION_THRESHOLD:
                    new_sound = "pos"

                if new_sound != axis_sound_state:
                    now_wall = time.time()

                    if new_sound == "neg":
                        axis4_neg_taps = [
                            t for t in axis4_neg_taps
                            if now_wall - t <= AXIS4_MULTI_TAP_WINDOW_SEC
                        ]
                        axis4_neg_taps.append(now_wall)

                        if len(axis4_neg_taps) >= 2:
                            self._emit_event({
                                "event": "music",
                                "action": "play",
                                "track": MUSIC_TRACK_ID,
                            })
                            self._emit_event({
                                "event": "talk",
                                "duration": MUSIC_TALK_DURATION_SEC,
                            })
                            axis4_pending_speech_deadline = None
                            axis4_neg_taps = []
                        else:
                            axis4_pending_speech_deadline = now_wall + AXIS4_MULTI_TAP_WINDOW_SEC

                        last_active_t = time.monotonic()
                        local_engaged = True

                    elif new_sound == "pos":
                        self._emit_event({"event": "audio", "volume_pct": AUDIO_FULL_VOLUME_PCT})
                        self._emit_event({"event": "talk", "duration": TALK_DURATION_SEC})
                        self._emit_tts(AXIS4_POS_MESSAGE, speech_seq)
                        speech_seq += 1
                        last_active_t = time.monotonic()
                        local_engaged = True

                    axis_sound_state = new_sound

                # Lock sequence bookkeeping.
                pre_camera_mode = camera_mode
                now_seq = time.time()

                if a_edge:
                    if not lock_seq and seq_start_state is None:
                        seq_start_state = pre_camera_mode
                    lock_seq.append(("A", now_seq))

                if b_edge:
                    if not lock_seq and seq_start_state is None:
                        seq_start_state = pre_camera_mode
                    lock_seq.append(("B", now_seq))

                if y_edge:
                    if not lock_seq and seq_start_state is None:
                        seq_start_state = pre_camera_mode
                    lock_seq.append(("Y", now_seq))

                if x_edge:
                    if not lock_seq and seq_start_state is None:
                        seq_start_state = pre_camera_mode
                    lock_seq.append(("X", now_seq))

                lock_seq = [
                    (k, t) for (k, t) in lock_seq
                    if now_seq - t <= LOCK_SEQUENCE_TIMEOUT
                ]

                if not sequence_active and len(lock_seq) >= 2:
                    first2 = "".join(k for k, _ in lock_seq[:2])
                    if first2 in ("AB", "BA"):
                        sequence_active = True

                sequence_matched = False

                if len(lock_seq) >= 2:
                    last2 = lock_seq[-2:]
                    seq_str = "".join(k for k, _ in last2)
                    span = last2[-1][1] - last2[0][1]

                    if span <= LOCK_SEQUENCE_TIMEOUT:
                        if seq_str == "AB":
                            if robot_lock:
                                robot_lock = False
                                max_speed = 1.0
                                speed_level = 1
                                log("local_gp", "robot unlocked A→B")
                            else:
                                speed_level += 1
                                if speed_level > 3:
                                    speed_level = 1
                                max_speed = float(speed_level)
                                log("local_gp", f"speed cycle → {speed_level}")

                            lock_user_engaged = True
                            sequence_matched = True
                            lock_seq = []
                            last_active_t = time.monotonic()
                            local_engaged = True

                        elif seq_str == "BA":
                            robot_lock = True
                            log("local_gp", "robot locked B→A")

                            lock_user_engaged = True
                            sequence_matched = True
                            lock_seq = []
                            last_active_t = time.monotonic()
                            local_engaged = True

                if sequence_matched:
                    camera_mode = seq_start_state if seq_start_state is not None else pre_camera_mode
                    seq_start_state = None
                    sequence_active = False

                elif not lock_seq and seq_start_state is not None:
                    seq_start_state = None
                    sequence_active = False

                # Camera cycling, only when no lock sequence in flight.
                if not sequence_matched and not sequence_active:
                    if x_edge:
                        camera_index = (camera_index + 1) % len(camera_modes)
                        camera_mode = camera_modes[camera_index]
                        last_active_t = time.monotonic()
                        local_engaged = True

                    if y_edge:
                        camera_index = (camera_index - 1) % len(camera_modes)
                        camera_mode = camera_modes[camera_index]
                        last_active_t = time.monotonic()
                        local_engaged = True

                # Cruise.
                both_cruise = bool(cu and cd)

                if both_cruise:
                    cruise_level_idx = cruise_zero_idx
                    last_active_t = time.monotonic()
                    local_engaged = True
                else:
                    if cu_edge:
                        cruise_level_idx = min(cruise_level_idx + 1, len(CRUISE_LEVELS) - 1)
                    if cd_edge:
                        cruise_level_idx = max(cruise_level_idx - 1, 0)

                if cu_edge or cd_edge:
                    last_active_t = time.monotonic()
                    local_engaged = True

                # Compute lift.
                lp = _lift_axis_to_cmd(axis["lift_pos"])
                ln = _lift_axis_to_cmd(axis["lift_neg"])

                if lp > ln:
                    lift = lp
                elif ln > lp:
                    lift = -ln
                else:
                    lift = 0

                if lift != 0:
                    last_active_t = time.monotonic()
                    local_engaged = True

                # Head.
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
                    last_active_t = time.monotonic()
                    local_engaged = True

                # Steering and motion.
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
                    last_active_t = time.monotonic()
                    local_engaged = True

                speed_frac = min(abs(lin_x) / max_speed, 1.0) if max_speed > 0 else 0.0
                yaw_limit = MAX_YAW_INPLACE * (1.0 - speed_frac) + MAX_YAW_MOVING * speed_frac
                ang_z = _clamp(s * yaw_limit, -yaw_limit, yaw_limit)

                if both_cruise:
                    ang_z = 0.0

                if abs(ang_z) > 0.001:
                    last_active_t = time.monotonic()
                    local_engaged = True

                # Button field.
                current_button = 0
                if a:
                    current_button = 1
                elif b:
                    current_button = 2
                elif x:
                    current_button = 3
                elif y:
                    current_button = 4
                elif cd:
                    current_button = 5
                elif cu:
                    current_button = 6
                elif loff:
                    current_button = 7
                elif lon:
                    current_button = 8

                speed_label = {1: "slow", 2: "medium", 3: "fast"}.get(speed_level, "slow")

                # Idle release.
                now_m = time.monotonic()
                idle_for = now_m - last_active_t if last_active_t > 0 else 999.0

                if robot_lock:
                    if idle_for > LOCAL_IDLE_LOCKED_DISARM_SEC:
                        local_engaged = False
                else:
                    if idle_for > LOCAL_IDLE_UNLOCKED_DISARM_SEC:
                        local_engaged = False

                # Periodic motion packet only when local is engaged.
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

                        # Critical:
                        # Do not send initial robot_lock=True just because local exists.
                        # Only send robot_lock after a local A→B or B→A sequence.
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
                try:
                    dev.ungrab()
                except Exception:
                    pass
            try:
                dev.close()
            except Exception:
                pass

    # ── Event mapping ────────────────────────────────────────────────────────

    def _handle_abs_event(self, event, ecodes, axis: dict, abs_info: dict) -> None:
        code = event.code
        val = _axis_norm(event.value, abs_info.get(code))

        # Common Linux/8BitDo mappings. These cover both old simple evdev and
        # modern 8BitDo modes. Adjust here if your evtest output differs.

        # Steering.
        if code in (
            getattr(ecodes, "ABS_X", -1),
        ):
            axis["steer"] = val

        # Triggers / secondary axes.
        elif code in (
            getattr(ecodes, "ABS_Z", -1),
            getattr(ecodes, "ABS_GAS", -1),
        ):
            # On many 8BitDo profiles this is signal or lift neg.
            axis["signal"] = val
            axis["lift_neg"] = val

        elif code in (
            getattr(ecodes, "ABS_RZ", -1),
            getattr(ecodes, "ABS_BRAKE", -1),
        ):
            # On many 8BitDo profiles this is sound or lift pos.
            axis["sound"] = val
            axis["lift_pos"] = val

        # Right stick / D-pad axes for head.
        elif code in (
            getattr(ecodes, "ABS_RX", -1),
            getattr(ecodes, "ABS_HAT0X", -1),
        ):
            axis["head_lr"] = val

        elif code in (
            getattr(ecodes, "ABS_RY", -1),
            getattr(ecodes, "ABS_HAT0Y", -1),
        ):
            axis["head_ud"] = val

        # Some controllers expose D-pad as ABS_HAT1X/Y or ABS_Y.
        elif code in (
            getattr(ecodes, "ABS_HAT1X", -1),
        ):
            axis["head_lr"] = val

        elif code in (
            getattr(ecodes, "ABS_HAT1Y", -1),
        ):
            axis["head_ud"] = val

    def _handle_key_event(self, event, ecodes, btn: dict) -> None:
        pressed = 1 if event.value else 0
        code = event.code

        # A/B/X/Y aliases.
        if code in (
            getattr(ecodes, "BTN_SOUTH", -1),
            getattr(ecodes, "BTN_A", -1),
            304,
        ):
            btn["a"] = pressed

        elif code in (
            getattr(ecodes, "BTN_EAST", -1),
            getattr(ecodes, "BTN_B", -1),
            305,
        ):
            btn["b"] = pressed

        elif code in (
            getattr(ecodes, "BTN_WEST", -1),
            getattr(ecodes, "BTN_X", -1),
            307,
        ):
            btn["x"] = pressed

        elif code in (
            getattr(ecodes, "BTN_NORTH", -1),
            getattr(ecodes, "BTN_Y", -1),
            308,
        ):
            btn["y"] = pressed

        # Shoulders / cruise.
        elif code in (
            getattr(ecodes, "BTN_TL", -1),
            310,
        ):
            btn["cruise_down"] = pressed

        elif code in (
            getattr(ecodes, "BTN_TR", -1),
            311,
        ):
            btn["cruise_up"] = pressed

        # Start/select/thumb buttons for lights. These may vary by 8BitDo mode.
        elif code in (
            getattr(ecodes, "BTN_SELECT", -1),
            getattr(ecodes, "BTN_THUMBL", -1),
            314,
            317,
        ):
            btn["lights_off"] = pressed

        elif code in (
            getattr(ecodes, "BTN_START", -1),
            getattr(ecodes, "BTN_THUMBR", -1),
            315,
            318,
        ):
            btn["lights_on"] = pressed

    # ── Device discovery ──────────────────────────────────────────────────────

    def _find_device(self, InputDevice, list_devices, ecodes):
        """
        Pick first likely joystick/gamepad.

        This intentionally ignores keyboards, mice, consumer/system controls.
        """
        candidates = []

        for path in list_devices():
            try:
                dev = InputDevice(path)
            except Exception:
                continue

            name = (dev.name or "").lower()
            phys = (dev.phys or "").lower()

            if any(x in name for x in ("keyboard", "mouse", "consumer", "system")):
                continue

            try:
                caps = dev.capabilities()
            except Exception:
                continue

            has_keys = ecodes.EV_KEY in caps
            has_abs = ecodes.EV_ABS in caps

            if not has_keys or not has_abs:
                continue

            score = 0
            if "8bitdo" in name:
                score += 100
            if "ultimate" in name:
                score += 50
            if "x-box" in name or "xbox" in name:
                score += 20
            if "gamepad" in name or "joystick" in name:
                score += 20
            if "usb" in phys:
                score += 5

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