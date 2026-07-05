# -*- coding: utf-8 -*-
"""
prod_revopilot_udp_tcp_gamepad_cmds_win_lin.py

REVOPILOT gamepad teleop — REDESIGNED transport layer.

TWO SOCKETS ONLY:
    1. UDP motion channel        -> port 55999, 50 Hz, fire-and-forget
    2. TCP unified event channel -> port 57000, persistent, ack-per-message

UDP MOTION PAYLOAD (unacked, port 55999, 50 Hz):
    {
        "seq":        <int>,
        "t":          <float unix ts>,
        "lin_x":      <float -max_speed..+max_speed>,   # sign = direction
        "ang_z":      <float ±yaw_limit>,
        "brake":      <float 0.0..1.0>,
        "robot_lock": <bool>,
        "head":       "center"|"left"|"right"|"up"|"down",
        "speed":      "slow"|"medium"|"fast",
        "lift":       <int -255..-50, 0, +50..+255>,
        "origin":     "human",
        "ai_request": "enable" | null
    }

TCP EVENT ENVELOPE (acked, port 57000):
    {
        "seq":  <int>,
        "t":    <float unix ts>,
        "type": "lights" | "audio" | "talk" | "music" | "indicator",
        "data": { ... type-specific fields ... }
    }
    Framing: 4-byte big-endian length prefix + UTF-8 JSON body.
    Robot ack: {"ack_of": <seq>, "status": "ok"|"error", "t": <ts>[, "error": ...]}

TYPE PAYLOADS:
    lights    -> {"headlights": bool, "parklights": bool, "strobe": bool}
    audio     -> {"volume_pct": int}
    talk      -> {"text": str, "duration": float}     # combined talk+speech
    music     -> {"action": "play", "track": int}
    indicator -> {"side": "left"|"right"|"center"}
    ptz       -> {"action": "capture_home"|"goto_home"}

TRIGGERS:
    A/B lock sequence  -> lock/unlock (motion payload robot_lock field)
    A->B while locked  -> also sends ptz capture_home
    Lights-ON button   -> lights ALL-ON blink + ptz goto_home
    Lights-OFF button  -> lights all off
    Axis 3 threshold   -> indicator left/right/center
    Axis 4 single tap  -> audio volume + talk (speech)
    Axis 4 double tap  -> music play + long talk blink
    Both lift triggers -> ai_request "enable" for N motion packets

BUTTON MAPPING is unchanged from the previous release. Camera cycling on
X/Y is removed (camera control has moved off the gamepad). All other
button behavior (A/B lock sequence, cruise ±, lights on/off buttons,
lift triggers, AI-enable chord) is preserved.
"""

import argparse
import json
import math
import platform
import queue
import socket
import struct
import threading
import time

import pygame


# =====================================================================
# CONFIG
# =====================================================================

ROBOT_IPS = {
    "SEGWAY":       "100.109.21.91",
    "ELEPHANT":     "100.80.7.54",
    "NATIVE":       "127.0.0.1",
    "GB-NANO-DEV":  "100.125.123.72",
}

start_record = True  # retained for local logging only; not sent on the wire

# ---- REMOTE UDP + TCP CONTROL --------------------------------------
DEFAULT_ROBOT = "SEGWAY"
MOTION_UDP_PORT = 55999
EVENT_TCP_PORT = 57000
# ---------------------------------------------------------------------

SEND_HZ = 50
AXIS_ACTION_THRESHOLD_COUNTS = 30000

AXIS4_NEG_MESSAGE = "Hellow how are you today?"
AXIS4_POS_MESSAGE = "please let me go!"
TALK_DURATION_SEC = 7.0
AXIS4_MULTI_TAP_WINDOW_SEC = 1.0
MUSIC_TRACK_ID = 1
MUSIC_TALK_DURATION_SEC = 60.0
AUDIO_FULL_VOLUME_PCT = 100

# Steering behavior
MAX_YAW_MOVING = 2.0
MAX_YAW_INPLACE = 3.5
STEER_DEADZONE = 0.1
STEER_EXPO = 0.8
STEER_GAIN = 1.0

# Pedal & cruise (cruise still drives lin_x internally; not sent separately)
BRAKE_THRESHOLD = 0.2
CRUISE_LEVELS = [-1.0, -0.6, -0.4, -0.2, -0.1, -0.05,
                 0.0, 0.05, 0.1, 0.2, 0.4, 0.6, 1.0]
PEDAL_DEADBAND = 0.05
LIFT_MIN_CMD = 50
LIFT_MAX_CMD = 255
LIFT_AXIS_DEADBAND = 0.02
PRINT_LIFT_DEBUG = False
PRINT_CRUISE_DEBUG = True

# AI-enable chord (both lift triggers >0.95 in press-space)
AI_ENABLE_PRESS_THRESHOLD = 0.95
AI_REQUEST_REPEAT_PACKETS = 5

# Lock sequence
LOCK_SEQUENCE_TIMEOUT = 2.0
MAX_SPEED_INITIAL = 1.0
SWAP_XY_BUTTONS = False

# TCP event channel
TCP_CONNECT_TIMEOUT_SEC = 3.0
TCP_ACK_TIMEOUT_SEC = 2.0
TCP_RECONNECT_INITIAL_SEC = 0.5
TCP_RECONNECT_MAX_SEC = 8.0
TCP_RECONNECT_MULTIPLIER = 2.0
TCP_SEND_QUEUE_MAX = 256
LENGTH_PREFIX_FORMAT = ">I"
LENGTH_PREFIX_SIZE = struct.calcsize(LENGTH_PREFIX_FORMAT)


# ---- Gamepad mappings (UNCHANGED) ----------------------------------
GAMEPAD_MAPPINGS = {
    "8bitdo_ultimate_wireless_pc": {
        "axis_steer": 0,
        "axis_sound": 3,
        "axis_signal": 4,  # just check this and try 2
        "axis_head_lr": 6,
        "axis_head_ud": 7,
        "axis_lift_pos": 4,
        "axis_lift_neg": 5,
        "btn_a": 0, "btn_b": 1, "btn_x": 3, "btn_y": 4,
        "btn_cruise_down": 6, "btn_cruise_up": 7,
        "btn_lights_on": 11, "btn_lights_off": 10,
    },
    "8bitdo_ultimate2_wireless": {
        "axis_steer": 0,
        "axis_signal": 3,
        "axis_sound": 4,
        "axis_head_lr": 6,
        "axis_head_ud": 7,
        "axis_lift_pos": 5,
        "axis_lift_neg": 2,
        "btn_a": 0, "btn_b": 1, "btn_x": 2, "btn_y": 3,
        "btn_cruise_down": 4, "btn_cruise_up": 5,
        "btn_lights_on": 7, "btn_lights_off": 6,
    },
    "8bitdo_ultimate2_wireless_windows": {
        "axis_steer": 0,
        "axis_signal": 2,
        "axis_sound": 3,
        "axis_head_lr": 6,
        "axis_head_ud": 7,
        "axis_lift_pos": 5,
        "axis_lift_neg": 4,
        "btn_a": 0, "btn_b": 1, "btn_x": 2, "btn_y": 3,
        "btn_cruise_down": 4, "btn_cruise_up": 5,
        "btn_lights_on": 7, "btn_lights_off": 6,
    },
}

# Button-number identity map (kept as-is; no longer sent on the wire).
BUTTON_NUM_MAP = {
    "btn_a": 1, "btn_b": 2, "btn_x": 3, "btn_y": 4,
    "btn_cruise_down": 5, "btn_cruise_up": 6,
    "btn_lights_off": 7, "btn_lights_on": 8,
}

DEFAULT_MAPPING_KEY = "8bitdo_ultimate_wireless_pc"
JOYSTICK_RETRY_SEC = 1.0


# =====================================================================
# UTIL
# =====================================================================

def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def apply_deadzone(x, dz):
    if abs(x) <= dz:
        return 0.0
    return math.copysign((abs(x) - dz) / (1.0 - dz), x)


def expo_curve(x, expo):
    return (1.0 - expo) * x + expo * (x ** 3)


def lift_axis_to_cmd(axis_value):
    press = clamp((axis_value + 1.0) * 0.5, 0.0, 1.0)
    if press <= LIFT_AXIS_DEADBAND:
        return 0
    return int(round(LIFT_MIN_CMD + press * (LIFT_MAX_CMD - LIFT_MIN_CMD)))


# =====================================================================
# TCP EVENT CHANNEL CLIENT
# =====================================================================

class EventChannelClient:
    """
    Persistent TCP client for the unified event channel (port 57000).

    - Non-blocking send() from caller's perspective; a background thread
      owns the socket and delivers messages in order.
    - 4-byte length-prefixed framing.
    - Every message is expected to be acknowledged by the robot with:
        {"ack_of": <seq>, "status": "ok"|"error", "t": <ts>[, "error": ...]}
      Ack outcomes are logged; delivery is not retried at this layer
      (TCP already retransmits at the transport level).
    - Auto-reconnects with exponential backoff on any disconnect.
    """

    def __init__(self, host, port):
        self.host = host
        self.port = port

        self._sock = None
        self._connected = threading.Event()
        self._stop_flag = threading.Event()
        self._send_queue = queue.Queue(maxsize=TCP_SEND_QUEUE_MAX)
        self._worker_thread = None

        self._seq_lock = threading.Lock()
        self._seq = 0
        self._last_send_t = 0.0

        # Stats — read by the main loop for the status line
        self.stats_lock = threading.Lock()
        self.sent_ok = 0
        self.sent_err = 0
        self.queued = 0
        self.dropped = 0
        self.reconnects = 0
        self.last_rtt_ms = 0.0
        self.last_error = ""

    def qsize(self):
        return self._send_queue.qsize()

    # ------------------------------------------------ public
    def start(self):
        self._stop_flag.clear()
        self._worker_thread = threading.Thread(
            target=self._run, name="EventChannelClient", daemon=True
        )
        self._worker_thread.start()

    def stop(self):
        self._stop_flag.set()
        try:
            self._send_queue.put_nowait(None)
        except queue.Full:
            pass
        if self._worker_thread is not None:
            self._worker_thread.join(timeout=2.0)
        self._close_socket()

    def is_connected(self):
        return self._connected.is_set()

    def send(self, type_, data):
        """
        Queue an event for delivery. Non-blocking.
        Returns True if queued, False if the queue was full.
        """
        with self._seq_lock:
            self._seq += 1
            seq = self._seq
        envelope = {
            "seq": seq,
            "t": time.time(),
            "type": type_,
            "data": data,
        }
        try:
            self._send_queue.put_nowait(envelope)
            with self.stats_lock:
                self.queued += 1
            print(f"\n[TCP →] queued  type={type_:<9} seq={seq} "
                  f"qsize={self._send_queue.qsize()}/{TCP_SEND_QUEUE_MAX} "
                  f"data={json.dumps(data, separators=(',', ':'))}",
                  flush=True)
            return True
        except queue.Full:
            with self.stats_lock:
                self.dropped += 1
            print(f"\n[TCP ✗] DROP    type={type_:<9} seq={seq} "
                  f"(queue full at {TCP_SEND_QUEUE_MAX})", flush=True)
            return False

    # ------------------------------------------------ internals
    def _run(self):
        backoff = TCP_RECONNECT_INITIAL_SEC
        while not self._stop_flag.is_set():
            try:
                print(f"\n[TCP ..] connecting to {self.host}:{self.port} "
                      f"(timeout={TCP_CONNECT_TIMEOUT_SEC}s)", flush=True)
                self._connect()
                backoff = TCP_RECONNECT_INITIAL_SEC
                self._pump_until_disconnected()
            except (OSError, socket.timeout) as e:
                self._connected.clear()
                self._close_socket()
                with self.stats_lock:
                    self.last_error = str(e)
                if self._stop_flag.is_set():
                    break
                print(f"\n[TCP ✗] link error: {e!r}; "
                      f"retrying in {backoff:.1f}s", flush=True)
                time.sleep(backoff)
                backoff = min(backoff * TCP_RECONNECT_MULTIPLIER,
                              TCP_RECONNECT_MAX_SEC)

    def _connect(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(TCP_CONNECT_TIMEOUT_SEC)
        sock.connect((self.host, self.port))
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._sock = sock
        self._connected.set()
        with self.stats_lock:
            self.reconnects += 1
        local = sock.getsockname()
        print(f"\n[TCP ✓] CONNECTED to {self.host}:{self.port} "
              f"(local={local[0]}:{local[1]}, reconnect #{self.reconnects})",
              flush=True)

    def _pump_until_disconnected(self):
        """Send-then-wait-for-ack loop. One in-flight message at a time."""
        while not self._stop_flag.is_set():
            try:
                envelope = self._send_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            if envelope is None:
                break
            self._send_one(envelope)
            self._await_ack(envelope["seq"], envelope["type"])

    def _send_one(self, envelope):
        body = json.dumps(envelope).encode("utf-8")
        header = struct.pack(LENGTH_PREFIX_FORMAT, len(body))
        self._sock.settimeout(None)
        self._last_send_t = time.time()
        self._sock.sendall(header + body)
        print(f"\n[TCP →] on-wire type={envelope['type']:<9} "
              f"seq={envelope['seq']} bytes={len(header) + len(body)}",
              flush=True)

    def _read_exactly(self, n):
        buf = b""
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise OSError("peer closed during ack read")
            buf += chunk
        return buf

    def _await_ack(self, expected_seq, type_):
        self._sock.settimeout(TCP_ACK_TIMEOUT_SEC)
        try:
            header = self._read_exactly(LENGTH_PREFIX_SIZE)
            (msg_len,) = struct.unpack(LENGTH_PREFIX_FORMAT, header)
            if msg_len <= 0 or msg_len > 1_000_000:
                raise OSError(f"suspicious ack length {msg_len}")
            body = self._read_exactly(msg_len)
            ack = json.loads(body.decode("utf-8"))
            rtt_ms = (time.time() - self._last_send_t) * 1000.0
        finally:
            self._sock.settimeout(None)

        ack_of = ack.get("ack_of")
        status = ack.get("status")
        with self.stats_lock:
            self.last_rtt_ms = rtt_ms
            if status == "ok":
                self.sent_ok += 1
            else:
                self.sent_err += 1

        if ack_of != expected_seq:
            print(f"\n[TCP ⚠]  ACK SEQ MISMATCH: expected {expected_seq}, "
                  f"got {ack_of} (type={type_})", flush=True)
        if status == "ok":
            print(f"\n[TCP ✓] ACK     type={type_:<9} seq={expected_seq} "
                  f"rtt={rtt_ms:.1f}ms", flush=True)
        else:
            err = ack.get("error", "unknown")
            print(f"\n[TCP ✗] NACK    type={type_:<9} seq={expected_seq} "
                  f"rtt={rtt_ms:.1f}ms error={err!r}", flush=True)

    def _close_socket(self):
        self._connected.clear()
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None


# =====================================================================
# INPUT HELPERS
# =====================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="REVOPILOT gamepad teleop (UDP motion + TCP event channel)."
    )
    parser.add_argument("--robot", dest="robot",
                        choices=sorted(ROBOT_IPS.keys()), type=str.upper,
                        default=DEFAULT_ROBOT,
                        help=f"Target robot (default: {DEFAULT_ROBOT}).")
    parser.add_argument("--robot_lock", default="true",
                        help="Initial robot_lock state (default: true).")
    return parser.parse_args()


def read_button(js, idx):
    if idx is None or idx < 0:
        return 0
    return js.get_button(idx) if js.get_numbuttons() > idx else 0


def read_axis(js, idx):
    if js.get_numaxes() <= idx:
        return 0.0
    try:
        value = js.get_axis(idx)
    except pygame.error:
        return 0.0
    if abs(value) > 1.5:
        value = value / 32767.0
    return clamp(value, -1.0, 1.0)


def read_axis_counts(js, idx):
    return int(round(read_axis(js, idx) * 32767.0))


def wait_for_joystick(retry_sec=JOYSTICK_RETRY_SEC):
    while True:
        pygame.joystick.quit()
        pygame.joystick.init()
        if pygame.joystick.get_count() > 0:
            try:
                js = pygame.joystick.Joystick(0)
                js.init()
            except pygame.error as e:
                print(f"⚠️  Joystick init failed: {e}. Retry in {retry_sec:.1f}s")
                time.sleep(retry_sec)
                continue
            print("======================================")
            print(" JOYSTICK DETECTED")
            print("======================================")
            print("Name    :", js.get_name())
            print("Axes    :", js.get_numaxes())
            print("Buttons :", js.get_numbuttons())
            print("Hats    :", js.get_numhats())
            print("======================================")
            return js
        print(f"⏳ No joystick found. Retry in {retry_sec:.1f}s...")
        time.sleep(retry_sec)


def get_gamepad_mapping(js):
    name = (js.get_name() or "").strip().lower()
    num_buttons = js.get_numbuttons()
    current_os = platform.system()

    if "ultimate 2 wireless" in name and current_os == "Linux":
        mapping_key = "8bitdo_ultimate2_wireless"
    elif "ultimate wireless controller for pc" in name and current_os == "Linux":
        mapping_key = "8bitdo_ultimate_wireless_pc"
    elif "ultimate 2 wireless" in name and current_os == "Windows":
        mapping_key = "8bitdo_ultimate2_wireless_windows"
    elif num_buttons <= 12:
        mapping_key = "8bitdo_ultimate2_wireless"
    else:
        mapping_key = DEFAULT_MAPPING_KEY

    mapping = GAMEPAD_MAPPINGS[mapping_key]
    print(f"[MAPPING] '{mapping_key}' for '{js.get_name()}'")
    return mapping


# =====================================================================
# MAIN
# =====================================================================

def main():
    args = parse_args()

    robot_name = args.robot
    robot_ip = ROBOT_IPS[robot_name]
    robot_lock = args.robot_lock.lower() == "true"

    print("\n======================================")
    print(" REVOPILOT CONTROL TARGET")
    print("======================================")
    print(f" ROBOT           : {robot_name}")
    print(f" ROBOT_IP        : {robot_ip}")
    print(f" MOTION_UDP_PORT : {MOTION_UDP_PORT}")
    print(f" EVENT_TCP_PORT  : {EVENT_TCP_PORT}")
    print(f" ROBOT_LOCK      : {robot_lock}")
    print("======================================\n")

    pygame.init()
    pygame.joystick.init()
    js = wait_for_joystick()
    gamepad = get_gamepad_mapping(js)

    # UDP motion socket
    motion_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    motion_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # TCP event channel
    events = EventChannelClient(robot_ip, EVENT_TCP_PORT)
    events.start()

    # State
    seq = 0
    period = 1.0 / SEND_HZ
    next_t = time.time()

    cruise_zero_idx = CRUISE_LEVELS.index(0.0)
    cruise_level_idx = cruise_zero_idx
    cruise_speed = CRUISE_LEVELS[cruise_level_idx]

    max_speed = MAX_SPEED_INITIAL
    speed_level = 1  # 1=slow, 2=medium, 3=fast

    lock_seq = []
    sequence_active = False
    prev_a = prev_b = prev_y = prev_x = 0
    prev_cruise_up = prev_cruise_down = 0
    prev_lights_on = prev_lights_off = 0
    lights_state = "UNKNOWN"
    axis3_state = "center"
    axis4_state = "center"
    axis4_neg_taps = []
    axis4_pending_speech_deadline = None

    prev_ai_chord = False
    ai_request_counter = 0

    try:
        while True:
            now = time.time()
            if now < next_t:
                time.sleep(next_t - now)
            next_t += period

            if pygame.joystick.get_count() == 0:
                print("\n⚠️  Joystick disconnected. Waiting...")
                js = wait_for_joystick()
                gamepad = get_gamepad_mapping(js)
                next_t = time.time()
                continue

            try:
                pygame.event.pump()
            except pygame.error:
                print("\n⚠️  Joystick event error. Waiting...")
                js = wait_for_joystick()
                gamepad = get_gamepad_mapping(js)
                next_t = time.time()
                continue

            head = "center"

            raw_steer = -read_axis(js, gamepad["axis_steer"])
            pedal_axis = 0.0  # pedal contribution disabled (kept for parity)
            head_lr = read_axis(js, gamepad["axis_head_lr"])
            head_ud = read_axis(js, gamepad["axis_head_ud"])
            signal_axis = read_axis_counts(js, gamepad["axis_signal"])
            sound_axis = read_axis_counts(js, gamepad["axis_sound"])

            if abs(head_lr) < 0.01 and abs(head_ud) < 0.01 and js.get_numhats() > 0:
                hx, hy = js.get_hat(0)
                head_lr = float(hx)
                head_ud = float(-hy)

            # -------- Axis 3: indicator (TCP event) -----------------
            if signal_axis < -AXIS_ACTION_THRESHOLD_COUNTS:
                new_axis3_state = "left"
            elif signal_axis > AXIS_ACTION_THRESHOLD_COUNTS:
                new_axis3_state = "right"
            else:
                new_axis3_state = "center"

            if new_axis3_state != axis3_state:
                events.send("indicator", {"side": new_axis3_state})
                axis3_state = new_axis3_state

            # -------- Axis 4: audio / talk / music (TCP event) ------
            if sound_axis < -AXIS_ACTION_THRESHOLD_COUNTS:
                new_axis4_state = "neg"
            elif sound_axis > AXIS_ACTION_THRESHOLD_COUNTS:
                new_axis4_state = "pos"
            else:
                new_axis4_state = "center"

            axis4_now = time.time()
            # Deferred single-tap neg -> speech after tap window closes
            if (axis4_pending_speech_deadline is not None
                    and axis4_now >= axis4_pending_speech_deadline):
                events.send("audio", {"volume_pct": AUDIO_FULL_VOLUME_PCT})
                events.send("talk", {"text": AXIS4_NEG_MESSAGE,
                                     "duration": TALK_DURATION_SEC})
                axis4_pending_speech_deadline = None
                axis4_neg_taps = []

            if new_axis4_state != axis4_state:
                if new_axis4_state == "neg":
                    axis4_neg_taps = [t for t in axis4_neg_taps
                                      if axis4_now - t <= AXIS4_MULTI_TAP_WINDOW_SEC]
                    axis4_neg_taps.append(axis4_now)
                    if len(axis4_neg_taps) >= 2:
                        # Double-tap: music + longer talk
                        events.send("music", {"action": "play",
                                              "track": MUSIC_TRACK_ID})
                        events.send("talk", {"text": "",
                                             "duration": MUSIC_TALK_DURATION_SEC})
                        axis4_pending_speech_deadline = None
                        axis4_neg_taps = []
                    else:
                        axis4_pending_speech_deadline = (
                            axis4_now + AXIS4_MULTI_TAP_WINDOW_SEC
                        )
                elif new_axis4_state == "pos":
                    events.send("audio", {"volume_pct": AUDIO_FULL_VOLUME_PCT})
                    events.send("talk", {"text": AXIS4_POS_MESSAGE,
                                         "duration": TALK_DURATION_SEC})
                axis4_state = new_axis4_state

            # -------- Pedal / brake (pedal contribution disabled) ---
            pedal_signed = clamp(-pedal_axis, -1.0, 1.0)
            brake = 0.0

            # -------- Buttons ---------------------------------------
            a_pressed = read_button(js, gamepad["btn_a"])
            b_pressed = read_button(js, gamepad["btn_b"])
            y_pressed = read_button(js, gamepad["btn_y"])
            x_pressed = read_button(js, gamepad["btn_x"])
            cruise_up = read_button(js, gamepad["btn_cruise_up"])
            cruise_down = read_button(js, gamepad["btn_cruise_down"])
            lights_on_pressed = read_button(js, gamepad["btn_lights_on"])
            lights_off_pressed = read_button(js, gamepad["btn_lights_off"])

            lift_pos_axis = read_axis(js, gamepad["axis_lift_pos"])
            lift_neg_axis = read_axis(js, gamepad["axis_lift_neg"])
            lift_pos_cmd = lift_axis_to_cmd(lift_pos_axis)
            lift_neg_cmd = lift_axis_to_cmd(lift_neg_axis)
            if lift_pos_cmd > lift_neg_cmd:
                lift = lift_pos_cmd
            elif lift_neg_cmd > lift_pos_cmd:
                lift = -lift_neg_cmd
            else:
                lift = 0

            # -------- AI-enable chord -------------------------------
            press_pos = clamp((lift_pos_axis + 1.0) * 0.5, 0.0, 1.0)
            press_neg = clamp((lift_neg_axis + 1.0) * 0.5, 0.0, 1.0)
            ai_chord = (press_pos > AI_ENABLE_PRESS_THRESHOLD
                        and press_neg > AI_ENABLE_PRESS_THRESHOLD)
            if ai_chord and not prev_ai_chord:
                ai_request_counter = AI_REQUEST_REPEAT_PACKETS
                print(f"[AI] enable-AI chord -> ai_request x{AI_REQUEST_REPEAT_PACKETS}")
            prev_ai_chord = ai_chord

            if SWAP_XY_BUTTONS:
                y_pressed, x_pressed = x_pressed, y_pressed

            now_t = time.time()
            a_edge = a_pressed and not prev_a
            b_edge = b_pressed and not prev_b
            y_edge = y_pressed and not prev_y  # reserved for future use
            x_edge = x_pressed and not prev_x  # reserved for future use
            lights_on_edge = lights_on_pressed and not prev_lights_on
            lights_off_edge = lights_off_pressed and not prev_lights_off

            # -------- Lights on/off buttons (TCP event) -------------
            if lights_on_edge:
                print("[LIGHTS] Manual ON (button)  + PTZ goto_home")
                events.send("lights", {"headlights": True,
                                       "parklights": True,
                                       "strobe": True})
                # The lights-ON button doubles as the PTZ return-to-home trigger.
                # Robot side (teleop.on_event) routes {"type":"ptz","data":{"action":"goto_home"}}
                # to ptz.goto_home(). Same button, two effects — matches the
                # previous behavior where "button=8" in the motion packet did both.
                events.send("ptz", {"action": "goto_home"})
                lights_state = "ON"
            if lights_off_edge:
                print("[LIGHTS] Manual OFF (button)")
                events.send("lights", {"headlights": False,
                                       "parklights": False,
                                       "strobe": False})
                lights_state = "OFF"

            # -------- Lock sequence (A/B) ---------------------------
            if a_edge:
                lock_seq.append(("A", now_t))
            if b_edge:
                lock_seq.append(("B", now_t))

            prev_a, prev_b = a_pressed, b_pressed
            prev_y, prev_x = y_pressed, x_pressed
            prev_lights_on = lights_on_pressed
            prev_lights_off = lights_off_pressed

            lock_seq = [(k, t) for (k, t) in lock_seq
                        if now_t - t <= LOCK_SEQUENCE_TIMEOUT]

            if not sequence_active and len(lock_seq) >= 2:
                first2 = "".join([k for (k, _) in lock_seq[:2]])
                if first2 in ("AB", "BA"):
                    sequence_active = True

            if len(lock_seq) >= 2:
                last2 = lock_seq[-2:]
                seq_str = "".join([k for (k, _) in last2])
                span = last2[-1][1] - last2[0][1]
                if span <= LOCK_SEQUENCE_TIMEOUT:
                    if seq_str == "AB":
                        if robot_lock:
                            robot_lock = False
                            max_speed = 1.0
                            speed_level = 1
                            print("[LOCK] Robot unlocked  + PTZ capture_home")
                            # Robot just unlocked — capture the PTZ's current
                            # position as "home" so the lights-ON goto_home
                            # button has somewhere to return to.
                            events.send("ptz", {"action": "capture_home"})
                        else:
                            speed_level = 1 if speed_level >= 3 else speed_level + 1
                            max_speed = float(speed_level)
                            print(f"[LOCK] Speed level -> {speed_level}")
                        lock_seq = []
                        sequence_active = False
                    elif seq_str == "BA":
                        robot_lock = True
                        lock_seq = []
                        sequence_active = False
                        print("[LOCK] Robot locked")

            # -------- Cruise / lin_x computation --------------------
            both_cruise_pressed = bool(cruise_up and cruise_down)
            brake_active = (brake > BRAKE_THRESHOLD) or both_cruise_pressed
            if both_cruise_pressed:
                brake = 1.0

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
            cruise_speed = clamp(cruise_speed, -cruise_abs_max, cruise_abs_max)

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

            # -------- Head direction --------------------------------
            axis_head_threshold = 0.5
            if head_lr < -axis_head_threshold:
                head = "left"
            elif head_lr > axis_head_threshold:
                head = "right"
            elif head_ud < -axis_head_threshold:
                head = "up"
            elif head_ud > axis_head_threshold:
                head = "down"

            # -------- Steering -> ang_z -----------------------------
            s = apply_deadzone(raw_steer, STEER_DEADZONE)
            s = expo_curve(s, STEER_EXPO)
            s *= STEER_GAIN
            s = clamp(s, -1.0, 1.0)

            speed_frac = min(abs(lin_x) / max_speed, 1.0) if max_speed > 0 else 0.0
            yaw_limit = (MAX_YAW_INPLACE * (1.0 - speed_frac)
                         + MAX_YAW_MOVING * speed_frac)
            ang_z = clamp(s * yaw_limit, -yaw_limit, yaw_limit)
            if both_cruise_pressed:
                ang_z = 0.0

            # -------- Build UDP motion payload ----------------------
            speed_label = {1: "slow", 2: "medium", 3: "fast"}.get(speed_level, "slow")
            payload = {
                "seq": seq,
                "t": time.time(),
                "lin_x": round(lin_x, 4),
                "ang_z": round(ang_z, 4),
                "brake": round(brake, 3),
                "robot_lock": robot_lock,
                "head": head,
                "speed": speed_label,
                "lift": lift,
                "origin": "human",
                "ai_request": "enable" if ai_request_counter > 0 else None,
            }
            if ai_request_counter > 0:
                ai_request_counter -= 1
            seq += 1

            # -------- Send UDP motion -------------------------------
            try:
                msg = json.dumps(payload).encode("utf-8")
                motion_sock.sendto(msg, (robot_ip, MOTION_UDP_PORT))
            except OSError as e:
                print("[MOTION] send error:", e)

            # -------- Debug (single overwriting status line) --------
            lock_label = ("LOCKED" if robot_lock
                          else f"UNLOCKED-spd{speed_level}({speed_label})")
            with events.stats_lock:
                tcp_up = events.is_connected()
                ok = events.sent_ok
                err = events.sent_err
                dropped = events.dropped
                rtt = events.last_rtt_ms
                reconnects = events.reconnects
                last_err = events.last_error
            qsize = events.qsize()
            if tcp_up:
                tcp_label = (f"UP  ok={ok} err={err} drop={dropped} "
                             f"q={qsize} rtt={rtt:.0f}ms rc={reconnects}")
            else:
                tcp_label = (f"DOWN err={err} drop={dropped} q={qsize} "
                             f"rc={reconnects} last={last_err[:30]!r}")
            status = (
                f"[STATUS] seq={seq:>6} "
                f"lin_x={payload['lin_x']:+.3f} ang_z={payload['ang_z']:+.3f} "
                f"brake={payload['brake']:.2f} head={head:<6} "
                f"lift={lift:+4d} "
                f"origin={payload['origin']} ai_req={payload['ai_request'] or '-'} "
                f"lock={lock_label} lights={lights_state} | tcp={tcp_label}"
            )
            # \r returns cursor to start of line; \033[K clears to end-of-line
            # so shorter lines don't leave stale characters behind.
            print(f"\r\033[K{status}", end="", flush=True)
            if PRINT_LIFT_DEBUG:
                print(f"\n[LIFT] pos={lift_pos_axis:+.3f} neg={lift_neg_axis:+.3f} "
                      f"lift={lift}", flush=True)
    finally:
        events.stop()
        motion_sock.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n🛑 Exiting cleanly.")