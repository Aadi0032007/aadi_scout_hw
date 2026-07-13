#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations

"""
LAB/pilot/pilot_bridge.py — local gamepad → WebSocket relay for the browser.

Runs on the OPERATOR'S PC, not the robot. Reads a USB gamepad and streams
normalized W3C Xbox-layout state to the RevoPilot browser dashboard on
ws://127.0.0.1:8765. The browser is the one that talks to the robot
(UDP :55999 for motion, WSS control channel for events).

This is a fork of revo_bridge_server.py, dropped into LAB/pilot/ so it
lives alongside teleop and can be maintained here. Protocol and default
port match the upstream exactly so it's a drop-in replacement.

Setup (once):
    pip3 install --user pygame websockets

List gamepads:
    python3 -m LAB.pilot.pilot_bridge --list-devices

Run:
    python3 -m LAB.pilot.pilot_bridge

Custom port:
    python3 -m LAB.pilot.pilot_bridge --port 8765

Wire protocol (unchanged from upstream):

  Browser → bridge (session updates, logged; not forwarded):
      {"type": "session", "robot_id": "...", "tailscale_ip": "...", "lock": 0}

  Bridge → browser:
      {"type": "gamepad", "connected": true|false, "id": "...", "name": "..."}
      {"type": "state", "ts": <float>,
       "buttons": [{"pressed": bool, "value": float}, ...] × 17,
       "axes": [lx, ly, rx, ry]}

Button layout (browser GP_BTN indices):
    0-3   A, B, X, Y
    4-5   LB, RB
    6-7   LT, RT (analog "value")
    8-11  Back, Start, L3, R3
    12-15 D-pad up, down, left, right
    16    Guide / Home
Axes 0-3: left stick X/Y, right stick X/Y.

UDP teleop half:
    When session.lock=0 and session.tailscale_ip is set and gamepad is
    connected and a browser client is connected: bridge sends the standard
    trimmed motion payload to udp://<tailscale_ip>:55999 at 50 Hz. On
    gamepad or browser loss, sends STOP_PACKET_COUNT stop packets then
    halts UDP until re-armed.

    Note: newer browser builds may drive robots directly and not want the
    bridge sending UDP too. If your browser does its own UDP, disable this
    half with --no-udp.
"""

import argparse
import asyncio
import json
import logging
import socket
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import pygame
import websockets

# websockets 12 exposes ServerConnection at package root; 13+ moved it under
# websockets.asyncio.server. Try both so the same file runs on either.
try:
    from websockets.asyncio.server import ServerConnection  # 13+
except ImportError:
    try:
        from websockets import ServerConnection  # 12
    except ImportError:
        # Very old websockets (< 12) — fall back to a permissive type.
        ServerConnection = Any  # type: ignore

LOG = logging.getLogger("pilot_bridge")

# Silence "opening handshake failed" tracebacks. These fire when something
# (Windows Defender, port scanners, browser TCP preflight) opens a socket to
# our port and closes it without sending an HTTP request. The websockets
# library correctly logs it at ERROR but it's not our problem — real client
# connects still show up at INFO as "Client connected from ...".
logging.getLogger("websockets.server").setLevel(logging.CRITICAL)

# ── Tunables (match upstream revo_bridge_server.py) ─────────────────────────
HOST = "127.0.0.1"
DEFAULT_PORT = 8765
UDP_PORT = 55999
STOP_PACKET_COUNT = 10

POLL_HZ = 60.0
POLL_INTERVAL = 1.0 / POLL_HZ
UDP_HZ = 50.0
UDP_INTERVAL = 1.0 / UDP_HZ
AXIS_EPS = 0.01
TRIGGER_EPS = 0.02
STEER_DEADZONE = 0.1

# W3C standard indices used for UDP teleop logic.
GP_BTN_LB = 4
GP_BTN_RB = 5
GP_DPAD_UP = 12
GP_DPAD_DOWN = 13
GP_DPAD_LEFT = 14
GP_DPAD_RIGHT = 15
GP_AXIS_LEFT_X = 0

LIN_X_LEVELS = [
    -1.0, -0.8, -0.6, -0.4, -0.2, -0.1, -0.05, 0.0,
    0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 1.0,
]
LIN_X_CENTER_IDX = LIN_X_LEVELS.index(0.0)

UDP_LOG = logging.getLogger("pilot_bridge.udp")
WS_LOG = logging.getLogger("pilot_bridge.ws")

NUM_BUTTONS = 17
NUM_AXES = 4

# SDL2 game-controller button indices → browser GP_BTN indices.
SDL_BTN_TO_GP: dict[int, int] = {
    0: 0,   # A
    1: 1,   # B
    2: 2,   # X
    3: 3,   # Y
    4: 8,   # Back
    5: 16,  # Guide
    6: 9,   # Start
    7: 10,  # L3
    8: 11,  # R3
    9: 4,   # LB
    10: 5,  # RB
    11: 12,  # D-pad up
    12: 13,  # D-pad down
    13: 14,  # D-pad left
    14: 15,  # D-pad right
}


def empty_buttons() -> list[dict[str, float | bool]]:
    return [{"pressed": False, "value": 0.0} for _ in range(NUM_BUTTONS)]


def empty_axes() -> list[float]:
    return [0.0] * NUM_AXES


@dataclass
class GamepadState:
    buttons: list[dict[str, float | bool]] = field(default_factory=empty_buttons)
    axes: list[float] = field(default_factory=empty_axes)

    def copy(self) -> GamepadState:
        return GamepadState(
            buttons=[dict(b) for b in self.buttons],
            axes=list(self.axes),
        )

    def equals(self, other: GamepadState) -> bool:
        for a, b in zip(self.axes, other.axes):
            if abs(a - b) > AXIS_EPS:
                return False
        for a, b in zip(self.buttons, other.buttons):
            if bool(a["pressed"]) != bool(b["pressed"]):
                return False
            if abs(float(a["value"]) - float(b["value"])) > TRIGGER_EPS:
                return False
        return True


@dataclass
class SessionInfo:
    robot_id: str = ""
    tailscale_ip: str = ""
    lock: int = 0


def _clamp(value: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def _norm_axis(raw: float | int) -> float:
    v = float(raw)
    if abs(v) <= 1.0:
        return _clamp(v)
    if v >= 0:
        return _clamp(v / 32767.0)
    return _clamp(v / 32768.0)


def _norm_trigger(raw: float | int) -> float:
    v = float(raw)
    if abs(v) <= 1.0:
        if v < 0:
            return _clamp((v + 1.0) / 2.0, 0.0, 1.0)
        return _clamp(v, 0.0, 1.0)
    if v <= 0:
        return _clamp((v + 32768.0) / 32768.0, 0.0, 1.0)
    return _clamp(v / 32767.0, 0.0, 1.0)


def _set_button(buttons: list[dict[str, float | bool]], index: int, value: float) -> None:
    if index < 0 or index >= NUM_BUTTONS:
        return
    pressed = value > 0.5
    buttons[index] = {"pressed": pressed, "value": float(value)}


def _apply_hat(buttons: list[dict[str, float | bool]], hat: tuple[int, int]) -> None:
    x, y = hat
    _set_button(buttons, 12, 1.0 if y > 0 else 0.0)
    _set_button(buttons, 13, 1.0 if y < 0 else 0.0)
    _set_button(buttons, 14, 1.0 if x < 0 else 0.0)
    _set_button(buttons, 15, 1.0 if x > 0 else 0.0)


def _format_ws_state(payload: dict[str, Any]) -> str:
    axes = payload.get("axes", [])
    pressed = [
        i
        for i, b in enumerate(payload.get("buttons", []))
        if b.get("pressed") or float(b.get("value", 0)) > 0.5
    ]
    return (
        f"state axes={[round(float(a), 2) for a in axes]} "
        f"pressed={pressed}"
    )


class GamepadReader:
    """Poll physical gamepad and emit normalized Gamepad API state."""

    def __init__(self) -> None:
        self._sdl_controller_mod: Any = None
        self._controller: Any = None
        self._joystick: Optional[pygame.joystick.Joystick] = None
        self._use_sdl_controller = False
        self._device_id = ""
        self._device_name = ""
        self._instance_id: Optional[int] = None
        self._connected = False
        self._last_state = GamepadState()
        self._init_pygame()

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def device_id(self) -> str:
        return self._device_id

    @property
    def device_name(self) -> str:
        return self._device_name

    @property
    def last_state(self) -> GamepadState:
        return self._last_state

    def _init_pygame(self) -> None:
        pygame.init()
        pygame.joystick.init()
        try:
            from pygame._sdl2 import controller as sdl_controller

            sdl_controller.init()
            self._sdl_controller_mod = sdl_controller
            try:
                sdl_controller.set_eventstate(True)
            except (AttributeError, pygame.error):
                pass
        except (ImportError, AttributeError):
            self._sdl_controller_mod = None

    def _open_first_device(self) -> bool:
        self._close_device()

        if self._sdl_controller_mod is not None:
            count = self._sdl_controller_mod.get_count()
            for idx in range(count):
                if self._sdl_controller_mod.is_controller(idx):
                    try:
                        ctrl = self._sdl_controller_mod.Controller(idx)
                        ctrl.init()
                    except pygame.error as exc:
                        LOG.debug("SDL controller %d init failed: %s", idx, exc)
                        continue
                    self._controller = ctrl
                    self._use_sdl_controller = True
                    self._device_id = f"sdl-{idx}"
                    self._device_name = ctrl.name or f"Controller {idx}"
                    self._instance_id = int(ctrl.id)
                    self._connected = True
                    LOG.info("Opened SDL game controller: %s", self._device_name)
                    return True

        count = pygame.joystick.get_count()
        for idx in range(count):
            try:
                js = pygame.joystick.Joystick(idx)
                js.init()
            except pygame.error as exc:
                LOG.debug("Joystick %d init failed: %s", idx, exc)
                continue
            self._joystick = js
            self._use_sdl_controller = False
            self._device_id = f"joy-{idx}"
            self._device_name = js.get_name() or f"Joystick {idx}"
            self._instance_id = js.get_instance_id()
            self._connected = True
            LOG.info(
                "Opened joystick (generic mapping): %s (axes=%d buttons=%d hats=%d)",
                self._device_name,
                js.get_numaxes(),
                js.get_numbuttons(),
                js.get_numhats(),
            )
            return True

        return False

    def _close_device(self) -> None:
        if self._controller is not None:
            try:
                self._controller.quit()
            except pygame.error:
                pass
            self._controller = None
        if self._joystick is not None:
            try:
                self._joystick.quit()
            except pygame.error:
                pass
            self._joystick = None
        self._use_sdl_controller = False
        self._connected = False
        self._device_id = ""
        self._device_name = ""
        self._instance_id = None
        self._last_state = GamepadState()

    def _controller_attached(self) -> bool:
        if self._controller is None:
            return False
        attached_fn = getattr(self._controller, "attached", None)
        if attached_fn is None:
            return True
        if callable(attached_fn):
            return bool(attached_fn())
        return bool(attached_fn)

    def _sdl_has_game_controller(self) -> bool:
        if self._sdl_controller_mod is None:
            return pygame.joystick.get_count() > 0
        return any(
            self._sdl_controller_mod.is_controller(i)
            for i in range(self._sdl_controller_mod.get_count())
        )

    def _disconnect_event_seen(self) -> bool:
        for event in pygame.event.get():
            if event.type not in (pygame.JOYDEVICEREMOVED, pygame.CONTROLLERDEVICEREMOVED):
                continue
            inst = getattr(event, "instance_id", None)
            if self._instance_id is None or inst == self._instance_id:
                LOG.info("Gamepad removed (SDL event instance_id=%s)", inst)
                return True
        return False

    def _is_sdl_controller_alive(self) -> bool:
        if self._controller is None:
            return False
        if self._disconnect_event_seen():
            return False
        if not self._controller_attached():
            LOG.info("Gamepad detached (attached() returned False)")
            return False
        if pygame.joystick.get_count() == 0:
            LOG.info("Gamepad detached (joystick count=0)")
            return False
        if not self._sdl_has_game_controller():
            LOG.info("Gamepad detached (no SDL game controllers present)")
            return False
        try:
            self._controller.get_axis(0)
            return True
        except pygame.error:
            LOG.info("Gamepad detached (axis read failed)")
            return False

    def _disconnect_gamepad_event(self) -> dict[str, Any]:
        old_id, old_name = self._device_id, self._device_name
        self._close_device()
        return {
            "type": "gamepad",
            "connected": False,
            "id": old_id,
            "name": old_name,
        }

    def _read_sdl_controller(self) -> GamepadState:
        assert self._controller is not None
        buttons = empty_buttons()
        axes = empty_axes()

        for sdl_idx, gp_idx in SDL_BTN_TO_GP.items():
            pressed = bool(self._controller.get_button(sdl_idx))
            _set_button(buttons, gp_idx, 1.0 if pressed else 0.0)

        _set_button(buttons, 6, _norm_trigger(self._controller.get_axis(4)))
        _set_button(buttons, 7, _norm_trigger(self._controller.get_axis(5)))

        for axis_idx in range(4):
            axes[axis_idx] = _norm_axis(self._controller.get_axis(axis_idx))

        return GamepadState(buttons=buttons, axes=axes)

    def _read_joystick_xinput(self, js: pygame.joystick.Joystick) -> GamepadState:
        buttons = empty_buttons()
        axes = empty_axes()
        name = (js.get_name() or "").lower()

        for gp_idx in range(min(js.get_numbuttons(), 4)):
            val = 1.0 if js.get_button(gp_idx) else 0.0
            _set_button(buttons, gp_idx, val)

        if js.get_numbuttons() >= 11:
            mapping = {4: 4, 5: 5, 6: 8, 7: 9, 8: 10, 9: 11, 10: 16}
            for src, dst in mapping.items():
                if src < js.get_numbuttons():
                    val = 1.0 if js.get_button(src) else 0.0
                    _set_button(buttons, dst, val)

        if js.get_numaxes() >= 6:
            _set_button(buttons, 6, _norm_trigger(js.get_axis(2)))
            _set_button(buttons, 7, _norm_trigger(js.get_axis(5)))
        elif js.get_numbuttons() >= 8:
            _set_button(buttons, 6, 1.0 if js.get_button(6) else 0.0)
            _set_button(buttons, 7, 1.0 if js.get_button(7) else 0.0)

        if js.get_numhats() > 0:
            _apply_hat(buttons, js.get_hat(0))
        elif js.get_numbuttons() >= 16:
            for src, dst in ((11, 12), (12, 13), (13, 14), (14, 15)):
                val = 1.0 if js.get_button(src) else 0.0
                _set_button(buttons, dst, val)

        if js.get_numaxes() >= 2:
            axes[0] = _clamp(js.get_axis(0))
            axes[1] = _clamp(js.get_axis(1))
        if js.get_numaxes() >= 4:
            axes[2] = _clamp(js.get_axis(3) if "8bitdo" in name else js.get_axis(2))
            axes[3] = _clamp(js.get_axis(4) if "8bitdo" in name else js.get_axis(3))

        return GamepadState(buttons=buttons, axes=axes)

    def poll(self) -> tuple[Optional[dict[str, Any]], Optional[dict[str, Any]]]:
        pygame.event.pump()

        gamepad_event: Optional[dict[str, Any]] = None
        state_event: Optional[dict[str, Any]] = None

        if not self._connected:
            if self._open_first_device():
                gamepad_event = {
                    "type": "gamepad",
                    "connected": True,
                    "id": self._device_id,
                    "name": self._device_name,
                }
                self._last_state = self._read_current()
                state_event = self._state_message(self._last_state)
            else:
                return None, None
        else:
            if self._use_sdl_controller:
                if self._sdl_controller_mod is None or not self._is_sdl_controller_alive():
                    return self._disconnect_gamepad_event(), None
            elif self._joystick is None or pygame.joystick.get_count() == 0:
                return self._disconnect_gamepad_event(), None
            elif not self._joystick.get_attached():
                LOG.info("Gamepad detached (joystick.get_attached()=False)")
                return self._disconnect_gamepad_event(), None
            elif self._disconnect_event_seen():
                return self._disconnect_gamepad_event(), None

        current = self._read_current()
        if not self._last_state.equals(current):
            self._last_state = current
            state_event = self._state_message(current)

        return gamepad_event, state_event

    def _read_current(self) -> GamepadState:
        if self._use_sdl_controller and self._controller is not None:
            return self._read_sdl_controller()
        if self._joystick is not None:
            return self._read_joystick_xinput(self._joystick)
        return GamepadState()

    @staticmethod
    def _state_message(state: GamepadState) -> dict[str, Any]:
        return {
            "type": "state",
            "ts": time.time(),
            "buttons": state.buttons,
            "axes": state.axes,
        }


def _btn_pressed(state: GamepadState, index: int) -> bool:
    if index < 0 or index >= len(state.buttons):
        return False
    b = state.buttons[index]
    return bool(b["pressed"]) or float(b["value"]) > 0.5


class UdpTeleopController:
    """Build 50 Hz UDP payloads from normalized W3C gamepad state."""

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.lin_x_idx = LIN_X_CENTER_IDX
        self.brake = 0
        self.head = "center"
        self._prev_lb = False
        self._prev_rb = False
        self._last_left_x = 0.0

    @property
    def lin_x(self) -> float:
        return LIN_X_LEVELS[self.lin_x_idx]

    def update(self, state: GamepadState) -> None:
        lb = _btn_pressed(state, GP_BTN_LB)
        rb = _btn_pressed(state, GP_BTN_RB)
        left_x = _clamp(state.axes[GP_AXIS_LEFT_X])
        self._last_left_x = left_x

        if self.brake:
            if lb or rb or abs(left_x) > STEER_DEADZONE:
                self.brake = 0

        if lb and rb:
            self.lin_x_idx = LIN_X_CENTER_IDX
            self.brake = 1
            self._prev_lb = lb
            self._prev_rb = rb
        else:
            if rb and not self._prev_rb and self.lin_x_idx < len(LIN_X_LEVELS) - 1:
                self.lin_x_idx += 1
            if lb and not self._prev_lb and self.lin_x_idx > 0:
                self.lin_x_idx -= 1
            self._prev_lb = lb
            self._prev_rb = rb

        if _btn_pressed(state, GP_DPAD_LEFT):
            self.head = "left"
        elif _btn_pressed(state, GP_DPAD_RIGHT):
            self.head = "right"
        elif _btn_pressed(state, GP_DPAD_UP):
            self.head = "up"
        elif _btn_pressed(state, GP_DPAD_DOWN):
            self.head = "down"
        else:
            self.head = "center"

    def build_packet(self, seq: int) -> dict[str, Any]:
        lin_x = 0.0 if self.brake else self.lin_x
        ang_z = 0.0 if self.brake else -self._last_left_x
        return {
            "seq": seq,
            "t": int(time.time()),
            "brain": "HI",
            "lin_x": lin_x,
            "ang_z": ang_z,
            "brake": int(self.brake),
            "head": self.head,
        }

    @staticmethod
    def build_stop_packet(seq: int) -> dict[str, Any]:
        return {
            "seq": seq,
            "t": int(time.time()),
            "brain": "HI",
            "lin_x": 0.0,
            "ang_z": 0.0,
            "brake": 1,
            "head": "center",
        }


class BridgeServer:
    def __init__(self, host: str, port: int, udp_port: int = UDP_PORT,
                 udp_enabled: bool = True) -> None:
        self.host = host
        self.port = port
        self.udp_port = udp_port
        self.udp_enabled = udp_enabled
        self.clients: set[ServerConnection] = set()
        self.session = SessionInfo()
        self._prev_lock: Optional[int] = None
        self._prev_tailscale_ip = ""
        self.reader = GamepadReader()
        self.teleop = UdpTeleopController()
        self.udp_seq = 1
        self._udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._last_udp_log_at = 0.0
        self._last_udp_payload: Optional[dict[str, Any]] = None
        self._udp_halted = False

    @staticmethod
    def _stop_udp_packet(seq: int) -> dict[str, Any]:
        return UdpTeleopController.build_stop_packet(seq)

    def _halt_udp_with_stop_burst(self, reason: str) -> None:
        self._udp_halted = True
        self.teleop.reset()
        if self.session.lock != 0 or not self.session.tailscale_ip:
            LOG.info("UDP halted (%s)", reason)
            return
        for _ in range(STOP_PACKET_COUNT):
            payload = self._stop_udp_packet(self.udp_seq)
            self._send_udp(payload)
            self.udp_seq += 1
        self._last_udp_log_at = 0.0
        self._maybe_log_udp()
        LOG.info(
            "Sent %d stop UDP packets to %s:%d (%s); UDP halted",
            STOP_PACKET_COUNT,
            self.session.tailscale_ip,
            self.udp_port,
            reason,
        )

    def _on_gamepad_lost(self) -> None:
        LOG.warning("Gamepad connection lost")
        self._halt_udp_with_stop_burst("gamepad lost")

    def _on_browser_lost(self) -> None:
        LOG.warning("Browser disconnected")
        self._halt_udp_with_stop_burst("browser closed")

    def _on_gamepad_reconnected(self) -> None:
        LOG.info("Gamepad reconnected — resuming UDP teleop when lock=0")
        self._udp_halted = False
        self.teleop.reset()
        if self.session.lock == 0:
            self.udp_seq = 1

    def _on_session_update(self, session: SessionInfo) -> None:
        unlocked = session.lock == 0
        was_locked = self._prev_lock != 0
        ip_changed = session.tailscale_ip != self._prev_tailscale_ip
        if unlocked and (was_locked or ip_changed or self._prev_lock is None):
            self.udp_seq = 1
            self.teleop.reset()
            if self.reader.connected and self.clients:
                self._udp_halted = False
            if session.tailscale_ip:
                LOG.info(
                    "UDP teleop armed: %s:%d (lock=0)",
                    session.tailscale_ip,
                    self.udp_port,
                )
        elif session.lock == 1 and self._prev_lock == 0:
            LOG.info("UDP teleop stopped (lock=1)")
            self._udp_halted = False
        self._prev_lock = session.lock
        self._prev_tailscale_ip = session.tailscale_ip

    async def handler(self, websocket: ServerConnection) -> None:
        self.clients.add(websocket)
        peer = websocket.remote_address
        LOG.info("Client connected from %s", peer)

        await self._send_gamepad_info(websocket)
        if self.reader.connected:
            initial = self.reader._state_message(self.reader.last_state)
            await websocket.send(json.dumps(initial))
            WS_LOG.info("→ browser: %s", _format_ws_state(initial))

        try:
            async for raw in websocket:
                await self._handle_message(raw)
        except websockets.ConnectionClosed:
            pass
        finally:
            self.clients.discard(websocket)
            LOG.info("Client disconnected from %s", peer)
            if not self.clients:
                self._on_browser_lost()

    async def _handle_message(self, raw: str | bytes) -> None:
        text = raw.decode() if isinstance(raw, bytes) else raw
        WS_LOG.info("← browser: %s", text)
        try:
            msg = json.loads(text)
        except json.JSONDecodeError:
            LOG.warning("Ignoring non-JSON message: %r", text)
            return

        if msg.get("type") != "session":
            LOG.debug("Ignoring message type %r", msg.get("type"))
            return

        session = SessionInfo(
            robot_id=str(msg.get("robot_id", "")),
            tailscale_ip=str(msg.get("tailscale_ip", "")),
            lock=int(msg.get("lock", 0)),
        )
        self._on_session_update(session)
        self.session = session
        LOG.info(
            "Session update: robot_id=%s tailscale_ip=%s lock=%d",
            self.session.robot_id,
            self.session.tailscale_ip,
            self.session.lock,
        )

    async def _send_gamepad_info(self, websocket: ServerConnection) -> None:
        payload = {
            "type": "gamepad",
            "connected": self.reader.connected,
            "id": self.reader.device_id,
            "name": self.reader.device_name,
        }
        message = json.dumps(payload)
        await websocket.send(message)
        WS_LOG.info("→ browser: %s", message)

    async def _broadcast(self, payload: dict[str, Any]) -> None:
        if not self.clients:
            return
        message = json.dumps(payload)
        msg_type = payload.get("type")
        if msg_type == "gamepad":
            WS_LOG.info("→ browser: %s", message)
        elif msg_type == "state":
            WS_LOG.info("→ browser: %s", _format_ws_state(payload))
        await asyncio.gather(
            *[client.send(message) for client in list(self.clients)],
            return_exceptions=True,
        )

    def _send_udp(self, payload: dict[str, Any]) -> None:
        dest = self.session.tailscale_ip
        if not dest:
            return
        data = json.dumps(payload).encode("utf-8")
        self._udp_sock.sendto(data, (dest, self.udp_port))
        self._last_udp_payload = payload

    def _maybe_log_udp(self) -> None:
        now = time.time()
        if now - self._last_udp_log_at < 1.0:
            return
        self._last_udp_log_at = now
        if self._last_udp_payload is None:
            return
        dest = self.session.tailscale_ip
        UDP_LOG.info(
            "UDP → %s:%d %s",
            dest,
            self.udp_port,
            json.dumps(self._last_udp_payload),
        )

    async def poll_loop(self) -> None:
        while True:
            gamepad_event, state_event = self.reader.poll()
            if gamepad_event is not None:
                if gamepad_event.get("connected") is False:
                    self._on_gamepad_lost()
                    cleared = {
                        "type": "state",
                        "ts": time.time(),
                        "buttons": empty_buttons(),
                        "axes": empty_axes(),
                    }
                    await self._broadcast(cleared)
                elif gamepad_event.get("connected") is True:
                    self._on_gamepad_reconnected()
                await self._broadcast(gamepad_event)
            if state_event is not None:
                await self._broadcast(state_event)
            await asyncio.sleep(POLL_INTERVAL)

    async def udp_loop(self) -> None:
        if not self.udp_enabled:
            LOG.info("UDP teleop half disabled (--no-udp)")
            return
        while True:
            if (
                self.session.lock == 0
                and self.session.tailscale_ip
                and self.reader.connected
                and self.clients
                and not self._udp_halted
            ):
                self.teleop.update(self.reader.last_state)
                payload = self.teleop.build_packet(self.udp_seq)
                self._send_udp(payload)
                self.udp_seq += 1
                self._maybe_log_udp()
            await asyncio.sleep(UDP_INTERVAL)

    async def run(self) -> None:
        # compression=None disables permessage-deflate. Some browser + proxy
        # combinations negotiate it and then fail on the first frame; upstream
        # revo_bridge_server.py disables it for the same reason.
        async with websockets.serve(
            self.handler, self.host, self.port,
            compression=None,
        ):
            LOG.info("pilot_bridge listening on ws://%s:%d", self.host, self.port)
            if self.udp_enabled:
                LOG.info("UDP teleop target port %d (active when lock=0)", self.udp_port)
            else:
                LOG.info("UDP teleop half disabled — browser will drive robot directly")
            await asyncio.gather(self.poll_loop(), self.udp_loop())


def list_devices() -> None:
    pygame.init()
    pygame.joystick.init()

    print("Pygame joysticks:")
    if pygame.joystick.get_count() == 0:
        print("  (none)")
    for idx in range(pygame.joystick.get_count()):
        js = pygame.joystick.Joystick(idx)
        js.init()
        print(
            f"  [{idx}] {js.get_name()} "
            f"(axes={js.get_numaxes()}, buttons={js.get_numbuttons()}, hats={js.get_numhats()})"
        )
        js.quit()

    try:
        from pygame._sdl2 import controller as sdl_controller

        sdl_controller.init()
        print("\nSDL2 game controllers:")
        if sdl_controller.get_count() == 0:
            print("  (none)")
        for idx in range(sdl_controller.get_count()):
            label = "game controller" if sdl_controller.is_controller(idx) else "joystick only"
            name = sdl_controller.name_forindex(idx) or f"device-{idx}"
            print(f"  [{idx}] {name} ({label})")
    except (ImportError, AttributeError):
        print("\nSDL2 game controller API not available in this pygame build.")

    if sys.platform.startswith("linux"):
        try:
            import evdev
            from evdev import ecodes

            print("\nevdev input devices (gamepad-like):")
            found = False
            for path in evdev.list_devices():
                try:
                    dev = evdev.InputDevice(path)
                except OSError:
                    continue
                caps = dev.capabilities()
                keys = caps.get(ecodes.EV_KEY, [])
                abs_axes = caps.get(ecodes.EV_ABS, [])
                gamepad_keys = {
                    ecodes.BTN_A,
                    ecodes.BTN_B,
                    ecodes.BTN_X,
                    ecodes.BTN_Y,
                    ecodes.BTN_TL,
                    ecodes.BTN_TR,
                }
                if gamepad_keys.intersection(keys) or abs_axes:
                    found = True
                    print(f"  {path} — {dev.name}")
            if not found:
                print("  (none)")
        except ImportError:
            print("\nevdev not installed (pip install evdev) — skipping Linux device scan.")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="LAB/pilot bridge — gamepad → WebSocket relay for the browser.",
    )
    parser.add_argument(
        "--host",
        default=HOST,
        help=f"WebSocket bind address (default: {HOST}). "
             f"Use 0.0.0.0 to accept from all interfaces if the browser "
             f"resolves localhost to something other than 127.0.0.1.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"WebSocket listen port (default: {DEFAULT_PORT})",
    )
    parser.add_argument(
        "--udp-port",
        type=int,
        default=UDP_PORT,
        help=f"UDP teleop destination port when lock=0 (default: {UDP_PORT})",
    )
    parser.add_argument(
        "--no-udp",
        action="store_true",
        help="Disable the UDP teleop half (use if the browser drives the robot directly).",
    )
    parser.add_argument(
        "--list-devices",
        action="store_true",
        help="List detected gamepads and exit",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # A common source of noise: some Windows tools (antivirus, browser tabs
    # probing the port, HTTP-only clients hitting ws://) open a TCP
    # connection and close it before sending an HTTP upgrade. websockets
    # logs this at ERROR with a full traceback. Downgrade to WARNING
    # without a stack trace — real problems still surface.
    logging.getLogger("websockets.server").setLevel(logging.WARNING)
    logging.getLogger("websockets.asyncio.server").setLevel(logging.WARNING)

    if args.list_devices:
        list_devices()
        return 0

    server = BridgeServer(
        args.host, args.port,
        udp_port=args.udp_port,
        udp_enabled=not args.no_udp,
    )
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        LOG.info("Shutting down.")
    finally:
        server._udp_sock.close()
        pygame.quit()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())