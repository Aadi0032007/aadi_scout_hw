# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations

"""
bridge_ws.py — outbound WebSocket client to the fleet relay.

The robot connects OUT to wss://streams.revobots.ai/api/ws/robot/{robot_id}
and receives browser-pilot events as flat JSON dicts. Whatever comes in is
handed to on_message(dict). Fire-and-forget, no ack.

Structure mirrors azure_telemetry.py:
    - private thread hosting a private asyncio loop
    - start()/stop() public API
    - websockets import is lazy so a missing dep disables the module cleanly
    - exponential backoff on disconnect (1s -> 30s cap)

Singleton is enforced by the FLEET, not us. Only one WS per robot_id may be
open at a time; a second connection silently kicks the first. Consequence:
do NOT run util_receive_browser_cmds.py while teleop is up, or they'll
flap kicking each other.

Ping frames from the relay come through as {"type":"ping"} and are dropped
here — they're relay heartbeats, not browser events.

Message shapes we forward to on_message (from util_receive_browser_cmds.py):
    {"robot_lock": bool}
    {"lin_x": ..., "ang_z": ...}                    (drive; optional over WS)
    {"head": "left|right|up|down|center|"}
    {"speed_mode": "slow|medium|fast"}
    {"bubble_mode": bool}
    {"ai_mode": bool}
    {"high_visibility": bool}
    {"charging": bool}
    {"type": "stt", "text": "..."}
    {"type": "display_text", "text": "..."}          (TODO subsystem)
    {"type": "set_wallpaper", "image": "..."}        (TODO subsystem)
"""

import asyncio
import json
import logging
import threading
from typing import Callable, Optional

from .common import log

# Silence websockets INFO chatter (reconnect, close, etc). WARNING+ still shows.
for _name in ("websockets", "websockets.client", "websockets.protocol"):
    logging.getLogger(_name).setLevel(logging.WARNING)


class BridgeWsClient:
    def __init__(
        self,
        url:        str,
        on_message: Callable[[dict], None],
    ) -> None:
        self._url        = url
        self._on_message = on_message

        self._thread:   Optional[threading.Thread]         = None
        self._loop:     Optional[asyncio.AbstractEventLoop] = None
        self._stop_evt: Optional[asyncio.Event]             = None
        self._stopping = False

    # ── lifecycle ───────────────────────────────────────────────────────────

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._thread_main, daemon=True, name="bridge-ws"
        )
        self._thread.start()
        log("bridge_ws", f"connecting → {self._url}")

    def stop(self) -> None:
        self._stopping = True
        loop, evt = self._loop, self._stop_evt
        if loop is not None and evt is not None:
            try:
                loop.call_soon_threadsafe(evt.set)
            except Exception:
                pass
        if self._thread is not None:
            self._thread.join(timeout=3.0)
            self._thread = None

    # ── thread + asyncio bootstrap ──────────────────────────────────────────

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._async_main())
        except Exception as exc:
            log("bridge_ws", f"thread exited: {exc}")

    async def _async_main(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._stop_evt = asyncio.Event()
        if self._stopping:
            return

        try:
            import websockets
        except Exception as exc:
            log("bridge_ws",
                f"websockets not installed ({exc}) — bridge WS disabled")
            return

        backoff = 1.0
        while not self._stop_evt.is_set():
            try:
                async with websockets.connect(
                    self._url,
                    open_timeout=15,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                ) as ws:
                    log("bridge_ws", "connected")
                    backoff = 1.0
                    await self._recv_until_close_or_stop(ws)
            except Exception as exc:
                if self._stop_evt.is_set():
                    break
                log("bridge_ws",
                    f"link error: {exc} — retry in {backoff:.0f}s")
                try:
                    await asyncio.wait_for(self._stop_evt.wait(), timeout=backoff)
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2.0, 30.0)

    async def _recv_until_close_or_stop(self, ws) -> None:
        recv_task = asyncio.create_task(self._pump(ws))
        stop_task = asyncio.create_task(self._stop_evt.wait())
        try:
            done, pending = await asyncio.wait(
                {recv_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        finally:
            if not recv_task.done():
                recv_task.cancel()
                await asyncio.gather(recv_task, return_exceptions=True)

    async def _pump(self, ws) -> None:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError as exc:
                log("bridge_ws", f"bad JSON: {exc}")
                continue
            if not isinstance(msg, dict):
                continue
            # Relay heartbeat — not a browser event. Drop it.
            if msg.get("type") == "ping":
                continue
            try:
                self._on_message(msg)
            except Exception as exc:
                log("bridge_ws", f"dispatch error: {exc}")