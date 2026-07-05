# -*- coding: utf-8 -*-
"""
Created on Sun Jul  5 11:22:28 2026

@author: Aadi
"""
from __future__ import annotations

"""
azure_telemetry.py — real-data telemetry to the live dashboard + Azure IoT Hub.

Replaces the plain-UDP UdpTelemetryPublisher. Two independent cadences, both
driven off a single snapshot_fn() that returns REAL robot values (built in
teleop.py):

    every dashboard_interval_s (1 s):  WebSocket JSON → wss://streams.revobots.ai/api/ws/telemetry/<robot_id>
    every iot_interval_s      (30 s):  IoT Hub JSON via DPS (Grafana / long-term)

Adapted from util_send_telemetry.py — same DPS registration + WS/IoT loops and
the same backoff/reconnect behavior — but:
    - FakeTelemetry is gone; values come from snapshot_fn()
    - runs inside teleop as a subsystem: start()/stop() like everything else
      (its own thread hosts a private asyncio loop)
    - "fake" is always False

IoT Hub secrets come from an env file (default /etc/revobots/revo.env):
    AZURE_DEVICE_ID, AZURE_DPS_ID_SCOPE, AZURE_DPS_PRIMARY_KEY
If any are missing, the IoT Hub half is skipped and only the dashboard WS runs.

Dependencies (install once on the robot):
    pip3 install --user websockets azure-iot-device
Both imports are lazy, so a missing package disables only that half with a log
line instead of taking teleop down.

Payload shape (matches the dashboard):
    {"robot_id","ts","robot_battery_pct","speed_pct","speed_mode","box_temp_F",
     "cpu_temp_F","humidity_pct","gps_lat","gps_lng","gps_orient","gps_fix",
     "gps_alt","up_time","fake":false}
Whatever keys snapshot_fn() omits are simply absent (gps_fix defaults to
"NO_FIX"); the publisher never blocks on missing sensor data.
"""


import asyncio
import json
import threading
import time
from typing import Callable, Optional

from .common import log

import logging

# Silence the azure-iot-device SDK and its underlying paho MQTT chatter.
# Keep WARNING+ so real problems still surface.
for _name in (
    "azure",
    "azure.iot",
    "azure.iot.device",
    "azure.iot.device.common",
    "azure.iot.device.common.mqtt_transport",
    "azure.iot.device.common.pipeline",
    "azure.iot.device.iothub",
    "azure.iot.device.iothub.aio",
    "azure.iot.device.iothub.abstract_clients",
    "azure.iot.device.provisioning",
    "azure.iot.device.provisioning.aio",
    "azure.iot.device.provisioning.pipeline",
    "azure.iot.device.provisioning.abstract_provisioning_device_client",
    "paho",
    "paho.mqtt",
    "websockets",
    "websockets.client",
):
    logging.getLogger(_name).setLevel(logging.WARNING)

DPS_HOST        = "global.azure-devices-provisioning.net"
DEFAULT_WS_BASE = "wss://streams.revobots.ai/api/ws/telemetry"


# ── helpers ───────────────────────────────────────────────────────────────────

def _read_env_file(path: str) -> dict:
    out: dict = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[7:]
                if "=" not in line:
                    continue
                key, val = line.split("=", 1)
                out[key.strip()] = val.strip().strip("\"'")
    except FileNotFoundError:
        pass
    except Exception as exc:
        log("azure_tel", f"error reading {path}: {exc}")
    return out


def _format_uptime(seconds: float) -> str:
    total_minutes = int(max(0.0, seconds)) // 60
    return f"{total_minutes // 60:02d}:{total_minutes % 60:02d}"


# ── publisher ─────────────────────────────────────────────────────────────────

class AzureTelemetryPublisher:
    def __init__(
        self,
        robot_id:              str,
        snapshot_fn:           Callable[[], dict],
        *,
        env_file:              str   = "/etc/revobots/revo.env",
        ws_base:               str   = DEFAULT_WS_BASE,
        dashboard_interval_s:  float = 1.0,
        iot_interval_s:        float = 30.0,
        enable_iot:            bool  = True,
    ) -> None:
        self._robot_id      = robot_id
        self._snapshot_fn   = snapshot_fn
        self._ws_url        = f"{ws_base}/{robot_id}"
        self._dash_interval = max(0.1, dashboard_interval_s)
        self._iot_interval  = max(1.0, iot_interval_s)
        self._enable_iot    = enable_iot

        secrets = _read_env_file(env_file)
        self._iot_secrets = {
            k: secrets.get(k, "")
            for k in ("AZURE_DEVICE_ID", "AZURE_DPS_ID_SCOPE", "AZURE_DPS_PRIMARY_KEY")
        }
        self._iot_ok = all(self._iot_secrets.values())

        self._t0        = time.time()
        self._thread: Optional[threading.Thread] = None
        self._loop:   Optional[asyncio.AbstractEventLoop] = None
        self._stop_evt: Optional[asyncio.Event] = None
        self._stopping  = False

    # ── lifecycle ───────────────────────────────────────────────────────────

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._thread_main, daemon=True, name="azure-telemetry"
        )
        self._thread.start()
        log("azure_tel",
            f"dashboard WS → {self._ws_url} @ {1.0/self._dash_interval:.0f}Hz")
        if self._enable_iot and self._iot_ok:
            log("azure_tel",
                f"IoT Hub via DPS every {self._iot_interval:.0f}s "
                f"(device={self._iot_secrets['AZURE_DEVICE_ID']})")
        else:
            log("azure_tel", "IoT Hub disabled (secrets missing in env file)")

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
            log("azure_tel", f"telemetry thread exited: {exc}")

    async def _async_main(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._stop_evt = asyncio.Event()
        if self._stopping:               # stop() raced start()
            return
        tasks = [asyncio.create_task(self._dashboard_loop())]
        if self._enable_iot and self._iot_ok:
            tasks.append(asyncio.create_task(self._iot_hub_loop()))
        await asyncio.gather(*tasks, return_exceptions=True)

    # ── payload ─────────────────────────────────────────────────────────────

    def _build_payload(self) -> dict:
        try:
            snap = self._snapshot_fn() or {}
        except Exception as exc:
            log("azure_tel", f"snapshot error: {exc}")
            snap = {}
        now = time.time()
        payload: dict = {"robot_id": self._robot_id, "ts": now}
        payload.update(snap)
        payload.setdefault("gps_fix", "NO_FIX")
        payload["up_time"] = _format_uptime(now - self._t0)
        payload["fake"] = False
        return payload

    # ── dashboard WebSocket loop ────────────────────────────────────────────

    async def _dashboard_loop(self) -> None:
        try:
            import websockets
        except Exception as exc:
            log("azure_tel", f"websockets not installed ({exc}) — dashboard disabled")
            return

        backoff = 1.0
        while not self._stop_evt.is_set():
            try:
                async with websockets.connect(self._ws_url, open_timeout=15) as ws:
                    log("azure_tel", f"dashboard WS connected: {self._ws_url}")
                    backoff = 1.0
                    while not self._stop_evt.is_set():
                        body = json.dumps(self._build_payload(), separators=(",", ":"))
                        await ws.send(body)
                        try:
                            await asyncio.wait_for(self._stop_evt.wait(),
                                                   timeout=self._dash_interval)
                        except asyncio.TimeoutError:
                            pass
            except Exception as exc:
                if self._stop_evt.is_set():
                    break
                log("azure_tel", f"dashboard WS error: {exc} — retry in {backoff:.0f}s")
                try:
                    await asyncio.wait_for(self._stop_evt.wait(), timeout=backoff)
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2.0, 30.0)

    # ── Azure IoT Hub loop (DPS-provisioned) ────────────────────────────────

    async def _iot_hub_loop(self) -> None:
        try:
            from azure.iot.device import Message
            from azure.iot.device.aio import (
                IoTHubDeviceClient, ProvisioningDeviceClient,
            )
        except Exception as exc:
            log("azure_tel", f"azure-iot-device not installed ({exc}) — IoT Hub disabled")
            return

        backoff = 5.0
        client = None
        while not self._stop_evt.is_set():
            try:
                if client is None:
                    client = await self._connect_iot(
                        ProvisioningDeviceClient, IoTHubDeviceClient
                    )
                    backoff = 5.0

                msg = Message(json.dumps(self._build_payload(), separators=(",", ":")))
                msg.content_encoding = "utf-8"
                msg.content_type = "application/json"
                await client.send_message(msg)

                try:
                    await asyncio.wait_for(self._stop_evt.wait(),
                                           timeout=self._iot_interval)
                except asyncio.TimeoutError:
                    pass
            except Exception as exc:
                if self._stop_evt.is_set():
                    break
                log("azure_tel", f"IoT Hub error: {exc} — retry in {backoff:.0f}s")
                if client is not None:
                    try: await client.disconnect()
                    except Exception: pass
                    client = None
                try:
                    await asyncio.wait_for(self._stop_evt.wait(), timeout=backoff)
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2.0, 60.0)

        if client is not None:
            try: await client.disconnect()
            except Exception: pass

    async def _connect_iot(self, ProvisioningDeviceClient, IoTHubDeviceClient):
        s = self._iot_secrets
        provisioning = ProvisioningDeviceClient.create_from_symmetric_key(
            provisioning_host=DPS_HOST,
            registration_id=s["AZURE_DEVICE_ID"],
            id_scope=s["AZURE_DPS_ID_SCOPE"],
            symmetric_key=s["AZURE_DPS_PRIMARY_KEY"],
        )
        result = await provisioning.register()
        if result.status != "assigned":
            raise RuntimeError(f"DPS registration failed: {result.status}")

        hub = result.registration_state.assigned_hub
        dev = result.registration_state.device_id
        log("azure_tel", f"IoT Hub assigned: {dev} @ {hub}")

        client = IoTHubDeviceClient.create_from_symmetric_key(
            symmetric_key=s["AZURE_DPS_PRIMARY_KEY"],
            hostname=hub,
            device_id=dev,
        )
        await client.connect()
        return client