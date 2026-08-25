# -*- coding: utf-8 -*-
"""Peplink MAX BR1 cellular status for cloud telemetry.

Polls the local router (read-only) for phone-like WAN fields:
  carrier  - e.g. "RoamLink TMO"
  bars     - signalLevel 0..5
  network  - "5G" / "LTE" / …
  rsrp     - dBm (primary RAT band)
  sinr     - dB  (primary RAT band)

Background thread; snapshot() callers just read the cache. Login is
session-cookie based; we re-login when a status call fails. HTTPS uses an
unverified SSL context because the router ships a self-signed cert.
"""
from __future__ import annotations

import http.cookiejar
import json
import ssl
import threading
import time
import urllib.error
import urllib.request
from typing import Optional

from .common import log


def _ssl_ctx() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


class PeplinkWanReader:
    """Background poller for Peplink cellular carrier / bars / network."""

    def __init__(
        self,
        host: str = "192.168.10.1",
        username: str = "admin",
        password: str = "",
        poll_sec: float = 5.0,
        timeout_sec: float = 8.0,
        stale_after_sec: float = 30.0,
    ) -> None:
        self._base = f"https://{host}".rstrip("/")
        self._username = username
        self._password = password
        self._poll_sec = max(1.0, float(poll_sec))
        self._timeout_sec = float(timeout_sec)
        self._stale_after_sec = float(stale_after_sec)

        self._ssl = _ssl_ctx()
        self._cookies = http.cookiejar.CookieJar()
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self._cookies),
            urllib.request.HTTPSHandler(context=self._ssl),
        )
        self._logged_in = False

        self._data: dict = {}
        self._last_ok_t: float = 0.0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="peplink-wan")

    def start(self) -> None:
        if not self._password:
            log("peplink", "password unset — cellular telemetry disabled")
            return
        self._thread.start()
        log(
            "peplink",
            f"WAN reader started ({self._base}, poll={self._poll_sec:.0f}s)",
        )

    def get(self) -> dict:
        with self._lock:
            d = dict(self._data)
        if self._last_ok_t > 0:
            d["age_sec"] = time.monotonic() - self._last_ok_t
        return d

    def stop(self) -> None:
        self._stop.set()
        if self._logged_in:
            try:
                self._request("POST", "/api/logout", body=b"")
            except Exception:
                pass
            self._logged_in = False

    # ── HTTP ──────────────────────────────────────────────────────────────

    def _request(
        self,
        method: str,
        path: str,
        body: Optional[bytes] = None,
        content_type: Optional[str] = None,
    ) -> dict:
        url = self._base + path
        headers = {"Accept": "application/json"}
        if content_type:
            headers["Content-Type"] = content_type
        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        with self._opener.open(req, timeout=self._timeout_sec) as resp:
            raw = resp.read()
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8", errors="replace"))

    def _login(self) -> bool:
        payload = json.dumps(
            {"username": self._username, "password": self._password}
        ).encode("utf-8")
        try:
            d = self._request(
                "POST",
                "/api/login",
                body=payload,
                content_type="application/json",
            )
        except Exception as exc:
            log("peplink", f"login failed: {exc}")
            self._logged_in = False
            return False
        ok = d.get("stat") == "ok"
        self._logged_in = ok
        if not ok:
            log(
                "peplink",
                f"login rejected: code={d.get('code')} message={d.get('message')}",
            )
        return ok

    def _ensure_login(self) -> bool:
        if self._logged_in:
            return True
        return self._login()

    # ── parse ─────────────────────────────────────────────────────────────

    @staticmethod
    def _pick_cellular(resp: dict) -> Optional[dict]:
        if not isinstance(resp, dict):
            return None
        for key in resp.get("order") or []:
            item = resp.get(str(key))
            if isinstance(item, dict) and item.get("type") == "cellular":
                return item
        for key, item in resp.items():
            if key in ("order", "timestamp", "reportTimestamp", "supportGatewayProxy"):
                continue
            if isinstance(item, dict) and item.get("type") == "cellular":
                return item
        return None

    @staticmethod
    def _normalize_network(network) -> Optional[str]:
        if not network:
            return None
        text = str(network)
        if text.upper().startswith("5G"):
            return "5G"
        if "LTE" in text.upper() or text.upper().startswith("4G"):
            return "LTE"
        return text

    @classmethod
    def _pick_signal(cls, cel: dict, preferred_network: Optional[str]) -> dict:
        """Return rsrp/sinr from the preferred RAT band, else first available."""
        rats = cel.get("rat")
        if not isinstance(rats, list):
            return {}

        def bands_for(rat: dict):
            bands = rat.get("band") if isinstance(rat, dict) else None
            return bands if isinstance(bands, list) else []

        ordered = list(rats)
        if preferred_network:
            pref = preferred_network.upper()
            preferred = [
                r for r in rats
                if isinstance(r, dict) and pref in str(r.get("name") or "").upper()
            ]
            rest = [r for r in rats if r not in preferred]
            ordered = preferred + rest

        for rat in ordered:
            for band in bands_for(rat if isinstance(rat, dict) else {}):
                if not isinstance(band, dict):
                    continue
                sig = band.get("signal")
                if not isinstance(sig, dict):
                    continue
                out: dict = {}
                if sig.get("rsrp") is not None:
                    try:
                        out["cellular_rsrp"] = float(sig["rsrp"])
                    except (TypeError, ValueError):
                        pass
                if sig.get("sinr") is not None:
                    try:
                        out["cellular_sinr"] = float(sig["sinr"])
                    except (TypeError, ValueError):
                        pass
                if out:
                    return out
        return {}

    @classmethod
    def _extract(cls, wan: dict) -> dict:
        out: dict = {}
        cel = wan.get("cellular") if isinstance(wan, dict) else None
        if not isinstance(cel, dict):
            return out

        carrier = cel.get("carrier")
        if isinstance(carrier, dict) and carrier.get("name"):
            out["cellular_carrier"] = str(carrier["name"])
        elif wan.get("message"):
            # e.g. "Connected to RoamLink TMO"
            msg = str(wan["message"])
            prefix = "Connected to "
            if msg.startswith(prefix):
                out["cellular_carrier"] = msg[len(prefix):]

        level = cel.get("signalLevel")
        if level is not None:
            try:
                out["cellular_bars"] = int(level)
            except (TypeError, ValueError):
                pass

        network = cls._normalize_network(
            cel.get("network") or cel.get("mobileType") or cel.get("dataTechnology")
        )
        if network:
            out["cellular_network"] = network

        out.update(cls._pick_signal(cel, network))
        return out

    def _poll_once(self) -> bool:
        if not self._ensure_login():
            return False
        try:
            d = self._request("GET", "/api/status.wan.connection")
        except Exception as exc:
            log("peplink", f"status fetch failed: {exc}")
            self._logged_in = False
            return False
        if d.get("stat") != "ok":
            log(
                "peplink",
                f"status rejected: code={d.get('code')} message={d.get('message')}",
            )
            self._logged_in = False
            return False
        cel = self._pick_cellular(d.get("response") or {})
        extracted = self._extract(cel or {})
        with self._lock:
            self._data = extracted
            self._last_ok_t = time.monotonic()
        return True

    def _run(self) -> None:
        backoff = 2.0
        while not self._stop.is_set():
            ok = False
            try:
                ok = self._poll_once()
            except Exception as exc:
                log("peplink", f"poll error: {exc}")
                self._logged_in = False
            if ok:
                backoff = 2.0
                self._stop.wait(timeout=self._poll_sec)
            else:
                self._stop.wait(timeout=backoff)
                backoff = min(backoff * 2.0, 60.0)