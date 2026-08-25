#!/usr/bin/env python3
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations

"""
GPS mux — single owner of /dev/um982_gps.

Exposes two kinds of consumer from one physical port:
    1. PTY symlinked at /tmp/scoutlab_gps_pty  (bidirectional — Polaris talks here)
    2. UDP fan-out to one or more local ports  (NMEA — teleop, lab_inference, …)

NMEA from the receiver goes to the PTY (so Polaris can compute a position and
request corrections for it) AND to every configured UDP port (so each consumer
gets the full stream including HDT and #ADRNAVA).

RTCM corrections written by Polaris come back in via the PTY and are forwarded
to the real receiver.

Reconnects on USB drop. PTY symlink stays valid across reconnects.

WHY MULTIPLE UDP PORTS
──────────────────────
UDP unicast sockets cannot share a port. SO_REUSEADDR does not help (it only
relaxes TIME_WAIT / multicast), and SO_REUSEPORT would load-balance datagrams
between listeners rather than duplicating them — each consumer would receive a
random half of the NMEA sentences. So the fan-out has to happen here, at the
single writer, with one sendto() per target.

    57002 → LAB/teleop.py     (GpsReader → dashboard + recorder)
    57003 → lab_inference.py  (policy observation: lat / long / orientation)

Cost is one extra loopback sendto per sentence — negligible.

Env overrides:
    GPS_REAL_PORT  (default /dev/um982_gps)
    GPS_REAL_BAUD  (default 115200)
    GPS_PTY_PATH   (default /tmp/scoutlab_gps_pty)
    GPS_UDP_HOST   (default 127.0.0.1)
    GPS_UDP_PORTS  (default "57002,57003")  comma-separated fan-out targets
    GPS_UDP_PORT   (legacy, single port; used only if GPS_UDP_PORTS is unset)
"""

import errno
import fcntl
import os
import pty
import select
import signal
import socket
import sys
import termios
import time
import tty

import serial


REAL_PORT   = os.environ.get("GPS_REAL_PORT", "/dev/um982_gps")
REAL_BAUD   = int(os.environ.get("GPS_REAL_BAUD", "115200"))
PTY_SYMLINK = os.environ.get("GPS_PTY_PATH",   "/tmp/scoutlab_gps_pty")
UDP_HOST    = os.environ.get("GPS_UDP_HOST",   "127.0.0.1")

_DEFAULT_UDP_PORTS = "57002,57003"


def _parse_udp_ports() -> list:
    """Resolve the fan-out target list.

    GPS_UDP_PORTS wins. GPS_UDP_PORT is honoured for backwards compatibility
    with older unit files that only knew about a single consumer. Invalid or
    duplicate entries are dropped with a warning rather than taking the mux
    down — losing one consumer must never cost us the GPS feed itself.
    """
    raw = os.environ.get("GPS_UDP_PORTS")
    if not raw:
        legacy = os.environ.get("GPS_UDP_PORT")
        raw = legacy if legacy else _DEFAULT_UDP_PORTS

    ports: list = []
    for chunk in str(raw).split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            port = int(chunk)
        except ValueError:
            print(f"[gps_mux] ignoring bad port {chunk!r}", flush=True)
            continue
        if not (1 <= port <= 65535):
            print(f"[gps_mux] ignoring out-of-range port {port}", flush=True)
            continue
        if port in ports:
            continue
        ports.append(port)

    if not ports:
        print("[gps_mux] no valid UDP ports configured — NMEA fan-out disabled",
              flush=True)
    return ports


UDP_PORTS = _parse_udp_ports()


_running = True


def _on_signal(*_):
    global _running
    _running = False


def _open_real():
    try:
        ser = serial.Serial(
            port=REAL_PORT, baudrate=REAL_BAUD,
            bytesize=serial.EIGHTBITS, parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
            timeout=0, write_timeout=1.0,
        )
        # Avoid toggling DTR/RTS on open — some receivers interpret as reset
        try: ser.dtr = False
        except Exception: pass
        try: ser.rts = False
        except Exception: pass
        return ser
    except Exception as exc:
        print(f"[gps_mux] open {REAL_PORT} failed: {exc}", flush=True)
        return None


def _create_pty():
    master_fd, slave_fd = pty.openpty()
    slave_name = os.ttyname(slave_fd)
    # Raw mode on slave (Polaris sees a clean byte stream)
    tty.setraw(slave_fd, termios.TCSANOW)
    # Non-blocking master so our select loop never wedges
    flags = fcntl.fcntl(master_fd, fcntl.F_GETFL)
    fcntl.fcntl(master_fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    # Stable symlink for downstream tools
    try:
        if os.path.islink(PTY_SYMLINK) or os.path.exists(PTY_SYMLINK):
            os.unlink(PTY_SYMLINK)
    except OSError:
        pass
    os.symlink(slave_name, PTY_SYMLINK)
    return master_fd, slave_fd, slave_name


def _fan_out(udp: socket.socket, line: bytes) -> None:
    """Send one NMEA sentence to every configured consumer.

    A dead consumer (nothing bound on that port) produces ECONNREFUSED on
    loopback UDP. That is normal — teleop or lab_inference may simply not be
    running — so it is swallowed per-target and never stops the others.
    """
    for port in UDP_PORTS:
        try:
            udp.sendto(line, (UDP_HOST, port))
        except OSError:
            pass


def main():
    signal.signal(signal.SIGINT,  _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    master_fd, slave_fd, slave_name = _create_pty()
    print(f"[gps_mux] PTY {slave_name} → {PTY_SYMLINK}", flush=True)

    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    targets = ", ".join(f"udp://{UDP_HOST}:{p}" for p in UDP_PORTS) or "(none)"
    print(f"[gps_mux] NMEA fan-out → {targets}", flush=True)
    print(f"[gps_mux] real port    = {REAL_PORT} @ {REAL_BAUD}", flush=True)

    ser = None
    backoff = 1.0
    last_attempt = 0.0
    nmea_buf = bytearray()

    try:
        while _running:
            now = time.monotonic()

            if ser is None:
                if (now - last_attempt) >= backoff:
                    last_attempt = now
                    ser = _open_real()
                    if ser is None:
                        backoff = min(backoff * 2, 10.0)
                        print(f"[gps_mux] retry {REAL_PORT} in {backoff:.1f}s", flush=True)
                    else:
                        backoff = 1.0
                        print(f"[gps_mux] opened {REAL_PORT}", flush=True)
                else:
                    time.sleep(0.1)
                    continue

            try:
                rlist, _, _ = select.select([ser.fileno(), master_fd], [], [], 0.5)
            except (OSError, ValueError):
                try: ser.close()
                except Exception: pass
                ser = None
                continue

            # Receiver → Polaris (PTY) + UDP fan-out
            if ser is not None and ser.fileno() in rlist:
                try:
                    data = os.read(ser.fileno(), 4096)
                except OSError as exc:
                    if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                        data = b""
                    else:
                        print(f"[gps_mux] read err: {exc}", flush=True)
                        try: ser.close()
                        except Exception: pass
                        ser = None
                        data = b""

                if data:
                    try: os.write(master_fd, data)
                    except OSError: pass

                    nmea_buf.extend(data)
                    while True:
                        idx = nmea_buf.find(b"\n")
                        if idx < 0:
                            break
                        line = bytes(nmea_buf[:idx + 1])
                        del nmea_buf[:idx + 1]
                        text = line.replace(b"\r", b"").strip()
                        if text:
                            _fan_out(udp, text + b"\n")
                    if len(nmea_buf) > 8192:
                        del nmea_buf[:4096]

            # Polaris (PTY) → receiver  (RTCM corrections)
            if master_fd in rlist:
                try:
                    data = os.read(master_fd, 4096)
                except OSError as exc:
                    if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK, errno.EIO):
                        data = b""
                    else:
                        data = b""

                if data and ser is not None:
                    try:
                        ser.write(data)
                    except (serial.SerialTimeoutException, serial.SerialException, OSError):
                        # Heavy data pressure or full hardware buffers.
                        # Simply drop this packet and keep the serial line alive!
                        pass
                    except Exception as exc:
                        # Actual device unplugged or unrecoverable system failure.
                        print(f"[gps_mux] Fatal hardware failure: {exc}", flush=True)
                        try: ser.close()
                        except Exception: pass
                        ser = None
    finally:
        print("[gps_mux] shutdown", flush=True)
        if ser is not None:
            try: ser.close()
            except Exception: pass
        try: os.close(master_fd)
        except OSError: pass
        try: os.close(slave_fd)
        except OSError: pass
        try:
            if os.path.islink(PTY_SYMLINK):
                os.unlink(PTY_SYMLINK)
        except OSError:
            pass
        try: udp.close()
        except Exception: pass


if __name__ == "__main__":
    main()