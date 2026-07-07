# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations

"""
display.py — Fullscreen pygame monitor subsystem for the on-robot screen.

Consolidated controller + child into one file. Two roles:

  1) IMPORTED BY TELEOP as a module (the normal path):
         from LAB.display import DisplayController
         display = DisplayController(...)
         display.start()
         display.show_image("STOP.png")
         display.show_text("Robot stopped")
         display.stop()

     Same start/stop/show_* shape as LightsController and AudioController.

  2) RUN AS A SCRIPT (spawned by DisplayController itself):
         python3 -m LAB.display --child [--orientation N] [--windowed]

     This is what DisplayController launches under the hood. Same file,
     different entry point, driven by the __main__ guard at the bottom.

Why subprocess (rather than a thread in the teleop process)?
LocalGamepad already owns pygame's event queue on its worker thread.
Sharing pygame subsystems across two threads of the same process is
fragile on Linux X11 — SDL requires strict thread affinity per
subsystem. A subprocess sidesteps the entire class of races and
matches the rest of the stack's fail-open discipline: if the monitor
is unplugged or DISPLAY is wrong, only the child dies, teleop keeps
running with display=None.

Command wire between controller and child (stdin, one line per command):
    img <absolute path>          → show image fullscreen (scaled, centered)
    txt <base64 UTF-8>           → show text fullscreen (white on black)
    clear                        → blank screen
    quit                         → clean exit

Text is base64-encoded so newlines and shell metachars in operator
input survive the wire intact.
"""

import argparse
import base64
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Optional

from .common import log


# ═════════════════════════════════════════════════════════════════════════════
#  CONTROLLER — imported by teleop, runs in the teleop process
# ═════════════════════════════════════════════════════════════════════════════


class DisplayController:
    """Public subsystem API — mirrors LightsController / AudioController.

    All rendering state lives in the pygame subprocess; this class is a
    thin stdin-writer with a watcher thread that flips `_disabled` if the
    child dies unexpectedly.
    """

    def __init__(
        self,
        display_dir:    str,
        default_image:  str,
        orientation:    int = 0,
        x_display:      str = ":0",
        x_authority:    str = "",
        windowed:       bool = False,
    ) -> None:
        self._display_dir    = Path(display_dir).expanduser()
        self._default_image  = default_image
        self._orientation    = int(orientation) % 360
        self._x_display      = x_display
        self._x_authority    = x_authority or str(Path.home() / ".Xauthority")
        self._windowed       = bool(windowed)

        self._proc:      Optional[subprocess.Popen] = None
        self._proc_lock  = threading.Lock()
        self._stop       = threading.Event()
        self._disabled   = False

    # ── lifecycle ───────────────────────────────────────────────────────────

    def start(self) -> None:
        if not self._spawn():
            self._disabled = True
            return

        # Show default image on boot. If missing, blank screen.
        default_path = self._display_dir / self._default_image
        if default_path.is_file():
            self._send_cmd(f"img {default_path}")
            log("display", f"default → {default_path.name}")
        else:
            log("display",
                f"default image missing: {default_path} — blank screen")
            self._send_cmd("clear")

    def stop(self) -> None:
        if self._disabled:
            return
        self._stop.set()
        # Ask child to exit cleanly; give it a moment before force-kill.
        self._send_cmd("quit")
        with self._proc_lock:
            proc = self._proc
        if proc is not None:
            try:
                proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                try:
                    proc.terminate()
                    proc.wait(timeout=1.0)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
        with self._proc_lock:
            self._proc = None

    # ── public API (called from teleop dispatchers) ────────────────────────

    def show_image(self, filename: str) -> None:
        """Resolve filename against display_dir (or accept absolute path)."""
        if self._disabled or not filename:
            return
        raw = filename.strip()
        path = Path(raw) if os.path.isabs(raw) else self._display_dir / raw
        if not path.is_file():
            log("display", f"image not found: {path}")
            return
        self._send_cmd(f"img {path}")
        log("display", f"show image {path.name}")

    def show_text(self, text: str) -> None:
        """Base64-encode and send. Preserves newlines / quotes / metachars."""
        if self._disabled or not text:
            return
        encoded = base64.b64encode(text.encode("utf-8")).decode("ascii")
        self._send_cmd(f"txt {encoded}")
        preview = text if len(text) <= 40 else text[:37] + "..."
        log("display", f"show text {preview!r}")

    def clear(self) -> None:
        if self._disabled:
            return
        self._send_cmd("clear")
        log("display", "cleared")

    # ── internals ───────────────────────────────────────────────────────────

    def _spawn(self) -> bool:
        env = os.environ.copy()
        env["DISPLAY"]          = self._x_display
        env["XAUTHORITY"]       = self._x_authority
        env["PYTHONUNBUFFERED"] = "1"

        # Same file, re-invoked as a module with --child.
        cmd = [
            sys.executable, "-m", "LAB.display",
            "--child",
            "--orientation", str(self._orientation),
        ]
        if self._windowed:
            cmd.append("--windowed")

        try:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                env=env,
                text=True,
                bufsize=1,     # line-buffered on the Python side
            )
        except Exception as exc:
            log("display", f"spawn failed: {exc}")
            return False

        # Wait briefly. If pygame can't init (no monitor, wrong DISPLAY,
        # xauth issue), the child exits within a few hundred ms.
        time.sleep(0.5)
        if proc.poll() is not None:
            stderr = ""
            try:
                stderr = (proc.stderr.read() if proc.stderr else "")[:400]
            except Exception:
                pass
            log("display",
                f"child exited immediately (rc={proc.returncode}): "
                f"{stderr.strip() or 'no stderr'}")
            return False

        with self._proc_lock:
            self._proc = proc

        threading.Thread(
            target=self._watch, daemon=True, name="display-watch",
        ).start()

        log("display",
            f"child spawned (pid={proc.pid}, DISPLAY={self._x_display}, "
            f"rotate={self._orientation}°"
            + (", windowed" if self._windowed else "") + ")")
        return True

    def _watch(self) -> None:
        """Log + disable subsystem if the child dies while we weren't asking."""
        with self._proc_lock:
            proc = self._proc
        if proc is None:
            return
        proc.wait()
        if self._stop.is_set():
            return
        log("display",
            f"child exited unexpectedly (rc={proc.returncode}) — subsystem disabled")
        self._disabled = True
        with self._proc_lock:
            self._proc = None

    def _send_cmd(self, cmd: str) -> None:
        with self._proc_lock:
            proc = self._proc
        if proc is None or proc.stdin is None:
            return
        try:
            proc.stdin.write(cmd + "\n")
            proc.stdin.flush()
        except (BrokenPipeError, OSError) as exc:
            log("display", f"stdin write failed: {exc}")


# ═════════════════════════════════════════════════════════════════════════════
#  CHILD MODE — only runs when invoked as `python3 -m LAB.display --child`
# ═════════════════════════════════════════════════════════════════════════════
#
# Everything below is dormant when the module is imported by teleop.
# The pygame import is deferred inside _run_child() so teleop never pays
# the pygame import cost and won't crash on machines missing pygame.


_BG           = (0, 0, 0)
_FG           = (255, 255, 255)
_TEXT_PADDING = 80


def _wrap_text(font, text: str, max_width: int) -> list:
    """Word-wrap text to lines that fit within max_width for the given font."""
    lines: list = []
    for paragraph in text.splitlines():
        words = paragraph.split()
        if not words:
            lines.append("")
            continue
        current = words[0]
        for w in words[1:]:
            trial = f"{current} {w}"
            if font.size(trial)[0] <= max_width:
                current = trial
            else:
                lines.append(current)
                current = w
        lines.append(current)
    return lines


def _pick_font(pygame, text: str, canvas_w: int, canvas_h: int):
    """Binary-search-ish: largest font size that fits the whole message."""
    max_width  = canvas_w - _TEXT_PADDING * 2
    max_height = canvas_h - _TEXT_PADDING * 2
    for size in range(160, 16, -4):
        font = pygame.font.SysFont(None, size)
        lines = _wrap_text(font, text, max_width)
        block_h = font.get_linesize() * len(lines)
        if block_h <= max_height:
            widest = max((font.size(l)[0] for l in lines), default=0)
            if widest <= max_width:
                return font
    return pygame.font.SysFont(None, 24)


def _render_text(pygame, canvas, text: str) -> None:
    canvas.fill(_BG)
    cw, ch = canvas.get_size()
    font = _pick_font(pygame, text, cw, ch)
    max_width = cw - _TEXT_PADDING * 2
    lines = _wrap_text(font, text, max_width)
    line_h = font.get_linesize()
    block_h = line_h * len(lines)
    y = (ch - block_h) // 2
    for line in lines:
        surf = font.render(line, True, _FG)
        x = (cw - surf.get_width()) // 2
        canvas.blit(surf, (x, y))
        y += line_h


def _render_image(pygame, canvas, img_path: str) -> None:
    canvas.fill(_BG)
    try:
        image = pygame.image.load(img_path)
    except Exception as exc:
        # Load failed — render an error message rather than crash the child.
        _render_text(
            pygame, canvas,
            f"Error loading image:\n{os.path.basename(img_path)}\n{exc}",
        )
        return
    if image.get_alpha() is not None:
        image = image.convert_alpha()
    else:
        image = image.convert()
    cw, ch = canvas.get_size()
    iw, ih = image.get_size()
    scale = min(cw / iw, ch / ih)
    new_size = (max(1, int(iw * scale)), max(1, int(ih * scale)))
    image = pygame.transform.smoothscale(image, new_size)
    x = (cw - new_size[0]) // 2
    y = (ch - new_size[1]) // 2
    canvas.blit(image, (x, y))


def _canvas_size(sw: int, sh: int, orientation: int) -> tuple:
    """Portrait canvas for a portrait-mounted panel on a landscape framebuffer."""
    if orientation in (90, 270):
        return sh, sw
    return sw, sh


def _present(pygame, screen, canvas, orientation: int) -> None:
    if orientation == 0:
        screen.blit(canvas, (0, 0))
    else:
        rotated = pygame.transform.rotate(canvas, -orientation)
        if rotated.get_size() != screen.get_size():
            rotated = pygame.transform.smoothscale(rotated, screen.get_size())
        screen.fill(_BG)
        screen.blit(rotated, (0, 0))
    pygame.display.flip()


class _ChildState:
    """Thread-safe state shared between stdin reader and render loop."""

    def __init__(self) -> None:
        self._lock    = threading.Lock()
        self._kind    = "clear"    # clear | img | txt
        self._payload = None
        self._dirty   = True
        self._running = True

    def set(self, kind: str, payload=None) -> None:
        with self._lock:
            self._kind    = kind
            self._payload = payload
            self._dirty   = True

    def take(self) -> tuple:
        with self._lock:
            was_dirty = self._dirty
            self._dirty = False
            return was_dirty, self._kind, self._payload

    def quit(self) -> None:
        with self._lock:
            self._running = False

    def is_running(self) -> bool:
        with self._lock:
            return self._running


def _stdin_reader(state: "_ChildState") -> None:
    """Daemon: read commands from stdin. One per line. EOF or 'quit' → exit."""
    try:
        while True:
            line = sys.stdin.readline()
            if not line:            # EOF — parent closed pipe
                break
            line = line.rstrip("\r\n")
            if not line:
                continue
            if line == "quit":
                break
            if line == "clear":
                state.set("clear")
                continue
            if line.startswith("img "):
                state.set("img", line[4:])
                continue
            if line.startswith("txt "):
                try:
                    text = base64.b64decode(line[4:]).decode("utf-8", errors="replace")
                except Exception:
                    text = "<decode error>"
                state.set("txt", text)
                continue
            # unknown command — silently ignored
    except Exception:
        pass
    finally:
        state.quit()


def _run_child(args) -> int:
    orientation = int(args.orientation) % 360
    if orientation not in (0, 90, 180, 270):
        print(f"orientation must be 0/90/180/270; got {orientation}",
              file=sys.stderr)
        return 1

    try:
        import pygame
    except ImportError:
        print("pygame not installed — display disabled", file=sys.stderr)
        return 1

    try:
        pygame.display.init()
        pygame.font.init()
    except pygame.error as exc:
        print(f"pygame init failed: {exc}", file=sys.stderr)
        return 1

    # Try fullscreen; fall back to windowed if the compositor rejects it.
    try:
        if args.windowed:
            screen = pygame.display.set_mode((1024, 600), pygame.RESIZABLE)
        else:
            screen = pygame.display.set_mode((0, 0), pygame.FULLSCREEN)
    except pygame.error as exc:
        if args.windowed:
            print(f"windowed mode failed: {exc}", file=sys.stderr)
            return 1
        print(f"fullscreen failed ({exc}); falling back to windowed",
              file=sys.stderr)
        try:
            screen = pygame.display.set_mode((1024, 600), pygame.RESIZABLE)
        except pygame.error as exc2:
            print(f"windowed fallback also failed: {exc2}", file=sys.stderr)
            return 1

    pygame.mouse.set_visible(False)
    pygame.display.set_caption("REVO Scout Display")

    sw, sh = screen.get_size()
    cw, ch = _canvas_size(sw, sh, orientation)
    canvas = pygame.Surface((cw, ch))
    canvas.fill(_BG)
    _present(pygame, screen, canvas, orientation)

    print(f"[display_child] screen={sw}x{sh} canvas={cw}x{ch} "
          f"rotate={orientation}", flush=True)

    state = _ChildState()
    threading.Thread(
        target=_stdin_reader, args=(state,), daemon=True, name="stdin",
    ).start()

    clock = pygame.time.Clock()
    try:
        while state.is_running():
            # Pump events so the window stays alive. Keyboard/mouse are
            # deliberately ignored — nobody should be able to close the
            # display by hitting a stray key. QUIT (window-close) is honored.
            for event in pygame.event.get():
                if event.type == pygame.QUIT:
                    state.quit()

            dirty, kind, payload = state.take()
            if dirty:
                if kind == "img" and payload:
                    _render_image(pygame, canvas, payload)
                elif kind == "txt" and payload:
                    _render_text(pygame, canvas, payload)
                else:
                    canvas.fill(_BG)
                _present(pygame, screen, canvas, orientation)

            clock.tick(30)   # 30 fps idle — plenty for a poster display
    finally:
        pygame.quit()

    return 0


# ═════════════════════════════════════════════════════════════════════════════
#  Entry point
# ═════════════════════════════════════════════════════════════════════════════


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="REVO Scout display subsystem — child mode.",
    )
    p.add_argument("--child", action="store_true",
                   help="Run as pygame child process (required for child mode)")
    p.add_argument("--orientation", type=int, default=0,
                   help="Rotate 0/90/180/270 degrees clockwise")
    p.add_argument("--windowed", action="store_true",
                   help="Windowed mode instead of fullscreen (debug)")
    return p.parse_args()


if __name__ == "__main__":
    _args = _parse_args()
    if not _args.child:
        print(
            "LAB.display is a subsystem module.\n"
            "  Normal use: import DisplayController from teleop.\n"
            "  Manual test:\n"
            "    DISPLAY=:0 python3 -m LAB.display --child --orientation 90",
            file=sys.stderr,
        )
        sys.exit(2)
    sys.exit(_run_child(_args))