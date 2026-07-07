# -*- coding: utf-8 -*-
"""
Created on Wed Jul  8 04:37:38 2026

@author: Aadi
"""

from __future__ import annotations
"""
display.py — pygame fullscreen display subsystem.

Runtime-facing API:
    start()                 — start the pygame worker thread
    stop()                  — stop pygame and release the display
    show_text(text)         — render a centered fullscreen message
    set_wallpaper(image)    — render a PNG/JPG fullscreen, resolved from display/
    clear()                 — return to the default wallpaper or blank screen

This is intentionally modeled after util_pygame_monitor_test.py, but turned
into a non-blocking teleop subsystem. Pygame work stays on one background
thread; the WS dispatcher only enqueues commands and never waits on X/SDL.
"""

import os
import sys
import threading
from pathlib import Path
from queue import Empty, Full, Queue
from typing import Optional

from .common import log


# Colors copied from the working monitor utility style.
BG = (18, 22, 32)
PANEL_BG = (24, 30, 44)
TEXT = (240, 240, 245)
MUTED = (180, 188, 200)
ERROR = (255, 150, 120)


class DisplayController:
    def __init__(
        self,
        display: Optional[str] = None,
        asset_dir: str = "",
        default_wallpaper: str = "STOP.png",
        rotate: int = 90,
        fullscreen: bool = True,
        fps: int = 30,
        enabled: bool = True,
    ) -> None:
        self._display = display
        self._asset_dir = Path(asset_dir).expanduser() if asset_dir else None
        self._default_wallpaper = default_wallpaper
        self._rotate = rotate if rotate in (0, 90, 180, 270) else 0
        self._fullscreen = bool(fullscreen)
        self._fps = max(1, int(fps))
        self._enabled = bool(enabled)

        self._queue: Queue[dict] = Queue(maxsize=8)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()
        self._started = False

    # ── lifecycle ─────────────────────────────────────────────────────────

    def start(self) -> None:
        if not self._enabled:
            log("display", "disabled by config")
            return
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="display"
        )
        self._thread.start()
        self._started = True

    def stop(self) -> None:
        self._stop.set()
        self._enqueue({"cmd": "stop"})
        if self._thread is not None:
            self._thread.join(timeout=3.0)
            self._thread = None
        self._ready.clear()
        self._started = False

    # ── public commands ───────────────────────────────────────────────────

    def show_text(self, text: str) -> None:
        text = str(text or "").strip()
        if not text:
            return
        self._enqueue({"cmd": "text", "text": text})

    def set_wallpaper(self, image: str) -> None:
        image = str(image or "").strip()
        if not image:
            return
        self._enqueue({"cmd": "wallpaper", "image": image})

    def clear(self) -> None:
        self._enqueue({"cmd": "clear"})

    # ── internals ─────────────────────────────────────────────────────────

    def _enqueue(self, item: dict) -> None:
        if not self._enabled:
            return
        try:
            self._queue.put_nowait(item)
        except Full:
            # Keep the newest UI command. Dropping one stale display update is
            # preferable to ever blocking the WS/audio/motion dispatch thread.
            try:
                self._queue.get_nowait()
            except Empty:
                pass
            try:
                self._queue.put_nowait(item)
            except Full:
                pass

    def _run(self) -> None:
        try:
            import pygame  # type: ignore
        except Exception as exc:
            log("display", f"pygame not installed ({exc}) — display disabled")
            return

        try:
            chosen = _setup_display(self._display)
            os.environ.setdefault("SDL_MOUSE_TOUCH_EVENTS", "1")
            pygame.init()
            pygame.display.set_caption("Revo Display")
            screen = _open_screen(pygame, fullscreen=self._fullscreen)
            sw, sh = screen.get_size()
            cw, ch = _canvas_size(sw, sh, self._rotate)
            canvas = pygame.Surface((cw, ch))
            clock = pygame.time.Clock()
            log("display", f"ready DISPLAY={chosen} screen={sw}x{sh} canvas={cw}x{ch} rotate={self._rotate}")
            self._ready.set()

            dirty = True
            if self._default_wallpaper:
                dirty = self._draw_wallpaper(pygame, canvas, self._default_wallpaper)
            if not dirty:
                self._draw_text(pygame, canvas, "Display ready", subtitle="Waiting for browser command")
                dirty = True

            while not self._stop.is_set():
                for event in pygame.event.get():
                    if event.type == pygame.QUIT:
                        self._stop.set()
                    elif event.type == pygame.KEYDOWN and event.key == pygame.K_ESCAPE:
                        self._stop.set()

                # Drain all pending commands and apply the newest state changes.
                while True:
                    try:
                        item = self._queue.get_nowait()
                    except Empty:
                        break
                    cmd = item.get("cmd")
                    if cmd == "stop":
                        self._stop.set()
                        break
                    if cmd == "text":
                        self._draw_text(pygame, canvas, str(item.get("text") or ""))
                        dirty = True
                    elif cmd == "wallpaper":
                        dirty = self._draw_wallpaper(pygame, canvas, str(item.get("image") or ""))
                    elif cmd == "clear":
                        if self._default_wallpaper:
                            dirty = self._draw_wallpaper(pygame, canvas, self._default_wallpaper)
                        else:
                            canvas.fill(BG)
                            dirty = True

                if dirty:
                    _present_canvas(pygame, screen, canvas, self._rotate)
                    pygame.display.flip()
                    dirty = False

                clock.tick(self._fps)
        except Exception as exc:
            log("display", f"display worker error: {exc}")
        finally:
            try:
                pygame.quit()  # type: ignore[name-defined]
            except Exception:
                pass
            log("display", "stopped")

    def _draw_wallpaper(self, pygame, canvas, raw: str) -> bool:
        path = self._resolve_image_path(raw)
        if path is None:
            log("display", f"wallpaper not found: {raw!r}")
            self._draw_text(
                pygame,
                canvas,
                "Image not found",
                subtitle=str(raw),
                color=ERROR,
            )
            return True
        try:
            image = pygame.image.load(str(path))
            if image.get_alpha() is not None:
                image = image.convert_alpha()
            else:
                image = image.convert()
            iw, ih = image.get_size()
            cw, ch = canvas.get_size()
            scale = min(cw / iw, ch / ih)
            new_size = (max(1, int(iw * scale)), max(1, int(ih * scale)))
            image = pygame.transform.smoothscale(image, new_size)
            rect = image.get_rect(center=(cw // 2, ch // 2))
            canvas.fill((0, 0, 0))
            canvas.blit(image, rect)
            log("display", f"wallpaper={path.name}")
            return True
        except Exception as exc:
            log("display", f"wallpaper load error {path}: {exc}")
            self._draw_text(pygame, canvas, "Image load error", subtitle=path.name, color=ERROR)
            return True

    def _draw_text(
        self,
        pygame,
        canvas,
        text: str,
        subtitle: str = "",
        color: tuple = TEXT,
    ) -> None:
        canvas.fill(BG)
        cw, ch = canvas.get_size()
        margin = max(cw // 12, 32)
        panel = pygame.Rect(margin, margin, cw - margin * 2, ch - margin * 2)
        pygame.draw.rect(canvas, PANEL_BG, panel, border_radius=28)
        pygame.draw.rect(canvas, MUTED, panel, width=3, border_radius=28)

        title_font = _load_font(pygame, max(min(cw, ch) // 9, 42), bold=True)
        body_font = _load_font(pygame, max(min(cw, ch) // 18, 26), bold=False)
        max_w = panel.width - margin

        _draw_multiline_centered(
            pygame,
            canvas,
            title_font,
            text,
            color,
            panel.centery - (body_font.get_linesize() if subtitle else 0),
            max_w,
        )
        if subtitle:
            _draw_multiline_centered(
                pygame,
                canvas,
                body_font,
                subtitle,
                MUTED,
                panel.centery + max(title_font.get_linesize(), 60),
                max_w,
            )
        log("display", f"text={_preview(text)!r}")

    def _resolve_image_path(self, raw: str) -> Optional[Path]:
        raw = str(raw or "").strip()
        if not raw:
            return None

        module_dir = Path(__file__).resolve().parent
        candidates: list[Path] = []
        p = Path(raw).expanduser()
        if p.is_absolute():
            candidates.append(p)
        else:
            candidates.append(Path.cwd() / p)
            if self._asset_dir is not None:
                candidates.append(self._asset_dir / p)
                candidates.append(self._asset_dir / p.name)
            candidates.extend([
                module_dir / "display" / p,
                module_dir / "display" / p.name,
                module_dir.parent / "display" / p,
                module_dir.parent / "display" / p.name,
                Path.home() / "Revobots" / "development" / "display" / p,
                Path.home() / "Revobots" / "development" / "display" / p.name,
            ])

        seen: set[str] = set()
        for cand in candidates:
            key = str(cand)
            if key in seen:
                continue
            seen.add(key)
            if cand.is_file():
                return cand
        return None


def _setup_display(display: Optional[str]) -> str:
    """Pick an X11 display when teleop is started from SSH/systemd."""
    if display:
        os.environ["DISPLAY"] = display
    elif not os.environ.get("DISPLAY"):
        for cand in (":0", ":10", ":1"):
            if os.path.exists(f"/tmp/.X11-unix/X{cand.lstrip(':')}"):
                os.environ["DISPLAY"] = cand
                break

    if not os.environ.get("XAUTHORITY"):
        for path in (
            os.path.expanduser("~/.Xauthority"),
            f"/run/user/{os.getuid()}/gdm/Xauthority",
        ):
            if os.path.isfile(path):
                os.environ["XAUTHORITY"] = path
                break

    chosen = os.environ.get("DISPLAY", "")
    if not chosen:
        raise RuntimeError("no DISPLAY set; for the physical monitor use DISPLAY=:0")
    return chosen


def _open_screen(pygame, fullscreen: bool):
    try:
        if fullscreen:
            return pygame.display.set_mode((0, 0), pygame.FULLSCREEN)
        return pygame.display.set_mode((1024, 600), pygame.RESIZABLE)
    except pygame.error as exc:
        if fullscreen:
            log("display", f"fullscreen failed ({exc}); trying windowed mode")
            return pygame.display.set_mode((1024, 600), pygame.RESIZABLE)
        raise


def _load_font(pygame, size: int, bold: bool = False):
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf"
        if bold else "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"
        if bold else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    ]
    for path in candidates:
        if os.path.isfile(path):
            return pygame.font.Font(path, size)
    return pygame.font.SysFont("sans", size, bold=bold)


def _wrap_text(font, text: str, max_width: int) -> list[str]:
    lines: list[str] = []
    for paragraph in str(text or "").splitlines() or [""]:
        words = paragraph.split()
        if not words:
            lines.append("")
            continue
        current = words[0]
        for word in words[1:]:
            trial = f"{current} {word}"
            if font.size(trial)[0] <= max_width:
                current = trial
            else:
                lines.append(current)
                current = word
        lines.append(current)
    return lines or [""]


def _draw_multiline_centered(pygame, surface, font, text: str, color: tuple, center_y: int, max_width: int) -> None:
    lines = _wrap_text(font, text, max_width)
    line_h = font.get_linesize()
    total_h = line_h * len(lines)
    y = center_y - total_h // 2
    for line in lines:
        rendered = font.render(line, True, color)
        rect = rendered.get_rect(center=(surface.get_width() // 2, y + line_h // 2))
        surface.blit(rendered, rect)
        y += line_h


def _canvas_size(screen_w: int, screen_h: int, rotate: int) -> tuple[int, int]:
    if rotate in (90, 270):
        return screen_h, screen_w
    return screen_w, screen_h


def _present_canvas(pygame, screen, canvas, rotate: int) -> None:
    screen.fill(BG)
    if rotate == 0:
        screen.blit(canvas, (0, 0))
        return
    rotated = pygame.transform.rotate(canvas, -rotate)
    if rotated.get_size() != screen.get_size():
        rotated = pygame.transform.smoothscale(rotated, screen.get_size())
    screen.blit(rotated, (0, 0))


def _preview(text: str, limit: int = 60) -> str:
    text = " ".join(str(text).split())
    return text if len(text) <= limit else text[: limit - 3] + "..."
