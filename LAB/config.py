# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations


"""
LAB configuration — single source of truth for every tunable value.

Edit the class body to retune the robot. Secrets (Daily API key, PTZ password)
load from LAB/.env so they never end up in git.

Three UDP ports are bound:
    55999 — motion + camera + head + button (the gamepad's primary port)
    57000 — events: lights, signals, audio volume, talk, music
    57001 — TTS text (`type:"stt"`)

The operator code is unchanged — these match what the gamepad sender writes
when invoked with `--robot ELEPHANT`.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


# ── Camera source definition ──────────────────────────────────────────────────

@dataclass
class CameraConfig:
    """One physical camera (RTSP URL or V4L2 device path)."""
    name:   str
    source: str
    width:  int = 640
    height: int = 480
    fps:    int = 15
    rtsp_transport: str = "tcp"   # only used for RTSP sources

    # Frame-bus fan-out. If True, the capture thread also writes each frame
    # into a /dev/shm region named "lab_<name>" so external processes (e.g.
    # an AI inference worker) can read frames without touching V4L2.
    # See LAB/frame_bus.py for the reader API.
    publish_frames: bool = False

    # Route capture through a GStreamer pipeline that uses Jetson hardware
    # blocks (NVDEC for RTSP H.264, nvv4l2decoder mjpeg=1 for USB MJPEG, VIC
    # for resize/colorspace). The final stage still hands a BGR numpy frame
    # to OpenCV's appsink so the rest of the system is unchanged.
    # Requires OpenCV built with GStreamer support (JetPack's default does).
    hw_decode: bool = False

    # Pixel format requested from V4L2 USB cameras (ignored for RTSP).
    # "MJPG" → motion-JPEG, decoded on the JPEG hardware engine. Lower USB
    #          bandwidth but lossy. Usually only available at certain rates.
    # "YUYV" → raw YUY2 (uncompressed). Higher USB bandwidth, lossless,
    #          no decode needed. Colorspace conversion still runs on VIC.
    # Run `v4l2-ctl --list-formats-ext -d <device>` to see what each cam
    # actually supports at which framerates — cameras only allow discrete
    # combinations, so 'width/height/fps/pixel_format' must all match a row.
    pixel_format: str = "MJPG"

    @property
    def is_rtsp(self) -> bool:
        return self.source.startswith("rtsp://")

    def __post_init__(self) -> None:
        if self.rtsp_transport not in ("tcp", "udp"):
            raise ValueError(
                f"{self.name}: rtsp_transport must be 'tcp' or 'udp', "
                f"got {self.rtsp_transport!r}"
            )
        if self.pixel_format.upper() not in ("MJPG", "YUYV"):
            raise ValueError(
                f"{self.name}: pixel_format must be 'MJPG' or 'YUYV', "
                f"got {self.pixel_format!r}"
            )


# ══════════════════════════════════════════════════════════════════════════════
#  Edit everything below to match your robot
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class LabConfig:

    # ── UDP listener ports ────────────────────────────────────────────────────
    udp_listen_ip:          str = "0.0.0.0"
    udp_motion_port:        int = 55999    # lin_x, ang_z, head, camera, button, robot_lock
    udp_events_port:        int = 57000    # lights, signals, audio, talk, music
    udp_tts_port:           int = 57001    # type:"stt", text

    # ── Source priority arbitration ───────────────────────────────────────────
    local_dongle_priority:  int  = 100     # lower wins
    remote_gamepad_priority: int = 200
    source_activity_timeout_sec: float = 1.0   # silent > this → other source takes over

    # ── Motion ────────────────────────────────────────────────────────────────
    # /cmd_vel publishing has moved into the segway_ros1 Docker container.
    # We forward (lin_x, ang_z) as JSON UDP to revo_docker_udp_motion_keepalive.py.
    # Host port 56000 → container port 55999 (script's UDP_PORT). The container
    # must be launched with `-p 56000:55999/udp` so this forward reaches it.
    docker_motion_host:     str   = "127.0.0.1"
    docker_motion_port:     int   = 56000   # host-side port mapped to container's 55999
    motion_publish_hz:      int   = 50
    motion_watchdog_sec:    float = 0.30   # stop robot if no command for this long
    ang_z_scale:            float = 0.20   # turning attenuation (matches original)
    brake_threshold:        float = 0.20

    # ── PTZ ───────────────────────────────────────────────────────────────────
    ptz_ip:                 str   = "192.168.10.50"
    ptz_port:               int   = 8000
    ptz_user:               str   = "revolabs"
    ptz_pan_speed:          float = 0.65
    ptz_tilt_speed:         float = 0.55
    ptz_loop_hz:            float = 25.0
    ptz_deadband_sec:       float = 0.05    # don't re-issue same command within this window
    ptz_stop_after_sec:     float = 0.15    # halt if no command for this long
    ptz_home_button:        int   = 8       # gamepad button number that returns to home

    # ── Lights / signals ──────────────────────────────────────────────────────
    blink_period_sec:       float = 0.40    # turn-signal blink period (matches original)
    signal_timeout_sec:     float = 20.0    # turn-signal auto-cancel
    talk_default_duration:  float = 7.0
    all_lights_cooldown_sec: float = 5.0    # absorbs the gamepad's 10× repeat
    all_lights_blink_sec:   float = 5.0     # blink-all-on choreography duration

    # ── Audio ─────────────────────────────────────────────────────────────────
    piper_model:            str   = str(Path.home() / "Revobots" / "piper" / "voices" / "en_GB-northern_english_male-medium.onnx")
    piper_speaker_id:       Optional[int] = None
    music_dir:              str   = str(Path.home() / "Revobots" / "audio")
    music_tracks: dict      = field(default_factory=lambda: {
        1: "REVOBOTS_Anthem_v1.wav",
        2: "REVO_Track_old1.wav",
        3: "REVO_Track_old2.wav",
    })
    startup_volume_pct:     int   = 100
    preferred_sink_patterns: list = field(default_factory=lambda: [
        "ugreen", "u_green", "usb_audio", "usb-audio", "emeet", "alsa_output.usb-",
    ])
    preferred_source_patterns: list = field(default_factory=lambda: [
        "ugreen", "u_green", "usb_audio", "usb-audio", "emeet", "alsa_input.usb-",
    ])

    # ── Cameras ───────────────────────────────────────────────────────────────
    #
    # Set publish_frames=True on any camera whose frames you want to expose
    # to external processes (AI inference, debug viewers, etc.) via shared
    # memory. The region appears as /dev/shm/lab_<name>. See LAB/frame_bus.py.
    cameras: list = field(default_factory=lambda: [
        # Orbital is on the network — RTSP H.264 decoded by NVDEC
        CameraConfig(
            name="orbital",
    	    source="rtsp://admin:revolabs123%40@192.168.10.52:554/cam/realmonitor?channel=1&subtype=1",
	    width=640, height=480, fps=15, rtsp_transport="tcp",
            hw_decode=True,
        ),
        # Front AI camera is on USB — YUYV at 30fps (the only rate this cam
        # advertises at 640x480 for YUYV). VIC handles the YUY2→BGRx convert,
        # so capture stays mostly on hardware. The recorder and streamer
        # downsample to record_fps/stream_fps automatically via their tick
        # loops; the spare frames are simply overwritten in the 1-slot buffer.
        CameraConfig(
            name="rear",
    	    source="rtsp://admin:revolabs123%40@192.168.10.51:554/cam/realmonitor?channel=1&subtype=1",
	    width=640, height=480, fps=15, rtsp_transport="udp",
            hw_decode=True,
        ),
        CameraConfig(
            name="ai",
            source="/dev/video0",
            width=640, height=480, fps=30,
            pixel_format="YUYV",
            publish_frames=True,
            hw_decode=True,
        ),
        # CameraConfig(
        #     name="driver",
    	#     source="rtsp://admin:revolabs123%40@192.168.10.50:554/cam/realmonitor?channel=1&subtype=1",
	    # width=640, height=480, fps=15, rtsp_transport="udp",
        #     hw_decode=True,
        # ),
        
    ])

    # The gamepad sends these camera names. Map them to our internal names above.
    # Anything not listed here is passed through unchanged.
    camera_name_aliases: dict = field(default_factory=lambda: {
    # Orbital camera
    "pilot":    "orbital",
    "orbital":  "orbital",

    # AI camera
    "front":    "ai",
    "ai-front": "ai",
    "aifront":  "ai",
    "ai":       "ai",

    # Driver camera
    "driver":   "driver",

    # Rear camera
    "rear":     "rear",
    "back":     "rear",
})

    # ── Daily streaming ───────────────────────────────────────────────────────
    daily_room_url:         str   = "https://revolabs.daily.co/iwu_scout_1_cam"
    daily_room_name:        str   = "iwu_scout_1_cam"
    stream_width:           int   = 640
    stream_height:          int   = 480
    stream_fps:             int   = 15
    initial_main_source:    str   = "ai"   # which camera is shown on startup

    # ── PiP thumbnails on the main stream ─────────────────────────────────────
    pip_enabled:            bool  = True
    pip_left_source:        str   = "orbital"     # pilot on left
    pip_right_source:       str   = "rear"     # rear on right
    pip_width:              int   = 160
    pip_height:             int   = 120
    pip_margin:             int   = 10
    pip_gap:                int   = 8
    pip_stale_sec:          float = 0.60          # drop thumbnails older than this
    pip_show_label:         bool  = False

    # Speed/camera-name badges
    overlay_speed_badge:    bool  = True
    overlay_camera_name:    bool  = True
    overlay_timestamp:      bool  = False

    # ── Microphone (RTSP audio from orbital → Daily virtual mic) ──────────────
    mic_rtsp_url:           str   = "rtsp://revolabs:revolabs123%40@192.168.10.50:554/h264Preview_01_sub"
    mic_rtsp_transport:     str   = "tcp"
    mic_sample_rate:        int   = 16000
    mic_channels:           int   = 1
    mic_frame_ms:           int   = 5

    # ── Sensors (direct UART, no journalctl, no ROS2) ─────────────────────────
    imu_port_hint:          str   = "/dev/ttyCH341USB3"
    imu_baud:               int   = 9600
    gps_udp_host:           str   = "127.0.0.1"
    gps_udp_port:           int   = 57002

    # ── Recording ─────────────────────────────────────────────────────────────
    cache_dir:              str   = os.path.expanduser("~/.cache/scout/lab")
    record_camera_name:     str   = "ai"       # which camera goes into the MP4
    record_width:           int   = 640
    record_height:          int   = 480
    record_fps:             int   = 15            # same as stream — frame-aligned
    record_video_bitrate:   str   = "1500k"       # ffmpeg -b:v
    # Encoder preference order — first that opens cleanly wins. Auto-probed
    # at recording start.
    #   gst_nvenc → GStreamer + nvv4l2h264enc — Jetson HW encoder (NVENC).
    #               Requires python3-gi + gstreamer1.0-plugins-nvvideo4linux2.
    #   libx264   → ffmpeg + libx264 — CPU fallback. Always works.
    # h264_v4l2m2m is intentionally NOT listed: NVIDIA's L4T ffmpeg ships
    # decoder integration but no working v4l2-m2m encoder, so it fails with
    # "Could not find a valid device". HW encode is reachable only via GStreamer.
    record_encoder_preference: list = field(default_factory=lambda: [
        "gst_nvenc",       # Jetson HW encoder via nvv4l2h264enc
        "libx264",         # CPU fallback
    ])

    # ── Local dongle (evdev) ──────────────────────────────────────────────────
    local_dongle_enabled:   bool  = True
    # Device-name fragments that identify a real driving controller (not a mouse/kb).
    local_dongle_name_hints: list = field(default_factory=lambda: [
        "8bitdo", "ultimate", "tgz", "cx 2.4g",
    ])

    # ── Secrets (populated by load_secrets() from LAB/.env) ───────────────────
    daily_api_key:          str   = ""
    ptz_password:           str   = ""

    # ── Loader ────────────────────────────────────────────────────────────────

    @classmethod
    def load_secrets(cls, env_file: Optional[str] = None) -> "LabConfig":
        cfg = cls()

        if env_file is None:
            env_file = str(Path(__file__).parent / ".env")

        secrets = _read_env_file(env_file)

        cfg.daily_api_key = secrets.get("DAILY_API_KEY", "")
        cfg.ptz_password  = secrets.get("PTZ_PASSWORD",  "")

        if secrets:
            print(f"[config] secrets loaded from {env_file}")
        else:
            print(f"[config] no .env at {env_file} — secrets empty")

        return cfg


# ── private helper ────────────────────────────────────────────────────────────

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
        print(f"[config] error reading {path}: {exc}")
    return out