# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations


# -*- coding: utf-8 -*-
"""LabConfig — REDESIGN v3 (browser + bridge-server integration).

Changes vs previous version:

Removed:
    - tcp_events_port          (was port 57000; events now arrive over the
                                 fleet WebSocket that the robot connects OUT
                                 to — see bridge_ws.py)

Added:
    - robot_id                 (loaded from ROBOT_ID / AZURE_DEVICE_ID in env)
    - fleet_ws_url_template    (wss://streams.revobots.ai/api/ws/robot/{robot_id})
    - fleet_register_url       (https://streams.revobots.ai/api/robots/register)
    - heartbeat_interval_sec   (default 60s — matches util_robot_heartbeat.py)
    - fleet_pilot_camera       (which camera the browser opens by default)
    - fleet_camera_names       (list of camera names as seen by the fleet;
                                 separate from the internal CameraConfig list)
    - tailscale_ip_fallback    (used if `tailscale ip -4` fails at register)

Unchanged:
    - udp_motion_port = 55999  (bridge-server writes here; browser writes
                                 events over WS; local dongle stays in-proc)

Wire model after this redesign:
    UDP :55999                     → motion only (bridge OR local dongle)
    WSS → streams.revobots.ai/...  → all non-motion events from browser
    HTTPS POST /api/robots/register → fleet visibility, every 60s
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class CameraConfig:
    name:               str
    source:             str
    width:              int  = 640
    height:             int  = 480
    fps:                int  = 15
    rtsp_transport:     str  = "tcp"
    audio:              bool = True
    pixel_format:       str  = "MJPG"
    hw_decode:          bool = False
    publish_frames:     bool = False
    stream_only:        bool = False   # RTSP passthrough only, never opened locally

    @property
    def is_rtsp(self) -> bool:
        return isinstance(self.source, str) and self.source.startswith("rtsp://")

    def __post_init__(self) -> None:
        if self.rtsp_transport not in ("tcp", "udp"):
            raise ValueError(
                f"{self.name}: rtsp_transport must be 'tcp' or 'udp', "
                f"got {self.rtsp_transport!r}"
            )


@dataclass
class LabConfig:

    # ── Wire protocol ────────────────────────────────────────────────────────
    udp_listen_ip:          str = "0.0.0.0"
    udp_motion_port:        int = 55999      # trimmed motion payload, 50 Hz, unacked
    # tcp_events_port removed — events now arrive over the fleet WebSocket.

    # ── Fleet WebSocket + heartbeat ─────────────────────────────────────────
    # robot_id is overridden from env at load (ROBOT_ID or AZURE_DEVICE_ID).
    robot_id:               str = "iwu-scout-001"
    fleet_ws_url_template:  str = "wss://streams.revobots.ai/api/ws/robot/{robot_id}"
    fleet_register_url:     str = "https://streams.revobots.ai/api/robots/register"
    heartbeat_interval_sec: int = 60
    fleet_pilot_camera:     str = "iwu-scout-001-drive"
    fleet_camera_names:     list = field(default_factory=lambda: [
        "iwu-scout-001-orbital",
        "iwu-scout-001-drive",
        "iwu-scout-001-rear",
        "iwu-scout-001-ai",
        "iwu-scout-001-floor",
    ])
    tailscale_ip_fallback:  str = "100.109.21.91"

    # ── Source priority arbitration ──────────────────────────────────────────
    local_dongle_priority:  int  = 100
    remote_gamepad_priority: int = 200
    source_activity_timeout_sec: float = 1.0

    # ── Motion ───────────────────────────────────────────────────────────────
    docker_motion_host:     str   = "127.0.0.1"
    docker_motion_port:     int   = 56000
    motion_publish_hz:      int   = 50
    motion_watchdog_sec:    float = 0.30
    ang_z_scale:            float = 0.20
    brake_threshold:        float = 0.20

    # ── PTZ ──────────────────────────────────────────────────────────────────
    ptz_ip:                 str   = "192.168.10.50"
    ptz_port:               int   = 80
    ptz_user:               str   = "admin"
    ptz_pan_speed:          float = 0.65
    ptz_tilt_speed:         float = 0.55
    ptz_loop_hz:            float = 25.0
    ptz_deadband_sec:       float = 0.05
    ptz_stop_after_sec:     float = 0.15

    # ── Lights / signals ─────────────────────────────────────────────────────
    blink_period_sec:       float = 0.40
    signal_timeout_sec:     float = 5.0
    talk_default_duration:  float = 7.0
    all_lights_cooldown_sec: float = 5.0
    all_lights_blink_sec:   float = 5.0

    # ── Audio ────────────────────────────────────────────────────────────────
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
        "c-media",
        "usb",
        "usb_audio_device",
    ])
    preferred_source_patterns: list = field(default_factory=lambda: [
        "ugreen", "u_green", "usb_audio", "usb-audio", "emeet", "alsa_input.usb-",
    ])

    # ── Display / touchscreen ───────────────────────────────────────────────
    display_enabled:          bool = False
    display_name:             Optional[str] = None   # e.g. ":0"; auto-detect if None
    display_asset_dir:        str  = str(Path.home() / "Revobots" / "display")
    display_default_wallpaper: str = "REVOBOTS_LOGO_1.png"
    display_rotate:           int  = 90      # portrait-mounted monitor on landscape framebuffer
    display_fullscreen:       bool = True
    display_fps:              int  = 30

    # ── Cameras ──────────────────────────────────────────────────────────────
    cameras: list = field(default_factory=lambda: [
        CameraConfig(
            name="orbital",
            source="rtsp://admin:revolabs123%40@192.168.10.50:554/cam/realmonitor?channel=1&subtype=1",
            width=640, height=480, fps=15, rtsp_transport="udp",
            audio=True,
            stream_only=True,
        ),
        CameraConfig(
            name="drive",
            source="rtsp://admin:revolabs123%40@192.168.10.51:554/cam/realmonitor?channel=1&subtype=1",
            width=640, height=480, fps=15, rtsp_transport="udp",
            audio=False,
            stream_only=True,
        ),
        CameraConfig(
            name="rear",
            source="rtsp://admin:revolabs123%40@192.168.10.52:554/cam/realmonitor?channel=1&subtype=1",
            width=640, height=480, fps=15, rtsp_transport="udp",
            audio=False,
            stream_only=True,
        ),
        CameraConfig(
            name="ai",
            source="/dev/video2",
            width=640, height=480, fps=30,
            pixel_format="YUYV",
            publish_frames=True,
            hw_decode=True,
        ),
    ])

    # ── GstRtspServer (in-process, port 8556) ────────────────────────────────
    gst_rtsp_bind:              str  = "0.0.0.0"
    gst_rtsp_port:              int  = 8556
    gst_hw_encode:              bool = False
    usb_stream_mount:           str  = "ai"
    usb_stream_bitrate_kbps:    int  = 1500
    usb_stream_bitrate_bps:     int  = 1500000
    rtsp_close_wait_interval_sec: int = 30
    rtsp_close_wait_max:          int = 50

    # ── Telemetry ────────────────────────────────────────────────────────────
    udp_telemetry_host:     str   = "100.94.48.1"
    udp_telemetry_port:     int   = 57100
    udp_telemetry_hz:       int   = 5

    # ── Sensors ──────────────────────────────────────────────────────────────
    imu_port_hint:          str   = "/dev/ttyCH341USB3"
    imu_baud:               int   = 9600
    gps_udp_host:           str   = "127.0.0.1"
    gps_udp_port:           int   = 57002

    # ── TEMPerHUM ────────────────────────────────────────────────────────────
    temphum_enabled:        bool  = True
    temphum_vid:            str   = "3553"
    temphum_pid:            str   = "A001"
    temphum_poll_sec:       float = 2.0
    temphum_stale_after_sec: float = 10.0

    # ── Battery / chassis status (from aadi_segway_can_wrapper) ─────────────
    # Wrapper publishes JSON at 1 Hz over UDP; BatteryReader binds this
    # port and updates its snapshot dict. Same public keys as before
    # (bat_soc / bat_charging / bat_vol / bat_current / bat_temp / age_sec),
    # plus new chassis_mode / host_err for future dashboard use.
    battery_enabled:         bool  = True
    battery_udp_host:        str   = "127.0.0.1"
    battery_udp_port:        int   = 56500
    battery_stale_after_sec: float = 30.0

    # ── Lidar ────────────────────────────────────────────────────────────────
    lidar_enabled:          bool  = True
    lidar_symlink:          str   = "/dev/rplidar_s2"
    lidar_usb_serial:       str   = "4afc166e056ff011aec34b9b1045c30f"
    lidar_port:             str   = ""
    lidar_baud:             int   = 1_000_000
    lidar_poll_hz:          float = 2.0
    lidar_scan_timeout_sec: float = 3.0
    lidar_range_min_m:      float = 0.05
    lidar_range_max_m:      float = 18.0
    lidar_min_quality:      int   = 0
    lidar_front_min_deg:    float = -45.0
    lidar_front_max_deg:    float = 45.0
    lidar_left_min_deg:     float = 45.0
    lidar_left_max_deg:     float = 135.0
    lidar_right_min_deg:    float = -135.0
    lidar_right_max_deg:    float = -45.0
    lidar_bubble_front_m:   float = 0.10
    lidar_bubble_left_m:    float = 0.10
    lidar_bubble_right_m:   float = 0.10
    lidar_stale_after_sec:  float = 2.0
    # Initial value of the lidar safety brake. Runtime-togglable by the
    # browser via WS bubble_mode → motion.set_lidar_block_enabled().
    lidar_safety_brake:     bool  = False

    # ── Recording ────────────────────────────────────────────────────────────
    cache_dir:              str   = os.path.expanduser("~/.cache/scout/lab")
    record_camera_name:     str   = "ai"
    record_width:           int   = 640
    record_height:          int   = 480
    record_fps:             int   = 15
    record_video_bitrate:   str   = "1500k"
    record_encoder_preference: list = field(default_factory=lambda: [
        "gst_nvenc",
        "libx264",
    ])

    # ── Local dongle ─────────────────────────────────────────────────────────
    local_dongle_enabled:   bool  = True
    local_dongle_name_hints: list = field(default_factory=lambda: [
        "8bitdo", "ultimate", "tgz", "cx 2.4g",
    ])

    # ── Secrets ──────────────────────────────────────────────────────────────
    ptz_password:           str   = ""
    camera_password:        str   = ""

    @classmethod
    def load_secrets(cls, env_file: Optional[str] = None) -> "LabConfig":
        cfg = cls()
        if env_file is None:
            env_file = str(Path(__file__).parent / ".env")
        secrets = _read_env_file(env_file)
        cfg.ptz_password    = secrets.get("PTZ_PASSWORD", "")
        cfg.camera_password = secrets.get("CAMERA_PASSWORD", "")
        # robot_id: prefer explicit ROBOT_ID, fall back to AZURE_DEVICE_ID,
        # then keep the dataclass default. Same precedence as the utils.
        cfg.robot_id        = (secrets.get("ROBOT_ID")
                               or secrets.get("AZURE_DEVICE_ID")
                               or cfg.robot_id)
        if secrets:
            print(f"[config] secrets loaded from {env_file}")
        else:
            print(f"[config] no .env at {env_file} — secrets empty")
        return cfg


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