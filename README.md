# REVO Scout LAB — Teleop, Recording, and Inference Stack

A unified controller for the REVO Scout AGV (and Elephant-profile gear).
One process (`teleop.py`) owns every host-side subsystem — cameras, motion
forwarding, GPS, IMU, lidar, temp/hum, lights, audio, PTZ, Daily.co
streaming, and on-the-fly MP4+JSONL recording. A second, independent
process (`lab_inference.py`) runs the trained ACT policy and drives the
robot through the same `MotionController` path teleop uses.

The drivetrain itself lives in a separate Docker container
(`segway_ros1`) which runs ROS1 Noetic, the Segway SmartCar node, and a
UDP→`/cmd_vel` keepalive bridge. Host-side code never imports ROS1 — it
just throws JSON over a UDP socket and lets the container do the
ROS-side work.

---

## Table of contents

1. [Top-level architecture](#top-level-architecture)
2. [Teleoperation flow](#teleoperation-flow)
3. [Recording flow](#recording-flow)
4. [Inference flow](#inference-flow)
5. [Cameras and the frame bus](#cameras-and-the-frame-bus)
6. [Motion path (Host → Docker → SmartCar)](#motion-path)
7. [Sensors](#sensors)
8. [GPS + RTK (Polaris) pipeline](#gps--rtk-polaris-pipeline)
9. [PTZ camera control](#ptz-camera-control)
10. [Lights subsystem](#lights-subsystem)
11. [Audio subsystem (Piper TTS + music + PulseAudio)](#audio-subsystem)
12. [Source arbitration (local vs remote gamepad)](#source-arbitration)
13. [Daily.co streaming + overlays](#dailyco-streaming--overlays)
14. [systemd services / startup scripts](#systemd-services--startup-scripts)
15. [Hardware inventory](#hardware-inventory)
16. [Network / IP map](#network--ip-map)
17. [UDP port map](#udp-port-map)
18. [USB device map](#usb-device-map)
19. [Software prerequisites](#software-prerequisites)
20. [Re-enabling commented-out subsystems](#re-enabling-commented-out-subsystems)

---

## Top-level architecture

```
                ┌─────────────────────────────────────────────────────────┐
                │                    REMOTE OPERATOR                       │
                │  (gamepad UI sends JSON UDP packets to robot's host)     │
                └──────────────────────────┬──────────────────────────────┘
                                           │ UDP :55999 :57000 :57001
                                           ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                          JETSON ORIN NX (HOST)                            │
│                                                                           │
│   ┌──────────────┐   ┌──────────────────────────────────────────────┐    │
│   │ local        │   │              teleop.py                        │    │
│   │ gamepad      │──▶│  ┌───────────┐  ┌──────────┐  ┌──────────┐   │    │
│   │ (evdev,      │   │  │ UDP       │  │ source   │  │ session/ │   │    │
│   │  8BitDo USB) │   │  │ listeners │─▶│ arbiter  │─▶│ stream   │   │    │
│   └──────────────┘   │  │ x3        │  │ (lock)   │  │ manager  │   │    │
│                      │  └─────┬─────┘  └──────────┘  └────┬─────┘   │    │
│                      │        ▼                            │         │    │
│                      │   ┌─────────────────────────────┐   │         │    │
│                      │   │  MotionController            │   │         │    │
│                      │   │  • watchdog 300 ms           │   │         │    │
│                      │   │  • ang_z_scale 0.20          │   │         │    │
│                      │   │  • optional lidar gate       │   │         │    │
│                      │   └────────────┬─────────────────┘   │         │    │
│                      │                │ UDP :56000           │         │    │
│                      │                ▼                      ▼         │    │
│                      │  ┌────────────────────────┐   ┌─────────────┐  │    │
│                      │  │  Cameras (V4L2/RTSP)   │──▶│ Stream      │  │    │
│                      │  │  • per-cam threads     │   │ (Daily.co)  │  │    │
│                      │  │  • /dev/shm frame bus  │   └─────────────┘  │    │
│                      │  └───────────┬────────────┘   ┌─────────────┐  │    │
│                      │              └───────────────▶│ Recorder    │  │    │
│                      │                               │ MP4 + JSONL │  │    │
│                      │  ┌───────┐ ┌─────┐ ┌────────┐ └─────────────┘  │    │
│                      │  │ Lidar │ │ IMU │ │ TempHum│                  │    │
│                      │  └───────┘ └─────┘ └────────┘                  │    │
│                      │  ┌───────────────────────────┐                  │    │
│                      │  │ GpsReader  (udp :57002)   │                  │    │
│                      │  └───────────────────────────┘                  │    │
│                      │                                                  │    │
│                      │  (re-enable: lights, audio, PTZ controllers)    │    │
│                      └──────────────────────────────────────────────────┘    │
│                                                                           │
│   ┌────────────────────┐  ┌────────────────────────────────────────┐    │
│   │  gps_mux.py        │  │  Docker container: segway_ros1          │    │
│   │  /dev/um982_gps →  │  │  roscore + SmartCar node + Python       │    │
│   │  /tmp/...pty       │  │  UDP→/cmd_vel keepalive bridge          │    │
│   │  udp :57002        │  │  (container :55999 ← host :56000)       │    │
│   └─────────┬──────────┘  └────────────────────────────────────────┘    │
│             │                                                             │
│   ┌─────────▼──────────┐                                                  │
│   │ Polaris RTK client │                                                  │
│   │ (Point One Nav)    │                                                  │
│   └────────────────────┘                                                  │
│                                                                           │
│   ┌────────────────────┐                                                  │
│   │ lab_inference.py   │  ← parallel process; reads cameras via the       │
│   │ (ACT policy)       │     frame bus, drives via MotionController       │
│   └────────────────────┘                                                  │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Teleoperation flow

```
 ┌─────────────────┐                              ┌─────────────────┐
 │ Local 8BitDo    │                              │ Remote operator │
 │ over USB dongle │                              │  (gamepad UI)   │
 └────────┬────────┘                              └────────┬────────┘
          │ evdev events                                   │ JSON / UDP
          │ on_motion / on_events / on_tts                 │
          ▼                                                ▼
   ┌──────────────────────────────────────────────────────────────┐
   │  teleop.py — three UDP listeners                              │
   │  :55999 motion   :57000 events   :57001 tts                   │
   └──────────┬───────────────┬────────────────┬──────────────────┘
              │               │                │
              ▼               ▼                ▼
        on_motion_packet  on_events_packet  on_tts_packet
              │
              ▼
   ┌──────────────────────────────────────────────────────────────┐
   │ SourceArbiter — local=100 wins over remote=200                │
   │ if quiet > 1.0 s, the other source takes over                 │
   └──────────┬───────────────────────────────────────────────────┘
              │
              ▼
   ┌──────────────────────────────────────────────────────────────┐
   │ parse_lock_state  →  (locked, lock_present)                   │
   │ first_float(pkt, lin_x|ang_z)                                 │
   │ brake = first_float(pkt, "brake") > brake_threshold (0.20)    │
   │ camera switch?  PTZ head?  A+B combo?  button 8 home?         │
   └──────────┬───────────────────────────────────────────────────┘
              │
       ┌──────┼──────────────────────────────────────────────────┐
       │      │                                                   │
       ▼      ▼                                                   ▼
  motion.command(lin, ang, locked, brake)        session_stream_manager
       │                                          • debounced 750 ms
       ▼                                          • unlock → start stream + recorder
  publish loop @ 50 Hz                            • lock   → stop stream + recorder
   • watchdog (300 ms)
   • robot_lock=True → zero out
   • brake=True     → zero out
   • lidar_block_fn → zero out (optional)
   • ang_z *= 0.20
       │
       ▼
  UDP JSON  {"lin_x":..., "ang_z":...}  →  127.0.0.1:56000  (Docker bridge)
```

**Key behaviors**

- The local gamepad is always on. A→B unlocks; B→A locks.
- Source arbitration is by priority *and* recency — whoever sent a packet
  most recently within `source_activity_timeout_sec` (1.0 s) and has the
  lowest priority wins. Local (100) outranks remote (200).
- Lock edges are debounced 750 ms in `SessionAndStreamManager` so a
  bouncy lock packet doesn't thrash the stream / recorder.
- `motion.command()` updates a state struct; the actual publish happens
  in a background loop at `motion_publish_hz=50`. If commands stop
  arriving, the watchdog (`motion_watchdog_sec=0.30`) zeros the output
  automatically — the robot won't run away if the operator drops.

---

## Recording flow

```
 unlock edge (stable for 750 ms)
       │
       ▼
 SessionRecorder.start()
       │
       ▼
 ~/.cache/scout/lab/session_YYYYMMDD_HHMMSS/
       │
       ├── opens encoder (gst_nvenc first, libx264 fallback)
       │
       ├── starts rec-tick thread @ record_fps (15 Hz)
       │     │
       │     ├── pull latest frame from cameras["ai"]   ──┐
       │     ├── motion.published_state()  → lin, ang  ──┤
       │     ├── gps.get() → fix, lat, lon, heading    ──┤
       │     │  (imu.get() commented out today)        ──┤
       │     │                                            │
       │     └─► write video frame  → video.mp4         ──┤
       │         write json line    → data.jsonl ◄───────┘
       │         { "t": ..., "frame": N,
       │           "lin_x": ..., "ang_z": ...,
       │           "gps": {...} }
       │
       ▼
 lock edge → recorder.stop()
       │
       ▼
 session.json finalised (start, end, frame count, encoder used)
```

**Notes**

- The recorder takes `motion.published_state()` (post-gate, post-scale)
  rather than `motion.state()` — what got *sent* to the robot, not what
  the operator *requested*. This is what made the ACT training data
  consistent with inference-time observations.
- `record_camera_name` defaults to `"ai"` so the IL dataset is built on
  the front USB camera, not the PTZ.
- Two encoders are probed at startup: GStreamer `nvv4l2h264enc` (Jetson
  hardware NVENC) is preferred; if `gst_nvenc` doesn't open, ffmpeg
  `libx264` (CPU) takes over. NVIDIA's L4T ffmpeg does *not* ship a
  working `h264_v4l2m2m` encoder, so the only HW path is via GStreamer.

---

## Inference flow

```
 lab_inference.py --policy-path .../checkpoints/080000/pretrained_model \
                  --dataset-repo-id Aadi/scout_dataset_03 \
                  --device cuda --send --temporal-ensemble-coeff 0.01 \
                  --ang-deadband 0.15

 ┌────────────────────────────────────────────────────────────────┐
 │  build_policy_pipeline()                                       │
 │   • PreTrainedConfig.from_pretrained(policy_path)             │
 │   • LeRobotDatasetMetadata(dataset_repo_id)                   │
 │   • make_policy, make_pre_post_processors                     │
 │   • make_default_processors  (symmetry with training)         │
 └────────────────────────────┬──────────────────────────────────┘
                              │
                              ▼
 ┌────────────────────────────────────────────────────────────────┐
 │  LabInference loop @ stream_fps (15 Hz)                        │
 │                                                                 │
 │   read frame from cameras["ai"]                                │
 │      • prefer FrameBusReader  ──▶  /dev/shm/lab_ai             │
 │      • fall back to direct V4L2 (only if teleop is OFF)        │
 │                                                                 │
 │   FrameValidator(640, 480, BGR)                                │
 │      │                                                          │
 │      ▼                                                          │
 │   build_dataset_frame  →  obs dict                             │
 │      │                                                          │
 │      ▼                                                          │
 │   predict_action(obs)  →  raw policy action                    │
 │      │                                                          │
 │      ▼                                                          │
 │   postprocessor + temporal ensemble (coeff 0.01)               │
 │      │                                                          │
 │      ▼                                                          │
 │   (lin_x, ang_z_raw)                                            │
 │      │                                                          │
 │      ▼                                                          │
 │   ang_deadband filter  (|ang_z_raw| < 0.15 → 0)                │
 │      │                                                          │
 │      ▼                                                          │
 │   motion.command(lin_x, ang_z_raw, locked=False, braking=False)│
 │      │                                                          │
 │      ▼                                                          │
 │   MotionController publish loop                                │
 │      • applies ang_z *= 0.20  (same as teleop)                 │
 │      • UDP → 127.0.0.1:56000 → Docker → /cmd_vel               │
 └────────────────────────────────────────────────────────────────┘
```

**Why the frame bus matters here**

A V4L2 USB camera can be opened by exactly **one** process. Without the
frame bus, you'd have to stop teleop to run inference, which means no
overlay/safety/recording during evaluation. With `publish_frames=True`
on the AI camera, teleop is the sole V4L2 owner and inference attaches
read-only via `/dev/shm/lab_ai`. One memcpy (~100 µs per frame) for full
parallelism.

**Inference deployment caveat**

Inference at ~1 Hz on CPU is unusable (policy expects 15 Hz). GPU
deployment on the Orin NX is a hard blocker for live inference — the
Orin NX migration with the `libcudss` symlink fix is what makes
`--device cuda` actually work.

---

## Cameras and the frame bus

Each camera lives in its own daemon thread with a **1-slot latest-frame
buffer**. Readers are always non-blocking — they get the freshest frame
or `(None, None)`. There's no queue, no backpressure, no stale frames
ever sitting around.

```
                   ┌─────────────────────────────────────────┐
                   │           MultiCameraCapture             │
                   ├─────────────────────────────────────────┤
                   │ "ai"      (V4L2 /dev/video2, YUYV, hw)  │
                   │ "orbital" (RTSP, NVDEC, hw)             │ ← uncomment
                   │ "rear"    (RTSP, NVDEC, hw)             │ ← uncomment
                   │ "driver"  (RTSP, NVDEC, hw)             │ ← uncomment
                   └────────┬──────────────────┬─────────────┘
                            │                  │
                ┌───────────┘                  └─────────────────┐
                ▼                                                ▼
        cam.read_latest()                              capture thread (per cam)
        (None blocking)                                   │
                                                          │  hw_decode=True
                                                          ▼
                                            ┌───────────────────────────┐
                                            │ GStreamer pipeline         │
                                            │ • RTSP H.264 → NVDEC →    │
                                            │   nvvidconv → BGR appsink │
                                            │ • USB YUYV → nvvidconv →  │
                                            │   BGR appsink             │
                                            │ • USB MJPG → nvv4l2dec    │
                                            │   mjpeg=1 → nvvidconv     │
                                            └────────┬──────────────────┘
                                                     │
                                                     │ publish_frames=True
                                                     ▼
                                         ┌─────────────────────────────┐
                                         │  FrameBusPublisher           │
                                         │  /dev/shm/lab_<name>         │
                                         │  64-byte header + payload    │
                                         │  seqlock (odd = writing)     │
                                         └────────┬────────────────────┘
                                                  │
                                  ┌───────────────┼───────────────┐
                                  ▼               ▼               ▼
                          FrameBusReader   FrameBusReader   FrameBusReader
                          (inference)      (debug viewer)   (analytics)
```

**Backoff & reconnect**: RTSP drop → exponential backoff up to 10 s,
then reattempt. Cameras that fail to open at startup are simply absent
from the collection — the rest of the system carries on.

**Available cameras (after un-commenting in `config.py`)**

| Internal name | Source                                                              | Transport | Notes                                  |
|---------------|---------------------------------------------------------------------|-----------|----------------------------------------|
| `ai`          | `/dev/video2` (USB)                                                 | YUYV, hw  | Frame-bus publisher. Used for recording + inference. |
| `orbital`     | `rtsp://admin:***@192.168.10.52:554/cam/realmonitor?channel=1&subtype=1` | TCP, hw   | Pilot view (PiP left)                  |
| `rear`        | `rtsp://admin:***@192.168.10.51:554/cam/realmonitor?channel=1&subtype=1` | UDP, hw   | Rear (PiP right)                       |
| `driver`      | `rtsp://admin:***@192.168.10.50:554/cam/realmonitor?channel=1&subtype=1` | UDP, hw   | Driver-cam view (optional)             |

**Operator name aliases**: `pilot`→`orbital`, `front`/`ai-front`/`aifront`→`ai`,
`back`→`rear`. The gamepad UI's camera names are mapped before any internal
lookup.

---

## Motion path

```
 operator / inference                     teleop.py                          Docker container
 ─────────────────────                     ─────────                          ────────────────
  motion.command(lin, ang,                 MotionController                    segway_ros1
   locked, brake)        ───────────┐         │                                ┌─────────────┐
                                    │         │   publish loop @ 50 Hz         │ roscore     │
                                    │         │   gates: watchdog 300 ms,      │             │
                                    │         │   locked, brake, lidar,        │ SmartCar    │
                                    │         │   ang_z *= 0.20                │ node        │
                                    │         │                                │             │
                                    │         ▼                                │ revo_docker │
                                    │   {"lin_x":..,"ang_z":..}                │ _udp_motion │
                                    │   ─────── UDP ───────►  :56000  ────────▶│ _keepalive  │
                                    │                                          │ .py         │
                                    │                                          │   :55999    │
                                    │                                          │             │
                                    │                                          │ publishes   │
                                    │                                          │ /cmd_vel    │
                                    │                                          │ @ 50 Hz     │
                                    │                                          │ HOLD_LAST_  │
                                    │                                          │ CMD_S=0.40  │
                                    │                                          └─────────────┘
                                    │
                          state() ──┘  (raw — for stream overlay only)
                  published_state()    (post-gate — for recorder)
```

**Two snapshot APIs on `MotionController` exist for a reason:**

| API                  | What it returns                | Used by               |
|----------------------|--------------------------------|-----------------------|
| `state()`            | Raw `(lin_x, ang_z, locked, brake)` operator intent | Stream overlay (speed badge) |
| `published_state()`  | Post-watchdog, post-scale, post-lock `(lin, ang)` actually sent | Recorder (so IL data matches deployment-time observation) |

This separation is what allowed the ACT-based imitation learning
dataset to be consistent with inference-time control — training data
captures *what the robot got*, not *what the operator pushed*.

**Why UDP-to-Docker instead of rclpy?** The Segway SmartCar SDK is
ROS1 Noetic; the host runs ROS2 (or no ROS at all, post-migration).
Rather than try to bridge ROS1↔ROS2 with `ros1_bridge`, the SDK got
boxed up into a Docker container with its own roscore. Host code stays
ROS-free, the container handles all ROS-side concerns, and the
interface is a stupid-simple JSON UDP socket.

---

## Sensors

Each sensor is a daemon thread + snapshot-via-`get()`. Identical pattern
across IMU, GPS, lidar, and temp/humidity. Nothing blocks the
command loop.

### IMU (`ImuReader`)
- **Hardware**: WIT / JY901 over UART, typically `/dev/ttyCH341USB3` @ 9600.
- **Protocol**: 11-byte binary frames `0x55 <id> <8 bytes payload> <chk>`.
  Frame IDs: 0x51 accel, 0x52 gyro, 0x53 RPY, 0x54 mag, 0x59 quat.
- **Status**: instantiation commented in `teleop.py:351`. Re-enable when
  the IMU is physically connected.
- Snapshot: `imu.get()` → `{accel:{x,y,z}, gyro:{...}, rpy:{...}, ...}`

### GPS (`GpsReader`)
- **Input**: UDP `:57002` (NOT serial — gps_mux is the producer).
- **Sentences parsed**: `$GxRMC`, `$GxGGA`, `$GxGSA`, `$GxHDT`, plus
  Unicore UM982 `#ADRNAVA` for heading.
- **Snapshot fields**: `fix`, `fix_label`, `lat`, `lon`, `alt`, `speed_kn`,
  `heading_true`, `sats`, `hdop`, `pdop`, `t_unix`.

### Temp/Hum (`TempHumReader`)
- **Hardware**: PCsensor TEMPerHUM, VID `3553`, PID `A001` over hidraw.
- Polled every `temphum_poll_sec` (2 s default).
- Snapshot: `{temp_c, temp_f, humidity, t}`.
- Drives a color-coded overlay on the stream
  (green ≤ 70 °F, yellow ≤ 90 °F, red above; dim-gray after 10 s stale).

### Lidar (`LidarReader`)
- **Hardware**: RPLIDAR S2 / S2L over UART, 1 Mbps, preferred symlink
  `/dev/rplidar_s2` (else `lidar_usb_serial` → udevadm lookup → any
  `/dev/ttyUSBn`).
- **Sectors (deg, robot fwd = 0°, CCW positive)**:
  front [−45, +45], left [+45, +135], right [−135, −45].
- **Bubbles**: 10 cm front/left/right by default.
- **Safety hook**: when `lidar_safety_brake=True`, the
  `is_blocked_forward(lin_x)` method is wired into
  `MotionController.lidar_block_fn`. It zeros output for one publish
  tick when commanded forward AND the front bubble is tripped on a
  fresh scan. Reverse and turning are untouched.

---

## GPS + RTK (Polaris) pipeline

The UM982 receiver has one physical USB port but two consumers that
both need full-duplex access:

1. **Polaris** (Point One Nav RTK client) needs bidirectional access —
   it reads the UM982's NMEA position so the cloud can compute the right
   corrections, then writes RTCM back.
2. **`GpsReader`** in teleop just needs to read the NMEA stream.

`gps_mux.py` is the single owner of `/dev/um982_gps`:

```
                    ┌───────────────────────────────────────────────┐
                    │                gps_mux.py                      │
                    │                                                │
   /dev/um982_gps   │   ┌─────────┐                                 │
   (USB @ 115200) ◀─┼──▶│  serial │  NMEA  ┌──────────────┐        │
                    │   │  port   │───────▶│ UDP fan-out  │ ───────┼──▶ 127.0.0.1:57002
                    │   │         │        │              │        │     (GpsReader in teleop)
                    │   │         │        └──────────────┘        │
                    │   │         │                                 │
                    │   │         │  NMEA  ┌──────────────┐        │
                    │   │         │───────▶│   PTY        │        │
                    │   │         │        │ master/slave │◀───────┼── /tmp/scoutlab_gps_pty
                    │   │         │◀───────│              │        │   (symlink to PTY slave)
                    │   │         │  RTCM  └──────────────┘        │           ▲
                    │   └─────────┘                                 │           │
                    └───────────────────────────────────────────────┘           │
                                                                                 │
                                                  ┌──────────────────────────┐ │
                                                  │ Polaris serial_port_client│◀┘
                                                  │ (Point One Nav)           │
                                                  └──────────────────────────┘
                                                              ▲
                                                              │ HTTPS (POINTONE_API_KEY,
                                                              │        POLARIS_UNIQUE_ID)
                                                              │
                                                       virtualrtk.pointonenav.com
```

NMEA is duplicated to both legs; RTCM only flows PTY→serial. Reconnects
on USB drop. The PTY symlink stays valid across reconnects so Polaris
doesn't need to be restarted.

`gps_rtk.sh` is the supervisor: it launches `gps_mux.py`, waits for the
PTY symlink, then starts Polaris with the right flags. `wait -n` blocks
on either child; if either dies, the trap kills the other so systemd
can restart a clean pair.

---

## PTZ camera control

```
 UDP "head" field (left/right/up/down/center) — every motion packet
        │
        ▼
  PtzController.command(head)
        │
        ▼
   loop @ ptz_loop_hz (25 Hz)
        │
        ├── stop_after_sec (0.15 s) reached → "center" mode
        ├── _direction_to_velocity(desired) → (pan, tilt)
        ├── deadband_sec (0.05 s) gate on ContinuousMove repeats
        │
        ▼
   ONVIF ContinuousMove (Profile S, profile[0].token)
        │
        ▼
   integrate (pan_vel × dt, tilt_vel × dt) into self._pan_pos, _tilt_pos
                                            (dead-reckoning, ~good enough)

 Independent triggers
 ────────────────────
 • set_ptz_unlock_state(True)  on first unlock      → capture_home()
 • A+B combo on gamepad                              → capture_home()
 • speed-cycle on gamepad                            → capture_home()
 • button 8 (lights-ON) pressed                      → goto_home()
       │
       ▼
   goto_home() drives back toward origin until within ptz_return_deadband
   (0.02 rad). Once inside the deadband, snap to origin exactly.
```

PTZ motion is intentionally **independent** of the drivetrain
`robot_lock` — the operator can still look around when the chassis is
locked. PTZ has its own `_ptz_unlocked` flag.

---

## Lights subsystem

USB HID 4-channel relay (VID `0x16c0`, PID `0x05df`).

| Ch | Function on Elephant            |
|----|---------------------------------|
| 1  | Headlights                      |
| 2  | Strobe                          |
| 3  | Halo left + tail left           |
| 4  | Halo right + tail right         |

Three independent animation states that **don't fight each other**:

1. **Steady** — headlights / strobe / halos-as-parking, always reapplied.
2. **Turn signal** — left/right halo blink. Auto-cancels after
   `signal_timeout_sec` (20 s). Headlights and strobe untouched.
3. **All-blink** — used for talk events (blink for ~7 s) and the
   all-lights-ON combo (blink 5 s, then latch all four ON).

```
 robot_lock=True ──▶ force everything off, ignore further commands
 ─────────────────

 event:"lights"   ──▶ _handle_lights_event
   {headlights, parklights, strobe}     all three TRUE → ALL-ON combo (cooldown 5 s)
                                        else apply individually + _apply_steady()

 event:"signals" ──▶ _handle_signals_event
   {left, right}                        sets _left_until / _right_until

 event:"talk"    ──▶ _handle_talk_event
   {duration}                           sets _all_blink_until (no latch)

                          ┌──────────────────────────┐
                          │   blink loop @ 2.5 Hz    │
                          │   (period 0.40 s)        │
                          │                          │
                          │ precedence:              │
                          │   all-blink > signal     │
                          │              > steady    │
                          └──────────────────────────┘
```

HID auto-reconnect on `EIO` / "no such device" / "broken pipe", logged
at most once per second.

---

## Audio subsystem

```
        ┌────────────────────────────────────────────────────┐
        │            AudioController (background)             │
        └────┬──────────────────────┬───────────────────────┬─┘
             │                      │                       │
             ▼                      ▼                       ▼
  PulseAudio sink/source     Piper TTS worker        Music subprocess
  auto-selection             ───────────────         ──────────────
  ────────────────           queue (size 4)          1 process at a time
  pattern match on:          drop-on-full            kill old before new
  • ugreen / u_green         load voice once         player preference:
  • emeet                    synth → in-mem WAV       paplay → pw-play
  • usb_audio                temp file → player       → ffplay → aplay
  • alsa_output.usb-         (paplay → pw-play
                              → aplay)

 set-default-sink ▲                     ▲                          ▲
 set-default-source│                     │                          │
 unmute, vol=100% │      speak(text)     │       play_music(track_num)
                  │                      │
                  │       UDP :57001     │       UDP :57000
                  │     {type:"stt",     │     {event:"music",
                  │      text:"..."}     │      action:"play",
                  │                      │      track:1}
                  │
                  │       UDP :57000  {event:"audio", volume_pct:75}
                  │       set_volume()
```

Status: instantiation commented in `teleop.py:414`. Re-enable when the
USB audio device + speaker are wired up. The dispatch in
`on_events_packet` already guards with `if audio is not None`, so
nothing else needs to change.

Music tracks (from `config.py`):

| Slot | File                          |
|------|-------------------------------|
| 1    | REVOBOTS_Anthem_v1.wav        |
| 2    | REVO_Track_old1.wav           |
| 3    | REVO_Track_old2.wav           |

---

## Source arbitration

Local dongle and remote operator can both be active. Resolution:

```
                priority    quiet timeout (1 s)
  local           100      ────────┐
                                   │
                                   ▼
                          SourceArbiter._update_active_locked()
                                   │
                                   │  pick lowest priority that's been
                                   │  active within timeout
                                   ▼
                              active source
  remote          200      ────────┘
```

If the active source goes quiet for 1 s, the other takes over. If both
are quiet, nothing wins and motion times out (watchdog → zero output).

A→B unlock / B→A lock from the local gamepad is enforced *locally* —
the LocalGamepad thread injects packets with `_local=True` so
`on_motion_packet` tags them as `"local"`.

---

## Daily.co streaming + overlays

```
 ┌──────────────────────────────────────────────────────────────┐
 │                       DailyStream                              │
 │                                                                │
 │   ┌──────────────────┐                                        │
 │   │ encode loop      │ ── pull cam[main] frame                │
 │   │ @ stream_fps 15  │ ── pull cam[pip_left]  (orbital)       │
 │   │                  │ ── pull cam[pip_right] (rear)          │
 │   │                  │ ── composite PiP overlays              │
 │   │                  │ ── draw speed badge   (motion.state)   │
 │   │                  │ ── draw camera name                    │
 │   │                  │ ── draw temp/hum chip (temphum.get)    │
 │   │                  │ ── BGR → RGB                           │
 │   │                  │ ── daily.write_frame()                 │
 │   └────────┬─────────┘                                        │
 │            │                                                   │
 │            ▼                                                   │
 │     virtual camera ────────────────────────────► Daily.co room │
 │     virtual mic   ◀── RTSP audio from orbital (16 kHz mono)   │
 │                                                                │
 │   robot_lock=True ──▶ stop publishing (operator sees frozen   │
 │                       frame as visual lock confirmation)      │
 └──────────────────────────────────────────────────────────────┘
```

PiP, speed badge, camera-name badge, and temp/hum overlay are all
toggleable in `config.py` (`pip_enabled`, `overlay_speed_badge`,
`overlay_camera_name`, `overlay_temphum`).

---

## systemd services / startup scripts

Two bash scripts wrap the stack and are both intended to run under
systemd as `Restart=always` services.

### `ros_start.sh` — `aadi_ros_start_teleop.service`
Brings up the Segway ROS1 Docker stack and then runs `teleop.py` in the
foreground. On exit (Ctrl-C, SIGTERM from systemd, or `teleop.py`
dying), an EXIT trap tears the stack down cleanly:

```
 Start:
   1. wait for serial device (/dev/ttyUSB0 / ttyACM0 / rpserialport)
   2. docker start segway_ros1   (if not running)
   3. configure serial:
        sudo stty -F <dev> 921600 raw -echo
        ln -sf <dev> /dev/rpserialport   (inside container)
   4. roscore --port 11311           (background, in container)
   5. rosrun segwayrmp SmartCar _segwaySmartCarSerial:=<dev>   (background)
   6. python3 revo_docker_udp_motion_keepalive.py              (background)
   7. wait for /ros_set_chassis_enable_cmd_srv
   8. rosservice call /ros_set_chassis_enable_cmd_srv \
        "ros_set_chassis_enable_cmd: true"
   9. cd $LAB_DIR && python3 teleop.py   (foreground, $TELEOP_PID)
       wait $TELEOP_PID

 Stop / trap:
   1. SIGTERM teleop.py → wait
   2. rosservice call ... false   (safe disable)
   3. pkill keepalive / SmartCar / roscore
   4. docker stop segway_ros1
```

### `gps_rtk.sh` — `aadi_gps_rtk.service`
Supervises `gps_mux.py` + Polaris RTK client as a pair. If either dies,
the trap kills the survivor so systemd brings up a known-good pair on
restart. Single instance enforced via `flock` on
`/tmp/scoutlab_gps_rtk.lock`.

Required env (loaded from `LAB/.env`):
- `POINTONE_API_KEY`
- `POLARIS_UNIQUE_ID`
- `POLARIS_HOSTNAME` (default `virtualrtk.pointonenav.com`)
- `RECEIVER_SERIAL_BAUD` (default 115200)
- `POLARIS_BIN` (default `~/Revobots/polaris/build/examples/serial_port_client`)

---

## Hardware inventory

| Component                   | Interface           | Notes                                                |
|-----------------------------|---------------------|------------------------------------------------------|
| Jetson Orin NX (host)       | —                   | JetPack with OpenCV + GStreamer + CUDA               |
| Segway SmartCar chassis     | UART → USB          | Inside `segway_ros1` Docker, baud 921600             |
| RPLIDAR S2 / S2L            | UART → USB          | 1 Mbps, udev symlink `/dev/rplidar_s2`               |
| Unicore UM982 GPS           | UART → USB          | 115200 baud, udev symlink `/dev/um982_gps`           |
| WIT / JY901 IMU             | UART → USB          | 9600 baud, hint `/dev/ttyCH341USB3`                  |
| PCsensor TEMPerHUM          | USB HID             | VID 3553, PID A001                                   |
| 4-ch USB HID relay (lights) | USB HID             | VID 0x16c0, PID 0x05df                               |
| UGREEN / EMEET USB audio    | USB                 | Selected by PulseAudio name pattern                  |
| USB AI camera (front)       | USB / V4L2          | `/dev/video2`, YUYV 640×480@30, NVDEC via GStreamer  |
| Hikvision orbital (pilot)   | RTSP over IP        | `192.168.10.52`, channel 1 subtype 1                 |
| Hikvision rear              | RTSP over IP        | `192.168.10.51`, channel 1 subtype 1                 |
| Hikvision driver            | RTSP over IP        | `192.168.10.50`, channel 1 subtype 1 (optional)      |
| ONVIF PTZ camera            | IP                  | `192.168.10.50:8000`, user `revolabs`                |
| 8BitDo Ultimate USB dongle  | USB HID (evdev)     | Local driving controller                             |

---

## Network / IP map

| Host / device          | Address               | Service                                   |
|------------------------|-----------------------|-------------------------------------------|
| Jetson Orin NX         | (its LAN address)     | All UDP listeners, Daily streaming        |
| PTZ camera             | `192.168.10.50:8000`  | ONVIF                                     |
| Driver cam             | `192.168.10.50:554`   | RTSP (shares IP with PTZ, different port) |
| Rear cam               | `192.168.10.51:554`   | RTSP                                      |
| Orbital cam            | `192.168.10.52:554`   | RTSP                                      |
| Point One Polaris      | `virtualrtk.pointonenav.com` | HTTPS (NTRIP-like)                  |
| Daily.co room          | `https://revolabs.daily.co/iwu_scout_1_cam` | WebRTC                |

---

## UDP port map

| Port  | Bound by              | Producer                                | Purpose                              |
|-------|-----------------------|------------------------------------------|--------------------------------------|
| 55999 | `teleop.py` UDP-motion listener | Remote operator + local gamepad | lin_x, ang_z, head, camera, button, robot_lock |
| 57000 | `teleop.py` UDP-events listener | Operator                          | lights, signals, audio, talk, music  |
| 57001 | `teleop.py` UDP-tts listener    | Operator                          | `{type:"stt", text:"..."}` for Piper |
| 57002 | `GpsReader` (teleop)            | `gps_mux.py`                      | NMEA + #ADRNAVA fan-out              |
| 56000 | (host-side, sent TO)            | `MotionController`                | Forward to Docker `:55999`           |
| 55999 (in container) | `revo_docker_udp_motion_keepalive.py` | host `:56000` (port-mapped) | → /cmd_vel @ 50 Hz |

---

## USB device map

| Path / udev symlink          | Device                  | Used by                            |
|------------------------------|-------------------------|------------------------------------|
| `/dev/video2`                | Front AI camera         | `cameras.py` (V4L2 + GStreamer)    |
| `/dev/ttyUSB0` (or ACM0)     | Segway SmartCar UART    | Docker container (symlinked to `/dev/rpserialport`) |
| `/dev/rplidar_s2`            | RPLIDAR S2/S2L          | `lidar.py`                         |
| `/dev/um982_gps`             | Unicore UM982 GPS       | `gps_mux.py` (exclusive)           |
| `/tmp/scoutlab_gps_pty`      | PTY slave (symlink)     | Polaris RTK client                 |
| `/dev/ttyCH341USB3`          | WIT/JY901 IMU           | `sensors.py` (when re-enabled)     |
| `/dev/hidraw*` (VID 3553)    | TEMPerHUM               | `sensors.TempHumReader`            |
| `/dev/hidraw*` (VID 16c0)    | Lights relay            | `lights.py` (when re-enabled)      |
| `/dev/hidraw*` (UGREEN/EMEET)| USB audio               | `audio.py` (when re-enabled)       |
| 8BitDo evdev node            | Local driving controller | `local_gamepad.py`                |

Recommended udev rules (excerpt — adjust serials to match your hardware):

```
SUBSYSTEM=="tty", ATTRS{idVendor}=="10c4", ATTRS{idProduct}=="ea60", \
    ATTRS{serial}=="UM982-SERIAL", SYMLINK+="um982_gps"

SUBSYSTEM=="tty", ATTRS{idVendor}=="10c4", ATTRS{idProduct}=="ea60", \
    ATTRS{serial}=="4afc166e056ff011aec34b9b1045c30f", SYMLINK+="rplidar_s2"
```

---

## Software prerequisites

### On the host (Jetson Orin NX)
- JetPack with CUDA 12.x and `libcudss` (the `libcudss0-cuda-12` apt
  package plus a symlink into the linker path for PyTorch 2.6.0 wheels).
- Python 3 with: `opencv-python` (built with GStreamer support — the
  JetPack default has it), `pyserial`, `numpy`, `requests`, `hid`,
  `piper-tts`, `onvif-zeep`, `daily-python`, `evdev`, `torch` (CUDA
  build), `lerobot`.
- GStreamer 1.0 + `nvv4l2decoder` + `nvv4l2h264enc` + `nvvidconv`
  (`gstreamer1.0-plugins-nvvideo4linux2`, plus Python bindings
  `python3-gi`).
- PulseAudio (or pipewire-pulse) + `pactl` + at least one of
  `paplay` / `pw-play` / `aplay` / `ffplay`.
- `udevadm`, `ffmpeg`, `flock`, `pty` support, `stty`.
- Polaris client binary at `~/Revobots/polaris/build/examples/serial_port_client`.
- Piper voice ONNX at `~/Revobots/piper/voices/en_GB-northern_english_male-medium.onnx`.
- Music WAVs at `~/Revobots/audio/`.

### `LAB/.env` (not in git)
```
DAILY_API_KEY=...
PTZ_PASSWORD=...
POINTONE_API_KEY=...
POLARIS_UNIQUE_ID=...
```

### Docker (segway_ros1 container)
- ROS1 Noetic
- `segwayrmp` SmartCar package built into `/root/catkin_ws`
- Python 3 venv at `/root/catkin_ws/venv` with `rospy`
- `revo_docker_udp_motion_keepalive.py` at `/root/catkin_ws/`
- Must be started with `-p 56000:55999/udp` to receive host forwards
- Must have access to the chassis serial device (`--device` or
  `-v /dev/ttyUSB0:/dev/ttyUSB0`).

---

## Re-enabling commented-out subsystems

`teleop.py` has marker comments throughout: **`RE-ENABLE-WHEN-HARDWARE-INSTALLED`**.
Currently disabled in code but ready to go:

| Subsystem | Where to uncomment                                  |
|-----------|-----------------------------------------------------|
| `lights`  | imports near top of `teleop.py` + the `LightsController(...)` block (~line 427) |
| `ptz`     | imports near top + the `PtzController(...)` block (~line 437) |
| `audio`   | imports near top + the `AudioController(...)` block (~line 414) |
| `imu`     | the `ImuReader(...)` line (~line 350)              |

For cameras, edit `config.py`:
- `orbital` (RTSP `192.168.10.52`) — uncomment the `CameraConfig(...)` block.
- `rear`    (RTSP `192.168.10.51`) — uncomment.
- `driver`  (RTSP `192.168.10.50`) — uncomment.

All dispatchers (`on_motion_packet`, `on_events_packet`,
`on_tts_packet`) already guard with `if subsystem is not None`, so the
order in which subsystems come back doesn't matter.