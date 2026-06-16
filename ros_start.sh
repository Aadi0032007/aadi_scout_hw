#!/bin/bash
# revo_scoutlab_teleop.sh
# Launches agv_pro_bringup in the background, then runs LAB/teleop.py
# in the foreground. Ctrl-C (or any exit) cleans up bringup.

set -e

LAB_DIR="${LAB_DIR:-$HOME/Revobots/aditya/aadi_scout_hw/LAB}"
BRINGUP_PID=""

# ── 0. Pre-Flight Cleanup ────────────────────────────────────────────────────
# Ensure no zombie nodes are holding the Lidar or Motor serial ports open
echo "[teleop_start] Checking for and killing lingering ROS 2 processes..."
pkill -f "ros2 launch agv_pro_bringup" || true
pkill -f "agv_pro_node" || true
pkill -f "lslidar_driver_node" || true

# NEW: Nuke the leftover FastDDS shared memory lock files
echo "[teleop_start] Clearing FastDDS shared memory locks..."
rm -rf /dev/shm/fastrtps* || true

sleep 1 # Give the kernel a second to release the /dev/ hardware locks

cleanup() {
    # Disable the trap so we don't recurse if cleanup itself errors
    trap - SIGINT SIGTERM EXIT

    echo
    echo "[teleop_start] Shutting down ROS 2 bringup..."
    
    if [ -n "$BRINGUP_PID" ] && kill -0 "$BRINGUP_PID" 2>/dev/null; then
        kill -TERM "$BRINGUP_PID" 2>/dev/null || true
    fi

    # Aggressively kill the child nodes so they release the lidar/serial ports
    pkill -f "ros2 launch agv_pro_bringup" || true
    pkill -f "agv_pro_node" || true
    pkill -f "lslidar_driver_node" || true

    echo "[teleop_start] Cleanup complete."
}

trap cleanup SIGINT SIGTERM EXIT

# ── 1. Source ROS 2 ──────────────────────────────────────────────────────────
echo "[teleop_start] Sourcing ROS 2 Humble and local workspace..."
source /opt/ros/humble/setup.bash
source "$HOME/agv_pro_ros2/install/local_setup.bash"

# ── 2. Launch agv_pro_bringup in the background ──────────────────────────────
echo "[teleop_start] Launching agv_pro_bringup..."
ros2 launch agv_pro_bringup agv_pro_bringup.launch.py &
BRINGUP_PID=$!

# Give bringup time to bring up /cmd_vel, TF, etc.
echo "[teleop_start] Waiting 5s for ROS nodes to initialize..."
sleep 5

# Fail fast if bringup died during init
if ! kill -0 "$BRINGUP_PID" 2>/dev/null; then
    echo "[teleop_start] ERROR: agv_pro_bringup exited during startup."
    exit 1
fi

# ── 3. Run teleop in the foreground ──────────────────────────────────────────
echo "[teleop_start] Starting LAB/teleop.py..."
cd "$LAB_DIR"

# IMPORTANT: Removed 'exec' here so the bash script waits for python to finish.
# This ensures the EXIT trap is actually triggered when you press Ctrl-C.
python3 teleop.py