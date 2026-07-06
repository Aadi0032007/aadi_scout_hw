#!/bin/bash
# gps_rtk.sh
# Combined supervisor for LAB/utils/gps_mux.py + Point One RTK client.
#
# gps_mux owns /dev/um982_gps and exposes:
#   • a PTY at /tmp/scoutlab_gps_pty   → Polaris talks here
#   • UDP   127.0.0.1:57002             → teleop GpsReader listens here
#
# If either child exits, the survivor is killed and this script exits.
# That makes systemd Restart=always (or a manual re-run) bring back a
# known-good pair instead of leaving one half running.

set -euo pipefail

# ── Lockfile (single instance) ───────────────────────────────────────────────
LOCK_FILE="/tmp/scoutlab_gps_rtk.lock"
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "[gps_rtk] Another GPS+RTK supervisor is running. Exiting."
  exit 1
fi

# ── Config ───────────────────────────────────────────────────────────────────
LAB_DIR="${LAB_DIR:-$HOME/aditya/aadi_scout_hw}"
ENV_FILE="${ENV_FILE:-${LAB_DIR}/LAB/.env}"
if [ -f "${ENV_FILE}" ]; then
  set -a
  source "${ENV_FILE}"
  set +a
fi
GPS_MUX_PY="${LAB_DIR}/LAB/utils/gps_mux.py"
PTY_PATH="${PTY_PATH:-/tmp/scoutlab_gps_pty}"
PYTHON="${PYTHON:-/usr/bin/python3}"

POINTONE_API_KEY="${POINTONE_API_KEY:?POINTONE_API_KEY not set (source LAB/.env first)}"
POLARIS_UNIQUE_ID="${POLARIS_UNIQUE_ID:?POLARIS_UNIQUE_ID not set}"
RECEIVER_SERIAL_BAUD="${RECEIVER_SERIAL_BAUD:-115200}"
POLARIS_HOSTNAME="${POLARIS_HOSTNAME:-virtualrtk.pointonenav.com}"

POLARIS_BIN="${POLARIS_BIN:-$HOME/Revobots/Polaris/build/examples/serial_port_client}"

MUX_PID=""
RTK_PID=""

# ── Cleanup: kill both children when anything exits ──────────────────────────
cleanup() {
  trap - SIGINT SIGTERM EXIT
  echo
  echo "[gps_rtk] stopping children..."

  for pid_var in RTK_PID MUX_PID; do
    pid="${!pid_var:-}"
    if [ -n "${pid}" ] && kill -0 "${pid}" 2>/dev/null; then
      kill -TERM "${pid}" 2>/dev/null || true
    fi
  done

  # Up to 3 seconds graceful
  for _ in 1 2 3; do
    alive=0
    for pid_var in RTK_PID MUX_PID; do
      pid="${!pid_var:-}"
      if [ -n "${pid}" ] && kill -0 "${pid}" 2>/dev/null; then
        alive=1
      fi
    done
    [ "${alive}" -eq 0 ] && break
    sleep 1
  done

  # Anything still alive gets SIGKILL
  for pid_var in RTK_PID MUX_PID; do
    pid="${!pid_var:-}"
    if [ -n "${pid}" ] && kill -0 "${pid}" 2>/dev/null; then
      kill -KILL "${pid}" 2>/dev/null || true
    fi
  done

  echo "[gps_rtk] done"
}
trap cleanup SIGINT SIGTERM EXIT

# ── Sanity checks ────────────────────────────────────────────────────────────
if [ ! -f "${GPS_MUX_PY}" ]; then
  echo "[gps_rtk] not found: ${GPS_MUX_PY}"
  exit 1
fi
if [ ! -x "${POLARIS_BIN}" ]; then
  echo "[gps_rtk] not executable or missing: ${POLARIS_BIN}"
  exit 1
fi

# ── 1. Launch gps_mux ────────────────────────────────────────────────────────
echo "[gps_rtk] launching gps_mux  (${GPS_MUX_PY})"
"${PYTHON}" "${GPS_MUX_PY}" &
MUX_PID=$!

# Wait up to 10s for the PTY symlink
for _ in $(seq 1 20); do
  [ -L "${PTY_PATH}" ] && break
  sleep 0.5
done
if [ ! -L "${PTY_PATH}" ]; then
  echo "[gps_rtk] gps_mux did not create ${PTY_PATH}"
  exit 1
fi
if ! kill -0 "${MUX_PID}" 2>/dev/null; then
  echo "[gps_rtk] gps_mux died during startup"
  exit 1
fi
echo "[gps_rtk] gps_mux ready (PID ${MUX_PID})"

# ── 2. Launch Polaris RTK client ─────────────────────────────────────────────
export GLOG_v="${GLOG_v:-0}"

HELP_TEXT="$(${POLARIS_BIN} --help 2>&1 || true)"
EXTRA_ARGS=()
if echo "${HELP_TEXT}" | grep -q -- " -polaris_hostname "; then
  EXTRA_ARGS+=("--polaris_hostname=${POLARIS_HOSTNAME}")
fi

echo "[gps_rtk] launching Polaris → ${PTY_PATH} @ ${RECEIVER_SERIAL_BAUD}"
"${POLARIS_BIN}" \
  --polaris_api_key="${POINTONE_API_KEY}" \
  --polaris_unique_id="${POLARIS_UNIQUE_ID}" \
  --receiver_serial_port="${PTY_PATH}" \
  --receiver_serial_baud="${RECEIVER_SERIAL_BAUD}" \
  "${EXTRA_ARGS[@]}" &
RTK_PID=$!
echo "[gps_rtk] Polaris started (PID ${RTK_PID})"
echo "[gps_rtk] supervising both children..."

# ── 3. Block until either child exits, then cleanup trap fires ───────────────
set +e
wait -n "${MUX_PID}" "${RTK_PID}"
EXIT_CODE=$?
set -e

echo "[gps_rtk] a child exited (code=${EXIT_CODE}); shutting down"
exit "${EXIT_CODE}"
