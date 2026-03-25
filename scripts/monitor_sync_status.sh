#!/usr/bin/env bash
# Terminal-friendly sync monitoring.
#
# Usage:
#   ./scripts/monitor_sync_status.sh              # snapshot every MONITOR_INTERVAL (default 8s): latest batches + log tail
#   ./scripts/monitor_sync_status.sh stock        # same, but stock_sync log
#   ./scripts/monitor_sync_status.sh follow       # tail -f today's catalog log (live stream)
#   ./scripts/monitor_sync_status.sh follow stock # tail -f today's stock log
#
# Env:
#   MONITOR_INTERVAL   seconds between snapshots (default 8)
#   MONITOR_LOG_LINES  tail lines per snapshot (default 35)

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

if [[ -n "${PYTHON:-}" && -x "${PYTHON}" ]]; then
  :
elif [[ -x "${ROOT}/venv/bin/python" ]]; then
  PYTHON="${ROOT}/venv/bin/python"
else
  PYTHON="$(command -v python3 || true)"
fi

INTERVAL="${MONITOR_INTERVAL:-8}"
TAIL_LINES="${MONITOR_LOG_LINES:-35}"
TODAY="$(date '+%Y%m%d')"
MODE="catalog"
ACTION="refresh"

for arg in "$@"; do
  case "${arg}" in
    follow|f|-f) ACTION="follow" ;;
    stock|s) MODE="stock" ;;
    catalog|c) MODE="catalog" ;;
  esac
done

if [[ "${MODE}" == "stock" ]]; then
  LOG_FILE="${ROOT}/logs/stock_sync_${TODAY}.log"
  LABEL="stock"
else
  LOG_FILE="${ROOT}/logs/catalog_sync_${TODAY}.log"
  LABEL="catalog"
fi

if [[ "${ACTION}" == "follow" ]]; then
  echo "[monitor] Following ${LABEL} log (Ctrl+C to stop): ${LOG_FILE}"
  mkdir -p "${ROOT}/logs"
  touch "${LOG_FILE}"
  exec tail -f "${LOG_FILE}"
fi

print_batches() {
  if [[ -z "${PYTHON}" || ! -x "${PYTHON}" ]]; then
    echo "(no python — skipping batch query)"
    return 0
  fi
  if [[ ! -f "${ROOT}/.env" ]]; then
    echo "(no .env — skipping batch query)"
    return 0
  fi
  "${PYTHON}" - <<'PY' 2>/dev/null || echo "(batch query failed — check .env / network)"
import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(".env")
url, key = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY")
if not url or not key:
    print("(SUPABASE_URL / SUPABASE_SERVICE_KEY missing)")
    raise SystemExit(0)
sb = create_client(url, key)

def latest(table):
    r = (
        sb.table(table)
        .select("import_batch_id,imported_at")
        .order("imported_at", desc=True)
        .limit(1)
        .execute()
    )
    row = (r.data or [None])[0]
    return row or {}

for t in ("staging_moovies_raw", "staging_lasgo_raw"):
    x = latest(t)
    bid = x.get("import_batch_id")
    ts = x.get("imported_at")
    short = t.replace("staging_", "").replace("_raw", "")
    print(f"  {short:12}  batch={bid}  imported_at={ts}")
PY
}

echo "[monitor] ${LABEL} log: ${LOG_FILE}"
echo "[monitor] Refresh every ${INTERVAL}s (Ctrl+C to stop). For a live stream: $0 follow ${MODE}"
echo ""

mkdir -p "${ROOT}/logs"
touch "${LOG_FILE}"

while true; do
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ $(date '+%Y-%m-%d %H:%M:%S')"
  echo "Latest raw import batches (Supabase):"
  print_batches
  echo ""
  echo "Last ${TAIL_LINES} lines of ${LOG_FILE}:"
  echo "────────────────────────────────────────"
  tail -n "${TAIL_LINES}" "${LOG_FILE}" 2>/dev/null || true
  echo "────────────────────────────────────────"
  echo ""
  sleep "${INTERVAL}"
done
