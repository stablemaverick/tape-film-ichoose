#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# STOCK SYNC — Commercial-only (no harmonization, no catalog release-date updates)
# ============================================================================
#
# Sequence:
#   00  Fetch supplier files from FTP        (optional, set SKIP_FTP=1 to skip)
#   01  Import Moovies raw (stock_cost, existing barcodes only)
#   02  Import Lasgo raw (stock_cost, existing barcodes only)
#   03  Normalize raw -> staging_supplier_offers
#   04  Update existing catalog_items (--existing-only; no media_release_date)
#   05  Append pipeline run history
#
# Does not run harmonize (catalog sync step 04). `media_release_date` on catalog_items
# is updated on catalog sync only. Staging offers still carry supplier release fields.
#
# NO TMDB enrichment. NO films rebuild. NO new catalog inserts.
#
# Recommended cadence: daily (e.g. 00:30)
#
# Env vars:
#   MOOVIES_STOCK_FILE   path to Moovies inventory file (optional)
#   LASGO_STOCK_FILE     path to Lasgo stock file (optional)
#   MOOVIES_STOCK_DIR    local dir for Moovies *inventory* only (default: supplier_exports/moovies/stock)
#   LASGO_STOCK_DIR      local dir for Lasgo *stock* only (default: supplier_exports/lasgo/stock)
#   MOOVIES_STOCK_GLOB   pattern under MOOVIES_STOCK_DIR (default: Feed-* or MOOVIES_GLOB)
#   LASGO_STOCK_GLOB     pattern under LASGO_STOCK_DIR (default: LASGO_* or LASGO_GLOB)
#   Catalog exports must live under supplier_exports/*/catalog — this script never scans those dirs.
#   FTP (stock): MOOVIES_STOCK_REMOTE_DIR / LASGO_STOCK_REMOTE_DIR — not *_CATALOG_*
#   SKIP_FTP             set to 1 to skip FTP fetch
#
# Lasgo SFTP → FTP mirror (optional, runs inside step 00 before FTP fetch; same host/user as catalog):
#   LASGO_SFTP_MIRROR_ENABLED=1
#   LASGO_SFTP_HOST, LASGO_SFTP_USER, LASGO_SFTP_PASSWORD and/or LASGO_SFTP_IDENTITY
#   LASGO_SFTP_STOCK_REMOTE_DIR (SFTP folder), LASGO_SFTP_STOCK_GLOB (default LASGO_*)
#   Optional: LASGO_SFTP_CONNECT_TIMEOUT_SEC, LASGO_SFTP_BANNER_TIMEOUT_SEC, LASGO_SFTP_AUTH_TIMEOUT_SEC
#   Stages latest file onto LASGO_STOCK_REMOTE_DIR on your FTP (same as normal stock fetch).
#   Quick auth check: ./venv/bin/python scripts/test_lasgo_sftp_mirror.py --mode stock --connect-only --force
#
# Security scan on every downloaded supplier file (Moovies + Lasgo) before import:
#   SUPPLIER_FETCH_SECURITY_SCAN=basic (default) | clamav | none
#   SUPPLIER_FETCH_SCAN_MAX_BYTES, SUPPLIER_FETCH_SCAN_ALLOW_EXTENSIONS (optional, comma list)
# ============================================================================

PIPELINE_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "${PIPELINE_DIR}/.." && pwd)"
if [[ -n "${PYTHON:-}" && -x "${PYTHON}" ]]; then
  :
elif [[ -x "${PROJECT_DIR}/venv/bin/python" ]]; then
  PYTHON="${PROJECT_DIR}/venv/bin/python"
else
  PYTHON="$(command -v python3 || true)"
fi
if [[ -z "${PYTHON}" || ! -x "${PYTHON}" ]]; then
  echo "ERROR: No Python interpreter (set PYTHON or create ${PROJECT_DIR}/venv)."
  exit 1
fi

LOCK_DIR="${PROJECT_DIR}/.locks"
LOG_DIR="${LOG_DIR:-${PROJECT_DIR}/logs}"
STAMP="$(date '+%Y-%m-%d %H:%M:%S')"

MOOVIES_STOCK_GLOB="${MOOVIES_STOCK_GLOB:-${MOOVIES_GLOB:-Feed-*}}"
LASGO_STOCK_GLOB="${LASGO_STOCK_GLOB:-${LASGO_GLOB:-LASGO_*}}"

mkdir -p "${LOCK_DIR}" "${LOG_DIR}"

LOCK_PATH="${LOCK_DIR}/stock_sync.lock"
if ! mkdir "${LOCK_PATH}" 2>/dev/null; then
  echo "[${STAMP}] Another stock sync is already running. Exiting."
  exit 0
fi
trap 'rmdir "${LOCK_PATH}"' EXIT

LOG_FILE="${LOG_DIR}/stock_sync_$(date '+%Y%m%d').log"
echo "[stock_sync] Full log: ${LOG_FILE}" >&2
exec >> "${LOG_FILE}" 2>&1

echo "================================================================="
echo "[${STAMP}] Starting STOCK SYNC"
echo "================================================================="

cd "${PROJECT_DIR}"

# Stock/inventory files only — separate from catalog sync dirs (*/catalog and */stock never mixed).
DEFAULT_MOOVIES_STOCK="${PROJECT_DIR}/supplier_exports/moovies/stock"
DEFAULT_LASGO_STOCK="${PROJECT_DIR}/supplier_exports/lasgo/stock"
MOOVIES_STOCK_DIR="${MOOVIES_STOCK_DIR:-$DEFAULT_MOOVIES_STOCK}"
LASGO_STOCK_DIR="${LASGO_STOCK_DIR:-$DEFAULT_LASGO_STOCK}"
export MOOVIES_STOCK_DIR LASGO_STOCK_DIR MOOVIES_STOCK_GLOB LASGO_STOCK_GLOB
mkdir -p "${MOOVIES_STOCK_DIR}" "${LASGO_STOCK_DIR}"

# Step 00 — Fetch stock/inventory files from FTP (optional; --mode stock is default)
if [[ "${SKIP_FTP:-0}" != "1" ]]; then
  echo "[step 00] Fetch supplier stock files (Lasgo SFTP→FTP mirror if enabled, then FTP→local; scan before import)"
  "${PYTHON}" pipeline/00_fetch_supplier_files.py --mode stock \
    || echo "WARN: Step 00 failed (Lasgo SFTP mirror if enabled, then FTP→local). Continuing with latest files already under MOOVIES_STOCK_DIR / LASGO_STOCK_DIR — not guaranteed a fresh supplier pull."
fi

# Resolve file paths
pick_latest() {
  local dir="$1" glob="$2"
  [[ -z "${dir}" || ! -d "${dir}" ]] && return 0
  # shellcheck disable=SC2012
  ls -t "${dir}"/${glob} 2>/dev/null | head -n 1 || true
}

MOOVIES_STOCK_FILE="${MOOVIES_STOCK_FILE:-$(pick_latest "${MOOVIES_STOCK_DIR:-}" "${MOOVIES_STOCK_GLOB}")}"
LASGO_STOCK_FILE="${LASGO_STOCK_FILE:-$(pick_latest "${LASGO_STOCK_DIR:-}" "${LASGO_STOCK_GLOB}")}"

if [[ -z "${MOOVIES_STOCK_FILE:-}" || ! -f "${MOOVIES_STOCK_FILE}" ]]; then
  echo "ERROR: Cannot resolve Moovies stock file."
  echo "  Looked in: MOOVIES_STOCK_DIR=${MOOVIES_STOCK_DIR}"
  echo "  Glob (names must match, case-sensitive): ${MOOVIES_STOCK_GLOB}"
  if [[ -d "${MOOVIES_STOCK_DIR}" ]]; then
    echo "  Contents of that directory (newest pick uses ls -t):"
    ls -lat "${MOOVIES_STOCK_DIR}" 2>/dev/null | head -25 || true
  else
    echo "  Directory missing — create it or set MOOVIES_STOCK_DIR to where Feed files live."
  fi
  echo "  Fix: MOOVIES_STOCK_FILE=/absolute/path  or MOOVIES_STOCK_GLOB='YourPrefix-*' or set MOOVIES_STOCK_DIR (legacy: parent moovies folder)"
  exit 1
fi
if [[ -z "${LASGO_STOCK_FILE:-}" || ! -f "${LASGO_STOCK_FILE}" ]]; then
  echo "ERROR: Cannot resolve Lasgo stock file."
  echo "  Looked in: LASGO_STOCK_DIR=${LASGO_STOCK_DIR}"
  echo "  Glob (case-sensitive): ${LASGO_STOCK_GLOB}"
  if [[ -d "${LASGO_STOCK_DIR}" ]]; then
    echo "  Contents of that directory:"
    ls -lat "${LASGO_STOCK_DIR}" 2>/dev/null | head -25 || true
  else
    echo "  Directory missing — create it or set LASGO_STOCK_DIR."
  fi
  echo "  Fix: LASGO_STOCK_FILE=/absolute/path  or LASGO_STOCK_GLOB=… or set LASGO_STOCK_DIR"
  exit 1
fi

echo "MOOVIES_STOCK_DIR=${MOOVIES_STOCK_DIR} MOOVIES_STOCK_GLOB=${MOOVIES_STOCK_GLOB}"
echo "LASGO_STOCK_DIR=${LASGO_STOCK_DIR} LASGO_STOCK_GLOB=${LASGO_STOCK_GLOB}"
echo "MOOVIES_STOCK_FILE=${MOOVIES_STOCK_FILE}"
echo "LASGO_STOCK_FILE=${LASGO_STOCK_FILE}"

# Step 01 — Import Moovies stock_cost (existing only)
echo "[step 01] Import Moovies stock_cost into raw (existing barcodes only)"
"${PYTHON}" pipeline/01_import_moovies_raw.py "${MOOVIES_STOCK_FILE}" --mode stock_cost --existing-only-in-raw

# Step 02 — Import Lasgo stock_cost (existing only)
echo "[step 02] Import Lasgo stock_cost into raw (existing barcodes only)"
"${PYTHON}" pipeline/02_import_lasgo_raw.py "${LASGO_STOCK_FILE}" --mode stock_cost --existing-only-in-raw

# Step 03 — Normalize
echo "[step 03] Normalize stock batches -> staging_supplier_offers"
MOOVIES_BATCH=""
LASGO_BATCH=""
while IFS= read -r line || [[ -n "${line}" ]]; do
  case "$line" in
    MOOVIES_BATCH=*) MOOVIES_BATCH="${line#MOOVIES_BATCH=}" ;;
    LASGO_BATCH=*) LASGO_BATCH="${line#LASGO_BATCH=}" ;;
  esac
done < <("${PYTHON}" - <<'PY'
import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(".env")
sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))

def latest_batch(table):
    resp = (
        sb.table(table)
        .select("import_batch_id,imported_at")
        .order("imported_at", desc=True)
        .limit(1)
        .execute()
    )
    row = (resp.data or [None])[0]
    bid = row.get("import_batch_id") if row else None
    return bid if bid is not None else ""

print("MOOVIES_BATCH=" + str(latest_batch("staging_moovies_raw")))
print("LASGO_BATCH=" + str(latest_batch("staging_lasgo_raw")))
PY
)

"${PYTHON}" pipeline/03_normalize_supplier_products.py \
  --moovies-batch "${MOOVIES_BATCH}" \
  --lasgo-batch "${LASGO_BATCH}"

# Step 04 — Update existing catalog_items only (commercial whitelist; no harmonize / no release date)
echo "[step 04] Update existing catalog_items only (no new inserts)"
"${PYTHON}" pipeline/05_upsert_to_catalog_items.py --existing-only

# Completion banner BEFORE observability append so parse_log_file sees end timestamp + completed.
echo "================================================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S')] STOCK SYNC complete"
echo "================================================================="

echo "[step 05] Append pipeline run history (trends / observability)"
"${PYTHON}" scripts/observability/append_pipeline_run_history.py --log-file "${LOG_FILE}" --pipeline-type stock_sync \
  || echo "WARN: append_pipeline_run_history failed (non-fatal)"
