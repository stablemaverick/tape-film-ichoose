#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# CATALOG SYNC — Full pipeline for new titles, enrichment, and film linking
# ============================================================================
#
# Sequence:
#   00  Fetch supplier files from FTP        (optional, set SKIP_FTP=1 to skip)
#   01  Import Moovies raw (full)
#   02  Import Lasgo raw (full, Blu-ray only)
#   03  Normalize raw -> staging_supplier_offers
#   04  Cross-supplier harmonization
#   05  Upsert supplier offers -> catalog_items (insert new + update existing)
#   06  Enrich new unlinked catalog rows with TMDB (daily mode)
#   07  Build/link films from enriched catalog_items
#
# Recommended cadence: every few days or on-demand (e.g. Mon/Wed/Fri 02:15)
#
# Env vars:
#   MOOVIES_FILE           explicit Moovies catalog file (optional)
#   LASGO_FILE             explicit Lasgo catalog file (optional)
#   MOOVIES_CATALOG_DIR    local dir for Moovies *catalog* exports only (default: supplier_exports/moovies/catalog)
#   LASGO_CATALOG_DIR      local dir for Lasgo *catalog* exports only (default: supplier_exports/lasgo/catalog)
#   MOOVIES_CATALOG_GLOB   filename pattern under MOOVIES_CATALOG_DIR (default: Feed-*)
#   LASGO_CATALOG_GLOB     filename pattern under LASGO_CATALOG_DIR (default: LASGO_*)
#   Do NOT set MOOVIES_DIR / LASGO_DIR for this script — they are ignored (avoids picking stock folders).
#   FTP defaults: /TAPE_Film/Moovies/Catalog and /TAPE_Film/Lasgo/Catalog (override *_CATALOG_REMOTE_DIR in .env).
#   With FTP: strict fetch only — no local fallback; empty folder → "No Files to Process in path - <path>".
#   After a successful run, those files are moved on FTP to .../Catalog/Archive.
#   SKIP_FTP=1: use local MOOVIES_CATALOG_DIR/LASGO_CATALOG_DIR only; no FTP archive.
#
# Lasgo SFTP → FTP mirror (optional, inside step 00 before strict catalog fetch; same host/user as stock):
#   LASGO_SFTP_MIRROR_ENABLED=1
#   LASGO_SFTP_HOST, LASGO_SFTP_USER, LASGO_SFTP_PASSWORD and/or LASGO_SFTP_IDENTITY
#   LASGO_SFTP_CATALOG_REMOTE_DIR, LASGO_SFTP_CATALOG_GLOB (default LASGO_*)
#   Optional: LASGO_SFTP_CONNECT_TIMEOUT_SEC, LASGO_SFTP_BANNER_TIMEOUT_SEC, LASGO_SFTP_AUTH_TIMEOUT_SEC
#   Stages latest file onto LASGO_CATALOG_REMOTE_DIR on your FTP (same as normal catalog fetch).
#   Quick auth check: ./venv/bin/python scripts/test_lasgo_sftp_mirror.py --mode catalog --connect-only --force
#
# Security scan on every fetched catalog file (Moovies + Lasgo) before import:
#   SUPPLIER_FETCH_SECURITY_SCAN=basic (default) | clamav | none
#   SUPPLIER_FETCH_SCAN_MAX_BYTES, SUPPLIER_FETCH_SCAN_ALLOW_EXTENSIONS (optional)
#   TMDB_MAX_ROWS       default 4000
#   TMDB_MAX_GROUPS     default 1500
#   TMDB_SLEEP_MS       default 350
#   MOOVIES_IMPORT_LIMIT  optional max rows for step 01 (--limit; smoke tests)
#   LASGO_IMPORT_LIMIT    optional max rows for step 02 (--limit; smoke tests)
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

TMDB_MAX_ROWS="${TMDB_MAX_ROWS:-4000}"
TMDB_MAX_GROUPS="${TMDB_MAX_GROUPS:-1500}"
TMDB_SLEEP_MS="${TMDB_SLEEP_MS:-350}"

mkdir -p "${LOCK_DIR}" "${LOG_DIR}"

FETCH_ENV="${LOCK_DIR}/catalog_ftp_fetch.env"

LOCK_PATH="${LOCK_DIR}/catalog_sync.lock"
if ! mkdir "${LOCK_PATH}" 2>/dev/null; then
  echo "[${STAMP}] Another catalog sync is already running. Exiting."
  exit 0
fi
trap 'rmdir "${LOCK_PATH}"' EXIT

LOG_FILE="${LOG_DIR}/catalog_sync_$(date '+%Y%m%d').log"
# After exec, stdout/stderr only go to LOG_FILE — the terminal stays quiet unless we print here (stderr still TTY).
echo "[catalog_sync] Full log: ${LOG_FILE}" >&2
exec >> "${LOG_FILE}" 2>&1

echo "================================================================="
echo "[${STAMP}] Starting CATALOG SYNC"
echo "================================================================="

cd "${PROJECT_DIR}"

# Catalog files must live under */catalog — never the stock export tree (supplier_exports/moovies or .../lasgo alone).
DEFAULT_MOOVIES_CATALOG="${PROJECT_DIR}/supplier_exports/moovies/catalog"
DEFAULT_LASGO_CATALOG="${PROJECT_DIR}/supplier_exports/lasgo/catalog"
MOOVIES_CATALOG_DIR="${MOOVIES_CATALOG_DIR:-$DEFAULT_MOOVIES_CATALOG}"
LASGO_CATALOG_DIR="${LASGO_CATALOG_DIR:-$DEFAULT_LASGO_CATALOG}"
MOOVIES_CATALOG_GLOB="${MOOVIES_CATALOG_GLOB:-Feed-*}"
LASGO_CATALOG_GLOB="${LASGO_CATALOG_GLOB:-LASGO_*}"
export MOOVIES_CATALOG_DIR LASGO_CATALOG_DIR MOOVIES_CATALOG_GLOB LASGO_CATALOG_GLOB
mkdir -p "${MOOVIES_CATALOG_DIR}" "${LASGO_CATALOG_DIR}"

# Step 00 — FTP: /TAPE_Film/Moovies/Catalog and /TAPE_Film/Lasgo/Catalog only (no local fallback).
if [[ "${SKIP_FTP:-0}" != "1" ]]; then
  rm -f "${FETCH_ENV}"
  echo "[step 00] Fetch supplier catalog files (strict: Lasgo SFTP→FTP mirror if enabled, then FTP→local; scan before import)"
  "${PYTHON}" pipeline/00_fetch_supplier_files.py --mode catalog --strict-catalog --write-fetch-env "${FETCH_ENV}"
  set -a
  # shellcheck disable=SC1090
  source "${FETCH_ENV}"
  set +a
else
  rm -f "${FETCH_ENV}"
fi

pick_latest() {
  local dir="$1" glob="$2"
  [[ -z "${dir}" || ! -d "${dir}" ]] && return 0
  # shellcheck disable=SC2012
  ls -t "${dir}"/${glob} 2>/dev/null | head -n 1 || true
}

if [[ "${SKIP_FTP:-0}" == "1" ]]; then
  MOOVIES_FILE="${MOOVIES_FILE:-$(pick_latest "${MOOVIES_CATALOG_DIR}" "${MOOVIES_CATALOG_GLOB}")}"
  LASGO_FILE="${LASGO_FILE:-$(pick_latest "${LASGO_CATALOG_DIR}" "${LASGO_CATALOG_GLOB}")}"
fi

if [[ -z "${MOOVIES_FILE:-}" || ! -f "${MOOVIES_FILE}" ]]; then
  echo "ERROR: Cannot resolve Moovies catalog file."
  echo "  Looked in: MOOVIES_CATALOG_DIR=${MOOVIES_CATALOG_DIR}  glob: ${MOOVIES_CATALOG_GLOB}"
  if [[ -d "${MOOVIES_CATALOG_DIR}" ]]; then
    echo "  Directory listing:"
    ls -lat "${MOOVIES_CATALOG_DIR}" 2>/dev/null | head -25 || true
  else
    echo "  Directory missing: ${MOOVIES_CATALOG_DIR}"
  fi
  echo "  Fix: place catalog exports under MOOVIES_CATALOG_DIR or set MOOVIES_FILE=/absolute/path"
  exit 1
fi
if [[ -z "${LASGO_FILE:-}" || ! -f "${LASGO_FILE}" ]]; then
  echo "ERROR: Cannot resolve Lasgo catalog file."
  echo "  Looked in: LASGO_CATALOG_DIR=${LASGO_CATALOG_DIR}  glob: ${LASGO_CATALOG_GLOB}"
  if [[ -d "${LASGO_CATALOG_DIR}" ]]; then
    echo "  Directory listing:"
    ls -lat "${LASGO_CATALOG_DIR}" 2>/dev/null | head -25 || true
  else
    echo "  Directory missing: ${LASGO_CATALOG_DIR}"
  fi
  echo "  Fix: place catalog exports under LASGO_CATALOG_DIR or set LASGO_FILE=/absolute/path"
  exit 1
fi

echo "MOOVIES_CATALOG_DIR=${MOOVIES_CATALOG_DIR} MOOVIES_CATALOG_GLOB=${MOOVIES_CATALOG_GLOB}"
echo "LASGO_CATALOG_DIR=${LASGO_CATALOG_DIR} LASGO_CATALOG_GLOB=${LASGO_CATALOG_GLOB}"
echo "MOOVIES_FILE=${MOOVIES_FILE}"
echo "LASGO_FILE=${LASGO_FILE}"

MOOVIES_LIMIT_ARGS=()
LASGO_LIMIT_ARGS=()
[[ -n "${MOOVIES_IMPORT_LIMIT:-}" ]] && MOOVIES_LIMIT_ARGS+=(--limit "${MOOVIES_IMPORT_LIMIT}")
[[ -n "${LASGO_IMPORT_LIMIT:-}" ]] && LASGO_LIMIT_ARGS+=(--limit "${LASGO_IMPORT_LIMIT}")
if [[ "${#MOOVIES_LIMIT_ARGS[@]}" -gt 0 || "${#LASGO_LIMIT_ARGS[@]}" -gt 0 ]]; then
  echo "Sample import limits: MOOVIES_IMPORT_LIMIT=${MOOVIES_IMPORT_LIMIT:-} LASGO_IMPORT_LIMIT=${LASGO_IMPORT_LIMIT:-}"
fi

# Step 01 — Import Moovies raw
echo "[step 01] Import Moovies raw (full)"
if [[ "${#MOOVIES_LIMIT_ARGS[@]}" -gt 0 ]]; then
  "${PYTHON}" pipeline/01_import_moovies_raw.py "${MOOVIES_FILE}" --mode full "${MOOVIES_LIMIT_ARGS[@]}"
else
  "${PYTHON}" pipeline/01_import_moovies_raw.py "${MOOVIES_FILE}" --mode full
fi

# Step 02 — Import Lasgo raw
echo "[step 02] Import Lasgo raw (full, Blu-ray only)"
if [[ "${#LASGO_LIMIT_ARGS[@]}" -gt 0 ]]; then
  "${PYTHON}" pipeline/02_import_lasgo_raw.py "${LASGO_FILE}" --mode full "${LASGO_LIMIT_ARGS[@]}"
else
  "${PYTHON}" pipeline/02_import_lasgo_raw.py "${LASGO_FILE}" --mode full
fi

# Step 03 — Normalize
echo "[step 03] Normalize raw -> staging_supplier_offers"
MOOVIES_BATCH=""
LASGO_BATCH=""
SHOPIFY_BATCH=""
while IFS= read -r line || [[ -n "${line}" ]]; do
  case "$line" in
    MOOVIES_BATCH=*) MOOVIES_BATCH="${line#MOOVIES_BATCH=}" ;;
    LASGO_BATCH=*) LASGO_BATCH="${line#LASGO_BATCH=}" ;;
    SHOPIFY_BATCH=*) SHOPIFY_BATCH="${line#SHOPIFY_BATCH=}" ;;
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
print("SHOPIFY_BATCH=" + str(latest_batch("staging_shopify_raw")))
PY
)

NORMALIZE_ARGS=()
[[ -n "${MOOVIES_BATCH:-}" ]] && NORMALIZE_ARGS+=(--moovies-batch "${MOOVIES_BATCH}")
[[ -n "${LASGO_BATCH:-}" ]] && NORMALIZE_ARGS+=(--lasgo-batch "${LASGO_BATCH}")
[[ -n "${SHOPIFY_BATCH:-}" ]] && NORMALIZE_ARGS+=(--shopify-batch "${SHOPIFY_BATCH}")

if [[ "${#NORMALIZE_ARGS[@]}" -gt 0 ]]; then
  "${PYTHON}" pipeline/03_normalize_supplier_products.py "${NORMALIZE_ARGS[@]}"
else
  echo "WARN: No batch IDs found, skipping normalize."
fi

# Step 04 — Cross-supplier harmonization
echo "[step 04] Cross-supplier harmonization"
"${PYTHON}" pipeline/04_harmonize_supplier_offers.py

# Step 05 — Upsert to catalog_items
echo "[step 05] Upsert supplier offers -> catalog_items"
"${PYTHON}" pipeline/05_upsert_to_catalog_items.py

# Step 06 — TMDB enrichment (daily mode: new unlinked rows only)
echo "[step 06] Enrich new unlinked catalog rows with TMDB (daily mode)"
"${PYTHON}" pipeline/06_enrich_catalog_with_tmdb_daily.py \
  --max-rows "${TMDB_MAX_ROWS}" \
  --max-groups "${TMDB_MAX_GROUPS}" \
  --sleep-ms "${TMDB_SLEEP_MS}"

# Step 07 — Build/link films
echo "[step 07] Build/link films from catalog_items"
"${PYTHON}" pipeline/07_build_films_from_catalog.py

# Move processed files on FTP into .../Catalog/Archive (skipped when SKIP_FTP=1).
if [[ "${SKIP_FTP:-0}" != "1" && -f "${FETCH_ENV}" ]]; then
  echo "[step 07b] Archive catalog files on FTP (Catalog/Archive)"
  if ! "${PYTHON}" pipeline/00_fetch_supplier_files.py --archive-from-env "${FETCH_ENV}" --env ".env"; then
    echo "WARN: FTP archive (07b) failed — catalog sync completed; move files to Archive manually if needed."
  fi
fi

# Completion banner BEFORE observability append so parse_log_file sees end timestamp + completed.
echo "================================================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S')] CATALOG SYNC complete"
echo "================================================================="

echo "[step 08] Append pipeline run history (trends / observability)"
"${PYTHON}" scripts/observability/append_pipeline_run_history.py --log-file "${LOG_FILE}" --pipeline-type catalog_sync \
  || echo "WARN: append_pipeline_run_history failed (non-fatal)"
