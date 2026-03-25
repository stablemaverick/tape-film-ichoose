#!/usr/bin/env bash
set -euo pipefail

# Intraday/nightly stock + price sync (legacy entrypoint).
# No harmonization, no catalog media_release_date updates (see catalog_update_rules STOCK_SYNC_WHITELIST).
# No TMDB enrichment, no films linking, no new catalog inserts.

PROJECT_DIR="/Users/simonpittaway/Dropbox/tape-film-ichoose"
LOCK_DIR="${PROJECT_DIR}/.locks"
LOCK_PATH="${LOCK_DIR}/daily_stock_sync.lock"
LOG_DIR="${LOG_DIR:-${PROJECT_DIR}/logs}"
STAMP="$(date '+%Y-%m-%d %H:%M:%S')"

MOOVIES_STOCK_FILE="${MOOVIES_STOCK_FILE:-}"
LASGO_STOCK_FILE="${LASGO_STOCK_FILE:-}"
MOOVIES_STOCK_DIR="${MOOVIES_STOCK_DIR:-}"
LASGO_STOCK_DIR="${LASGO_STOCK_DIR:-}"
MOOVIES_GLOB="${MOOVIES_GLOB:-Feed-*}"
LASGO_GLOB="${LASGO_GLOB:-LASGO_*}"

mkdir -p "${LOCK_DIR}" "${LOG_DIR}"

if ! mkdir "${LOCK_PATH}" 2>/dev/null; then
  echo "[${STAMP}] Another stock sync is already running. Exiting."
  exit 0
fi
trap 'rmdir "${LOCK_PATH}"' EXIT

LOG_FILE="${LOG_DIR}/daily_stock_sync_$(date '+%Y%m%d').log"
exec >> "${LOG_FILE}" 2>&1

echo "================================================================="
echo "[${STAMP}] Starting daily stock sync"
echo "MOOVIES_STOCK_FILE=${MOOVIES_STOCK_FILE}"
echo "LASGO_STOCK_FILE=${LASGO_STOCK_FILE}"
echo "MOOVIES_STOCK_DIR=${MOOVIES_STOCK_DIR} MOOVIES_GLOB=${MOOVIES_GLOB}"
echo "LASGO_STOCK_DIR=${LASGO_STOCK_DIR} LASGO_GLOB=${LASGO_GLOB}"

cd "${PROJECT_DIR}"

pick_latest_file() {
  local dir="$1"
  local glob="$2"
  if [[ -z "${dir}" || ! -d "${dir}" ]]; then
    return 0
  fi
  # shellcheck disable=SC2012
  ls -t "${dir}"/${glob} 2>/dev/null | head -n 1 || true
}

if [[ -z "${MOOVIES_STOCK_FILE}" ]]; then
  MOOVIES_STOCK_FILE="$(pick_latest_file "${MOOVIES_STOCK_DIR}" "${MOOVIES_GLOB}")"
fi
if [[ -z "${LASGO_STOCK_FILE}" ]]; then
  LASGO_STOCK_FILE="$(pick_latest_file "${LASGO_STOCK_DIR}" "${LASGO_GLOB}")"
fi

if [[ -z "${MOOVIES_STOCK_FILE}" || -z "${LASGO_STOCK_FILE}" ]]; then
  echo "ERROR: could not resolve stock files."
  echo "Provide MOOVIES_STOCK_FILE/LASGO_STOCK_FILE directly, or set MOOVIES_STOCK_DIR/LASGO_STOCK_DIR."
  exit 1
fi

if [[ ! -f "${MOOVIES_STOCK_FILE}" ]]; then
  echo "ERROR: Moovies stock file not found: ${MOOVIES_STOCK_FILE}"
  exit 1
fi
if [[ ! -f "${LASGO_STOCK_FILE}" ]]; then
  echo "ERROR: Lasgo stock file not found: ${LASGO_STOCK_FILE}"
  exit 1
fi

echo "Resolved files:"
echo "  MOOVIES_STOCK_FILE=${MOOVIES_STOCK_FILE}"
echo "  LASGO_STOCK_FILE=${LASGO_STOCK_FILE}"

echo "[step 1] Import Moovies stock_cost into raw (existing-only)"
MOOVIES_BATCH="$(
  MOOVIES_FILE="${MOOVIES_STOCK_FILE}" venv/bin/python - <<'PY'
import os
from import_moovies_raw import import_raw
batch = import_raw(os.environ["MOOVIES_FILE"], mode="stock_cost", existing_only_in_raw=True)
print(batch)
PY
)"
MOOVIES_BATCH="$(echo "${MOOVIES_BATCH}" | tail -n 1 | tr -d '\r')"
echo "MOOVIES_BATCH=${MOOVIES_BATCH}"

echo "[step 2] Import Lasgo stock_cost into raw (existing-only)"
LASGO_BATCH="$(
  LASGO_FILE="${LASGO_STOCK_FILE}" venv/bin/python - <<'PY'
import os
from import_lasgo_raw import import_lasgo_raw
batch = import_lasgo_raw(os.environ["LASGO_FILE"], mode="stock_cost", existing_only_in_raw=True)
print(batch)
PY
)"
LASGO_BATCH="$(echo "${LASGO_BATCH}" | tail -n 1 | tr -d '\r')"
echo "LASGO_BATCH=${LASGO_BATCH}"

echo "[step 3] Normalize stock batches -> staging_supplier_offers"
venv/bin/python normalize_supplier_products.py \
  --moovies-batch "${MOOVIES_BATCH}" \
  --lasgo-batch "${LASGO_BATCH}"

echo "[step 4] Update existing catalog_items only (no inserts, commercial whitelist — no release date)"
venv/bin/python upsert_supplier_offers_to_catalog_items_preserve_tmdb.py --existing-only

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Daily stock sync complete"
echo "================================================================="

