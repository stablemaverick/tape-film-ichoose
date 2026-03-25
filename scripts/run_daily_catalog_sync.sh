#!/usr/bin/env bash
set -euo pipefail

# Daily catalog sync pipeline:
# 1) Normalize latest raw supplier batches into staging_supplier_offers.
# 2) Upsert supplier offers into catalog_items (preserve TMDB fields for existing rows).
# 3) Enrich only unattempted catalog rows with TMDB metadata.

PROJECT_DIR="/Users/simonpittaway/Dropbox/tape-film-ichoose"
LOCK_DIR="${PROJECT_DIR}/.locks"
LOCK_PATH="${LOCK_DIR}/daily_catalog_sync.lock"
LOG_DIR="${LOG_DIR:-${PROJECT_DIR}/logs}"
STAMP="$(date '+%Y-%m-%d %H:%M:%S')"
START_ISO_UTC="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

TMDB_MAX_ROWS="${TMDB_MAX_ROWS:-4000}"
TMDB_MAX_GROUPS="${TMDB_MAX_GROUPS:-1500}"
TMDB_SLEEP_MS="${TMDB_SLEEP_MS:-350}"
NORMALIZE_LATEST_RAW="${NORMALIZE_LATEST_RAW:-1}"
SYNC_FILMS_FROM_CATALOG="${SYNC_FILMS_FROM_CATALOG:-1}"

mkdir -p "${LOCK_DIR}" "${LOG_DIR}"

if ! mkdir "${LOCK_PATH}" 2>/dev/null; then
  echo "[${STAMP}] Another daily sync is already running. Exiting."
  exit 0
fi
trap 'rmdir "${LOCK_PATH}"' EXIT

LOG_FILE="${LOG_DIR}/daily_catalog_sync_$(date '+%Y%m%d').log"
exec >> "${LOG_FILE}" 2>&1

echo "================================================================="
echo "[${STAMP}] Starting daily catalog sync"
echo "START_ISO_UTC=${START_ISO_UTC}"
echo "TMDB_MAX_ROWS=${TMDB_MAX_ROWS} TMDB_MAX_GROUPS=${TMDB_MAX_GROUPS} TMDB_SLEEP_MS=${TMDB_SLEEP_MS}"
echo "NORMALIZE_LATEST_RAW=${NORMALIZE_LATEST_RAW}"
echo "SYNC_FILMS_FROM_CATALOG=${SYNC_FILMS_FROM_CATALOG}"

cd "${PROJECT_DIR}"

echo "[metrics] Baseline snapshot"
BASELINE_EXPORTS="$(
  RUN_START_ISO="${START_ISO_UTC}" venv/bin/python - <<'PY'
import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(".env")
sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))

def c(q):
    return q.execute().count or 0

print(f"BASE_CATALOG_TOTAL={c(sb.table('catalog_items').select('id', count='exact'))}")
print(f"BASE_FILMS_TOTAL={c(sb.table('films').select('id', count='exact'))}")
print(f"BASE_MATCHED={c(sb.table('catalog_items').select('id', count='exact').eq('tmdb_match_status', 'matched'))}")
print(f"BASE_NOT_FOUND={c(sb.table('catalog_items').select('id', count='exact').eq('tmdb_match_status', 'not_found'))}")
print(f"BASE_PENDING={c(sb.table('catalog_items').select('id', count='exact').is_('tmdb_last_refreshed_at', 'null'))}")
PY
)"
eval "${BASELINE_EXPORTS}"
echo "BASE_CATALOG_TOTAL=${BASE_CATALOG_TOTAL} BASE_FILMS_TOTAL=${BASE_FILMS_TOTAL} BASE_MATCHED=${BASE_MATCHED} BASE_NOT_FOUND=${BASE_NOT_FOUND} BASE_PENDING=${BASE_PENDING}"

if [[ "${NORMALIZE_LATEST_RAW}" == "1" ]]; then
  echo "[step 1] Normalize latest raw batches -> staging_supplier_offers"
  BATCH_EXPORTS="$(
    venv/bin/python - <<'PY'
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
    return row.get("import_batch_id") if row else ""

print(f"MOOVIES_BATCH={latest_batch('staging_moovies_raw')}")
print(f"LASGO_BATCH={latest_batch('staging_lasgo_raw')}")
print(f"SHOPIFY_BATCH={latest_batch('staging_shopify_raw')}")
PY
  )"
  eval "${BATCH_EXPORTS}"

  NORMALIZE_ARGS=()
  if [[ -n "${MOOVIES_BATCH:-}" ]]; then
    NORMALIZE_ARGS+=(--moovies-batch "${MOOVIES_BATCH}")
  fi
  if [[ -n "${LASGO_BATCH:-}" ]]; then
    NORMALIZE_ARGS+=(--lasgo-batch "${LASGO_BATCH}")
  fi
  if [[ -n "${SHOPIFY_BATCH:-}" ]]; then
    NORMALIZE_ARGS+=(--shopify-batch "${SHOPIFY_BATCH}")
  fi

  if [[ "${#NORMALIZE_ARGS[@]}" -gt 0 ]]; then
    venv/bin/python normalize_supplier_products.py "${NORMALIZE_ARGS[@]}"
  else
    echo "No latest raw batch IDs found, skipping normalize step."
  fi
else
  echo "[step 1] Normalize step disabled (NORMALIZE_LATEST_RAW=${NORMALIZE_LATEST_RAW})"
fi

echo "[step 1b] Cross-supplier harmonization on staging_supplier_offers"
venv/bin/python harmonize_supplier_offers.py

echo "[step 2] Upsert supplier offers -> catalog_items (preserve TMDB)"
venv/bin/python upsert_supplier_offers_to_catalog_items_preserve_tmdb.py

echo "[metrics] Supplier stock/price/release updates applied in this run window"
RUN_START_ISO="${START_ISO_UTC}" venv/bin/python - <<'PY'
import os
from collections import defaultdict
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(".env")
sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))
run_start = os.getenv("RUN_START_ISO")

offset = 0
page_size = 1000
counts = defaultdict(int)

while True:
    page = (
        sb.table("catalog_items")
        .select("supplier")
        .gte("supplier_last_seen_at", run_start)
        .range(offset, offset + page_size - 1)
        .execute()
    ).data or []
    if not page:
        break
    for r in page:
        supplier = (r.get("supplier") or "Unknown").strip() or "Unknown"
        counts[supplier] += 1
    if len(page) < page_size:
        break
    offset += page_size

total = sum(counts.values())
print(f"SUPPLIER_TOUCH_TOTAL={total}")
if total == 0:
    print("SUPPLIER_TOUCH_BY_SUPPLIER=none")
else:
    for supplier in sorted(counts.keys()):
        print(f"SUPPLIER_TOUCH|supplier={supplier}|rows={counts[supplier]}")
PY

echo "[step 3] Enrich unattempted catalog_items rows with TMDB"
venv/bin/python enrich_catalog_with_tmdb_v2.py \
  --max-rows "${TMDB_MAX_ROWS}" \
  --max-groups "${TMDB_MAX_GROUPS}" \
  --sleep-ms "${TMDB_SLEEP_MS}"

if [[ "${SYNC_FILMS_FROM_CATALOG}" == "1" ]]; then
  echo "[step 4] Build/link films from catalog_items (only rows with film_id NULL; use --full-rebuild manually for recovery)"
  venv/bin/python build_films_from_catalog.py
else
  echo "[step 4] Film sync disabled (SYNC_FILMS_FROM_CATALOG=${SYNC_FILMS_FROM_CATALOG})"
fi

echo "[metrics] Final snapshot and deltas"
RUN_START_ISO="${START_ISO_UTC}" \
BASE_CATALOG_TOTAL="${BASE_CATALOG_TOTAL}" \
BASE_FILMS_TOTAL="${BASE_FILMS_TOTAL}" \
BASE_MATCHED="${BASE_MATCHED}" \
BASE_NOT_FOUND="${BASE_NOT_FOUND}" \
BASE_PENDING="${BASE_PENDING}" \
venv/bin/python - <<'PY'
import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(".env")
sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))

def c(q):
    return q.execute().count or 0

base_catalog = int(os.getenv("BASE_CATALOG_TOTAL", "0"))
base_films = int(os.getenv("BASE_FILMS_TOTAL", "0"))
base_matched = int(os.getenv("BASE_MATCHED", "0"))
base_not_found = int(os.getenv("BASE_NOT_FOUND", "0"))
base_pending = int(os.getenv("BASE_PENDING", "0"))
run_start = os.getenv("RUN_START_ISO")

catalog_total = c(sb.table("catalog_items").select("id", count="exact"))
films_total = c(sb.table("films").select("id", count="exact"))
matched = c(sb.table("catalog_items").select("id", count="exact").eq("tmdb_match_status", "matched"))
not_found = c(sb.table("catalog_items").select("id", count="exact").eq("tmdb_match_status", "not_found"))
pending = c(sb.table("catalog_items").select("id", count="exact").is_("tmdb_last_refreshed_at", "null"))
linked_this_run = c(sb.table("catalog_items").select("id", count="exact").gte("film_linked_at", run_start))

print(f"CATALOG_TOTAL={catalog_total} DELTA_CATALOG_ADDED={catalog_total - base_catalog}")
print(f"TMDB_MATCHED={matched} DELTA_MATCHED={matched - base_matched}")
print(f"TMDB_NOT_FOUND={not_found} DELTA_NOT_FOUND={not_found - base_not_found}")
print(f"TMDB_PENDING={pending} DELTA_PENDING={pending - base_pending}")
print(f"FILMS_TOTAL={films_total} DELTA_FILMS_ADDED={films_total - base_films}")
print(f"FILM_LINKED_ROWS_THIS_RUN={linked_this_run}")
PY

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Daily catalog sync complete"
echo "================================================================="

