# Tape Film — Data Pipeline

All numbered scripts in this folder are thin wrappers: each sets `sys.path`,
`chdir`s to the repo root, and calls **exactly one** `run_from_argv()` on the
matching module under `app.services.*` (in-process — no subprocess).

Root-level scripts with the same names still exist as **compatibility shims**
that delegate to the same services, so existing cron lines keep working.

---

## Pipeline scripts (numbered sequence)

| # | Script | Purpose | Used by |
|---|--------|---------|---------|
| 00 | `00_fetch_supplier_files.py` | Download latest supplier files from FTP (`--mode stock` or `--mode catalog`) | Both |
| 01 | `01_import_moovies_raw.py` | Import Moovies raw data into `staging_moovies_raw` | Both |
| 02 | `02_import_lasgo_raw.py` | Import Lasgo raw data into `staging_lasgo_raw` (Blu-ray only) | Both |
| 03 | `03_normalize_supplier_products.py` | Normalize raw → `staging_supplier_offers` (own data only) | Both |
| 04 | `04_harmonize_supplier_offers.py` | Cross-supplier harmonization by barcode | Catalog sync only (`run_stock_sync.sh` skips this) |
| 05 | `05_upsert_to_catalog_items.py` | Move offers → `catalog_items` (preserves TMDB) | Both |
| 06d | `06_enrich_catalog_with_tmdb_daily.py` | Enrich new unlinked rows with TMDB (daily/cron) | Catalog only |
| 06r | `06_enrich_catalog_with_tmdb_recovery.py` | Enrich all unattempted rows with TMDB (rebuild) | Manual only |
| 07 | `07_build_films_from_catalog.py` | Build/link `films` table from enriched catalog | Catalog only |
| — | *(shell)* | `append_pipeline_run_history.py` — append trends JSON | Both (end of run) |

---

## Two operational pipelines

### A. Stock Sync (daily)

Updates **commercial fields only** for **known products** (prices, stock/qty signals, availability, `supplier_last_seen_at`).  
**Does not** run harmonization and **does not** write `media_release_date` to `catalog_items` — use **catalog sync** for release dates and identity/harmonized fields.

No new titles, no TMDB enrichment, no films rebuild.

```
run_stock_sync.sh
  └─ 00 → 01 (stock_cost) → 02 (stock_cost) → 03 → 04 (--existing-only upsert)
      → 05 append_pipeline_run_history (logs/pipeline_run_history.json)
```

```bash
# Run manually (after `cd` to repo root). Default local dirs: supplier_exports/moovies/stock
# and supplier_exports/lasgo/stock (separate from catalog */catalog trees).
# Legacy: if files still live in supplier_exports/moovies (parent), set MOOVIES_STOCK_DIR to that path.
# Step 00 uses --mode stock. Stock FTP (override in .env if needed):
#   MOOVIES_STOCK_REMOTE_DIR=/TAPE_Film/Moovies/Inventory
#   LASGO_STOCK_REMOTE_DIR=/TAPE_Film/Lasgo/Incoming
./pipeline/run_stock_sync.sh

MOOVIES_STOCK_DIR=/path/to/moovies/stock LASGO_STOCK_DIR=/path/to/lasgo/stock \
  ./pipeline/run_stock_sync.sh

# Skip FTP fetch (files already local):
SKIP_FTP=1 MOOVIES_STOCK_FILE=Feed-20-03-2026.txt LASGO_STOCK_FILE=LASGO_19-Mar-2026-1201.xlsx \
  ./pipeline/run_stock_sync.sh
```

### B. Catalog Sync (every few days / on-demand)

Ingests new supplier titles, enriches with TMDB, builds/links films.

```
run_catalog_sync.sh
  └─ 00 → 01 (full) → 02 (full) → 03 → 04 → 05 → 06d (daily) → 07
      → append_pipeline_run_history (logs/pipeline_run_history.json)
```

```bash
# Run manually. Defaults: MOOVIES_CATALOG_DIR=supplier_exports/moovies/catalog,
# LASGO_CATALOG_DIR=supplier_exports/lasgo/catalog. MOOVIES_DIR/LASGO_DIR are ignored.
# Step 00 uses --mode catalog (FTP defaults: .../Moovies/Catalog, .../Lasgo/Catalog).
./pipeline/run_catalog_sync.sh

MOOVIES_CATALOG_DIR=/path/to/moovies/catalog LASGO_CATALOG_DIR=/path/to/lasgo/catalog \
  ./pipeline/run_catalog_sync.sh

# Point FTP at supplier catalog folders (in .env), e.g.:
#   MOOVIES_CATALOG_REMOTE_DIR=/TAPE_Film/Moovies/Catalog
#   LASGO_CATALOG_REMOTE_DIR=/TAPE_Film/Lasgo/Catalog

# Skip FTP, explicit files:
SKIP_FTP=1 MOOVIES_FILE=moovies_catalog.xlsx LASGO_FILE=Lasgo_20260318211252.xlsx \
  ./pipeline/run_catalog_sync.sh
```

---

## Running individual steps

Each numbered script accepts the same arguments as the root-level script
it wraps.  Always run from the project root:

```bash
# Example: import a Moovies file in full mode
venv/bin/python pipeline/01_import_moovies_raw.py moovies_catalog.xlsx --mode full

# Example: harmonize with dry-run
venv/bin/python pipeline/04_harmonize_supplier_offers.py --dry-run

# Example: daily enrichment with custom limits
venv/bin/python pipeline/06_enrich_catalog_with_tmdb_daily.py --max-rows 2000 --max-groups 800

# Example: recovery enrichment (all unattempted rows, no film_id filter)
venv/bin/python pipeline/06_enrich_catalog_with_tmdb_recovery.py --max-rows 50000 --max-groups 25000

# Example: rebuild films (full, not just unlinked)
venv/bin/python pipeline/07_build_films_from_catalog.py --full-rebuild
```

---

## Cron setup

See `cron_jobs.example` in this folder for ready-to-paste crontab entries.

---

## TMDB enrichment modes (step 06)

Two separate scripts, two different intents:

### Daily (`06_enrich_catalog_with_tmdb_daily.py`)

Used in the **catalog sync** cron pipeline.

| Filter | Value |
|--------|-------|
| `active` | `true` |
| `tmdb_last_refreshed_at` | `IS NULL` |
| `film_id` | `IS NULL` |

Groups by barcode, runs safe TMDB search, stamps `tmdb_last_refreshed_at`.
Does **not** revisit matched rows, retry stale not_found, refresh old metadata,
or touch already-linked rows.

### Recovery (`06_enrich_catalog_with_tmdb_recovery.py`)

For **manual** one-off rebuilds or initial bulk enrichment only.

| Filter | Value |
|--------|-------|
| `active` | `true` |
| `tmdb_last_refreshed_at` | `IS NULL` |

No `film_id` filter — processes all unattempted rows regardless of link status.

---

## Harmonization rules (step 04, catalog sync only)

Stock sync **skips** this step. For barcodes shared by multiple suppliers:

| Field | Leader | Rationale |
|-------|--------|-----------|
| `harmonized_title` | Lasgo | Cleaner, more accurate titles |
| `harmonized_format` | Moovies | Richer format descriptions |
| `harmonized_studio` | Moovies | More studio data |
| `harmonized_director` | Moovies | More director data |
| `media_release_date` | Lasgo | Trusted daily source for release dates |

Commercial fields (`cost_price`, `supplier_stock_status`, etc.) are
**never** cross-pollinated — each supplier keeps its own.

---

## Update whitelists (step 05)

Two modes, two whitelists for updating **existing** `catalog_items` rows:

### Stock sync (`--existing-only`)

Commercial fields only (**excludes** `media_release_date`):

- `supplier_stock_status`, `availability_status`
- `cost_price`, `calculated_sale_price`
- `supplier_last_seen_at`

### Catalog sync (default)

Full commercial set (including `media_release_date`) **plus** identity fields (from harmonized staging data):

- Same commercial fields as stock sync, **plus** `media_release_date`
- `title` (from `harmonized_title` in staging)
- `format` (from `harmonized_format` in staging)
- `director` (from `harmonized_director` in staging)
- `studio` (from `harmonized_studio` in staging)

### Never touched in either mode

- `tmdb_id`, `tmdb_title`, `tmdb_match_status`, `tmdb_last_refreshed_at`
- `film_id`, `film_linked_at`, `film_link_method`
- `genres`, `top_cast`, `country_of_origin`, `film_released`

---

## Logs and locking

- Logs: `logs/stock_sync_YYYYMMDD.log`, `logs/catalog_sync_YYYYMMDD.log`
- Locks: `.locks/stock_sync.lock`, `.locks/catalog_sync.lock` (prevents overlapping runs)
