# TAPE Film — Data Pipeline Operations Guide

This document covers every data process in the pipeline, from raw supplier file
ingestion through to the `films` table the agent uses. It is split into two
operational paths: **Catalog Sync** (new titles / enrichment) and **Stock Sync**
(price, quantity, release-date updates only).

---

## Architecture overview

```
Supplier files (.xlsx / .xls / .csv / .txt)
        │
        ▼
┌──────────────────────────┐
│  staging_moovies_raw     │   ◄── import_moovies_raw.py
│  staging_lasgo_raw       │   ◄── import_lasgo_raw.py
│  staging_shopify_raw     │   ◄── (Shopify GraphQL pull)
└──────────────────────────┘
        │
        ▼  normalize_supplier_products.py
┌──────────────────────────┐
│  staging_supplier_offers │   One row per (supplier, barcode).
│                          │   Pure normalisation only (own data).
└──────────────────────────┘
        │
        │
        ▼  harmonize_supplier_offers.py  (cross-supplier identity enrichment)
        │
        ▼  upsert_supplier_offers_to_catalog_items_preserve_tmdb.py
┌──────────────────────────┐
│  catalog_items           │   The master product table.
│                          │   Stock sync: commercial fields only.
│                          │   Catalog sync: commercial + harmonized identity.
└──────────────────────────┘
        │
        ▼  enrich_catalog_with_tmdb_v2.py
        │     Daily:    --daily (tmdb_last_refreshed_at IS NULL + film_id IS NULL)
        │     Recovery: (default, tmdb_last_refreshed_at IS NULL only)
        │
        ▼  build_films_from_catalog.py     (only rows with film_id IS NULL)
┌──────────────────────────┐
│  films                   │   The agent's canonical film entity.
│                          │   Linked back to catalog_items via film_id.
└──────────────────────────┘
```

---

## Database tables

| Table | Purpose |
|-------|---------|
| `staging_moovies_raw` | Raw Moovies supplier data, one row per product per import batch. Upsert key: `(supplier, upsert_key)`. |
| `staging_lasgo_raw` | Raw Lasgo supplier data (Blu-ray only). Inserted per batch. |
| `staging_shopify_raw` | Raw Shopify product/variant data pulled via GraphQL. |
| `staging_supplier_offers` | Normalised, harmonised supplier offers. One row per `(supplier, barcode)`. |
| `catalog_items` | Master product catalog. One row per `(supplier, barcode)` or `(supplier, shopify_variant_id)`. Enriched with TMDB metadata. Linked to `films` via `film_id`. |
| `films` | Canonical film entities for the agent. One row per unique `tmdb_id`. |

---

## 1. Loading raw data (pre-requisite for both pipelines)

### Moovies

```bash
cd /Users/simonpittaway/Dropbox/tape-film-ichoose

# Full catalog load (all fields — use for new catalog files)
venv/bin/python import_moovies_raw.py "/path/to/moovies_file.xlsx" --mode full

# Stock/price-only update (only updates known products)
venv/bin/python import_moovies_raw.py "/path/to/moovies_stock_file" --mode stock_cost --existing-only-in-raw
```

**How it works:**

- Reads `.xlsx`, `.xls`, `.csv`, or pipe-delimited `.txt` files.
- Maps columns flexibly (handles varying header names across Moovies files).
- Generates a `upsert_key` per row: `barcode:{barcode}`, `sku:{sku}`, or `row:{n}`.
- Upserts into `staging_moovies_raw` on `(supplier, upsert_key)` — safe to re-run.
- `--mode full`: writes all raw fields (title, format, studio, director, release date, etc.).
- `--mode stock_cost`: writes only barcode, SKU, price, qty, status.
- `--existing-only-in-raw`: in stock_cost mode, skips any barcode not already known in `staging_supplier_offers` or `catalog_items`. Prevents unknown products from entering the pipeline.

### Lasgo

```bash
# Full catalog load (filters to Blu-ray only via "Format L2" column)
venv/bin/python import_lasgo_raw.py "/path/to/LASGO_file.xlsx" --mode full

# Stock/price-only update
venv/bin/python import_lasgo_raw.py "/path/to/LASGO_file.xlsx" --mode stock_cost --existing-only-in-raw
```

**How it works:**

- Accepts a single file or a directory of files.
- **Filters out non-Blu-ray rows** by checking the `Format L2` column (CDs, vinyl, DVD, etc. are skipped).
- Maps Lasgo-specific headers (`EAN/Barcode`, `Selling Price Sterling`, `Free Stock`, `Artist` = director, `Label` = studio).
- Inserts into `staging_lasgo_raw` per batch.
- `--mode stock_cost` + `--existing-only-in-raw`: same protection as Moovies — only updates known barcodes.

### File naming conventions

| Supplier | Naming pattern | Example |
|----------|---------------|---------|
| Moovies (catalog) | varies | `moovies_catalog.xlsx`, `eazi1903Blu.xls` |
| Moovies (stock) | `Feed-DD-MM-YYYY.*` | `Feed-20-03-2026.txt` |
| Lasgo (catalog + stock) | `LASGO_DD-Mon-YYYY-HHMM.xlsx` | `LASGO_19-Mar-2026-1201.xlsx` |

---

## 2. Normalisation (`staging_supplier_offers`)

```bash
venv/bin/python normalize_supplier_products.py \
  --moovies-batch <BATCH_ID> \
  --lasgo-batch <BATCH_ID>
```

(Batch IDs are printed by the import scripts. The catalog sync shell script auto-detects the latest batch ID from each raw table.)

**What it does:**

- Reads raw rows for the given batch from each supplier's raw table.
- Creates one normalised row per `(supplier, barcode)` in `staging_supplier_offers`.
- Deduplicates within each batch (last row wins if same barcode appears twice).
- Upserts on `(supplier, barcode)`.

**Pure normalization only** — each supplier's data is mapped to the standard schema
with `harmonized_*` fields defaulting to the supplier's own values. No cross-supplier
logic runs here; that is handled entirely by step 4 (`harmonize_supplier_offers.py`).

**Pricing formula (for non-Shopify suppliers):**

- GBP cost × 2 = AUD base
- AUD base × 1.12 = total cost (shipping/handling)
- Total cost × (1 + margin%) = pre-GST sale price
- Pre-GST × 1.10 = final price (rounded up to .99)
- Margin tiers: ≤£15 → 32%, ≤£30 → 28%, ≤£40 → 24%, >£40 → 20%

---

## 3. Cross-supplier harmonization

```bash
venv/bin/python harmonize_supplier_offers.py
```

**When to run:** After normalisation, before upsert to `catalog_items`.

**`run_catalog_sync.sh` / `pipeline/run_catalog_sync.sh`** runs this automatically.  
**Stock sync** (`pipeline/run_stock_sync.sh`) **does not** run harmonization — it goes straight from normalize to `--existing-only` upsert so daily runs stay fast and do not rewrite harmonized identity or `media_release_date` on `catalog_items`.

**What it does:**

- Groups all `staging_supplier_offers` rows by barcode.
- For barcode groups with **multiple suppliers**, applies identity-field harmonization:

| Harmonized field | Leader | Rationale |
|-----------------|--------|-----------|
| `harmonized_title` | Lasgo (if present) | Lasgo has cleaner, more accurate titles |
| `harmonized_format` | Moovies (if present) | Moovies has richer format descriptions |
| `harmonized_studio` | Moovies (if present) | Moovies has more studio data |
| `harmonized_director` | Moovies (if present) | Moovies has more director data |
| `media_release_date` | Lasgo (if present) | Lasgo is the trusted daily source for physical release dates |

**Fields NEVER touched by harmonization:**

`supplier`, `supplier_sku`, `supplier_stock_status`, `availability_status`,
`cost_price`, `calculated_sale_price`, `supplier_currency`, `source_priority`,
`source_type`, `active`, `title` (native), `format` (native), `director` (native),
`studio` (native)

**Options:**

- `--dry-run` — prints what would change without writing
- `--barcode 5028836042020` — harmonize a single barcode (debugging)

**Safety:** Idempotent, only writes `harmonized_*` fields + `media_release_date` + `harmonized_from_supplier` + `harmonized_at`. Does not create or delete rows.

---

## 4. Upsert to `catalog_items`

```bash
# Catalog sync mode (default): inserts new + updates existing with commercial + harmonized identity
venv/bin/python upsert_supplier_offers_to_catalog_items_preserve_tmdb.py

# Stock sync mode: updates existing rows only, commercial fields only (no media_release_date), no inserts
venv/bin/python upsert_supplier_offers_to_catalog_items_preserve_tmdb.py --existing-only
```

Or via the pipeline folder:

```bash
venv/bin/python pipeline/05_upsert_to_catalog_items.py
venv/bin/python pipeline/05_upsert_to_catalog_items.py --existing-only
```

**How it works:**

- Reads all rows from `staging_supplier_offers`.
- Matches to existing `catalog_items` by:
  - Normal suppliers (Moovies, Lasgo): `(supplier, barcode)`
  - Tape Film (Shopify): `(supplier, shopify_variant_id)`, with barcode fallback
- Uses `upsert` with `on_conflict="supplier,barcode"` for new rows (safe against duplicates).
- Retries all Supabase calls on transient HTTP errors (RemoteProtocolError, timeouts, etc.).
- Prints progress during fetch, lookup, and update phases.

### Two update whitelists (mode-dependent)

**Stock sync (`--existing-only`) — commercial only, no release date on catalog:**

| Field | Description |
|-------|-------------|
| `supplier_stock_status` | Qty in stock |
| `availability_status` | e.g. `supplier_stock`, `supplier_out`, `preorder` |
| `cost_price` | Supplier cost (GBP) |
| `calculated_sale_price` | Calculated retail price (AUD) |
| `supplier_last_seen_at` | Timestamp of last sync |

`media_release_date` is **not** in the stock whitelist — physical release dates on `catalog_items` are updated during **catalog sync** (after harmonization) only.

**Catalog sync (default) — full commercial + harmonized identity:**

| Field | Source | Description |
|-------|--------|-------------|
| `media_release_date` | Harmonized staging (Lasgo-led in step 04) | Physical release date |
| `title` | `harmonized_title` from staging | Best title from cross-supplier harmonization |
| `format` | `harmonized_format` from staging | Best format from cross-supplier harmonization |
| `director` | `harmonized_director` from staging | Best director from cross-supplier harmonization |
| `studio` | `harmonized_studio` from staging | Best studio from cross-supplier harmonization |

Plus the same commercial fields as stock sync (including `media_release_date`).

This means when Moovies arrives later and improves a Lasgo row's harmonized fields
in `staging_supplier_offers`, the catalog sync will push those improved values into
the existing Lasgo `catalog_items` row's `title`, `format`, `director`, and `studio`,
and refresh `media_release_date` when harmonization picks a new value.

### Fields NEVER touched on existing rows (either mode)

- `film_id`, `film_link_status`, `film_link_method`, `film_linked_at`
- `tmdb_id`, `tmdb_title`, `tmdb_match_status`, `tmdb_last_refreshed_at`
- `genres`, `top_cast`, `country_of_origin`, `film_released`
- `tmdb_poster_path`, `tmdb_backdrop_path`
- `tmdb_vote_average`, `tmdb_vote_count`, `tmdb_popularity`

### New rows (when `--existing-only` is NOT set)

- Full catalog row is created via `map_offer_to_catalog_row()`.
- All TMDB/film fields are set to `NULL` — ready for enrichment.

---

## 4. TMDB enrichment

Two modes, two pipeline scripts, one underlying engine (`enrich_catalog_with_tmdb_v2.py`):

### Daily mode (for cron / catalog sync)

```bash
# Via pipeline wrapper:
venv/bin/python pipeline/06_enrich_catalog_with_tmdb_daily.py \
  --max-rows 4000 --max-groups 1500 --sleep-ms 350

# Directly:
venv/bin/python enrich_catalog_with_tmdb_v2.py --daily \
  --max-rows 4000 --max-groups 1500 --sleep-ms 350
```

Filters: `active = true` AND `tmdb_last_refreshed_at IS NULL` AND **`film_id IS NULL`**.

Only processes new, unlinked rows. Does **not** revisit matched rows, retry stale
not_found, refresh old metadata, or touch already-linked rows.

### Recovery mode (manual / one-off rebuild)

```bash
# Via pipeline wrapper:
venv/bin/python pipeline/06_enrich_catalog_with_tmdb_recovery.py \
  --max-rows 50000 --max-groups 25000 --sleep-ms 350

# Directly:
venv/bin/python enrich_catalog_with_tmdb_v2.py \
  --max-rows 50000 --max-groups 25000 --sleep-ms 350
```

Filters: `active = true` AND `tmdb_last_refreshed_at IS NULL` (no `film_id` filter).

Processes all unattempted rows regardless of link status. For initial bulk enrichment
or recovery after a failed run.

### How both modes work

- Groups rows by barcode (one TMDB API call per barcode group).
- For each group:
  1. Cleans the title aggressively (strips `4K`, `UHD`, `Ultra HD`, `Blu-ray`, `DVD`, `Steelbook`, `Limited Edition`, `Box Set`, `Dual Format`, `Remastered`, `Restored`, `Deluxe Edition`, edition noise, trailing format suffixes).
  2. Detects TV vs movie (presence of `Season`/`Series` keywords).
  3. Searches TMDB with cleaned title + optional year hint (from `film_released` only).
  4. Falls back to article-word removal variants if initial search misses.
  5. Fetches details + credits for matched TMDB ID.
  6. Updates all rows in the barcode group with TMDB metadata.

**Fields written to `catalog_items` on match:**

`tmdb_id`, `tmdb_title`, `tmdb_match_status` (`matched`), `director`, `film_released`,
`top_cast` (top 5), `genres` (top 4), `country_of_origin`, `tmdb_poster_path`,
`tmdb_backdrop_path`, `tmdb_vote_average`, `tmdb_vote_count`, `tmdb_popularity`,
`tmdb_last_refreshed_at`.

**On no match:** Sets `tmdb_match_status = 'not_found'` and `tmdb_last_refreshed_at = now`.

**Critical rule:** Once `tmdb_last_refreshed_at` is set (matched or not_found), the row is
**never re-attempted** by either mode. This is the "match once, never rematch" rule.

**Rate limiting:** 350ms sleep between groups + exponential backoff on 429/5xx/timeouts.

---

## 5. Build / link `films`

```bash
# Default (operational — only links rows with film_id IS NULL)
venv/bin/python build_films_from_catalog.py

# Recovery / full resync (relinks ALL matched rows)
venv/bin/python build_films_from_catalog.py --full-rebuild
```

**How it works:**

- Fetches `catalog_items` where `active = true`, `tmdb_match_status = 'matched'`, `tmdb_id IS NOT NULL`.
- Default mode adds: `film_id IS NULL` (skips already-linked rows).
- Prefetches all existing `films` rows (by `tmdb_id`) into memory.
- Groups catalog rows by `tmdb_id`.
- For each group:
  1. Picks the best representative row (richest TMDB metadata, then supplier priority: Tape Film > Moovies > Lasgo).
  2. If `tmdb_id` already exists in `films`: reuses that `film_id`.
  3. If new: inserts a `films` row with all TMDB metadata.
  4. Updates all `catalog_items` in the group with `film_id`, `film_link_status = 'linked'`, `film_link_method = 'tmdb_id'`, `film_linked_at`.
- Groups without a `tmdb_id` are **skipped** (no fallback film creation).

**Fields written to `films`:**

`title`, `original_title` (null), `film_released`, `director`, `tmdb_id`, `tmdb_title`,
`genres`, `top_cast`, `country_of_origin`, `tmdb_poster_path`, `tmdb_backdrop_path`,
`tmdb_vote_average`, `tmdb_vote_count`, `tmdb_popularity`, `metadata_source` (`tmdb`).

---

## 6. Maintenance: manual TMDB re-match

```bash
venv/bin/python maintenance_rematch_tmdb_from_csv.py \
  --csv /path/to/not_found_review.csv --sleep-ms 350
```

For rows previously marked `not_found` that you want to retry after cleaning titles
manually. Uses `manual_tmdb_id` or `manual_clean_title` columns from CSV. Updates only
the specific `catalog_items` IDs listed.

---

## 7. Publishing barcodes to Shopify

```bash
# Publish specific barcodes as draft Shopify products
venv/bin/python publish_selected_barcodes_to_shopify.py \
  --barcodes "5028836042020,5060974683031" --status draft

# Force a specific supplier
venv/bin/python publish_selected_barcodes_to_shopify.py \
  --barcodes "5028836042020" --supplier moovies --status draft

# From a file (one barcode per line)
venv/bin/python publish_selected_barcodes_to_shopify.py \
  --barcodes-file barcodes.txt --status draft

# Dry run (no Shopify writes)
venv/bin/python publish_selected_barcodes_to_shopify.py \
  --barcodes "5028836042020" --dry-run
```

Uses Shopify Admin GraphQL `productCreate` + `productVariantsBulkUpdate`.

---

## 8. FTP file fetching

Stock (inventory) and full-catalog supplier drops can use **different FTP folders** and **different local directories**. The fetch service selects behaviour with `--mode`:

```bash
# Operational / inventory feeds (default)
venv/bin/python scripts/fetch_supplier_files.py --mode stock

# Full catalog files from separate FTP paths (set *_CATALOG_REMOTE_DIR in .env)
venv/bin/python scripts/fetch_supplier_files.py --mode catalog
```

`pipeline/00_fetch_supplier_files.py` passes the same flags through to `supplier_fetch_service.run_from_argv`.  
`run_stock_sync.sh` calls `--mode stock`; `run_catalog_sync.sh` calls `--mode catalog`.

| Mode | Remote dirs (env) | Fallback if unset |
|------|-------------------|-------------------|
| `stock` | `MOOVIES_STOCK_REMOTE_DIR`, `LASGO_STOCK_REMOTE_DIR` | `MOOVIES_REMOTE_DIR` / `LASGO_REMOTE_DIR` (legacy), then `/TAPE_Film/Moovies/Inventory` and `.../Lasgo/Incoming` |
| `catalog` | `MOOVIES_CATALOG_REMOTE_DIR`, `LASGO_CATALOG_REMOTE_DIR` | **`/TAPE_Film/Moovies/Catalog` and `/TAPE_Film/Lasgo/Catalog`** (never inventory paths) |

| Mode | Local dirs | Default (if unset, when env not exported by shell) |
|------|------------|------------------------------------------------------|
| `stock` | `MOOVIES_STOCK_DIR`, `LASGO_STOCK_DIR` | `/opt/tape-film/sftp/moovies` and `.../lasgo` |
| `catalog` | `MOOVIES_CATALOG_DIR`, `LASGO_CATALOG_DIR` | **`supplier_exports/moovies/catalog` and `supplier_exports/lasgo/catalog` under cwd** — does **not** fall back to stock dirs |

Optional glob overrides: `MOOVIES_STOCK_GLOB` / `LASGO_STOCK_GLOB`, `MOOVIES_CATALOG_GLOB` / `LASGO_CATALOG_GLOB`; otherwise `MOOVIES_GLOB` / `LASGO_GLOB` (default `Feed-*` / `LASGO_*`).

Example **stock** FTP paths (`--mode stock`; use `*_STOCK_REMOTE_DIR`, not `*_CATALOG_*`):

```env
MOOVIES_STOCK_REMOTE_DIR=/TAPE_Film/Moovies/Inventory
LASGO_STOCK_REMOTE_DIR=/TAPE_Film/Lasgo/Incoming
```

If unset, stock mode falls back to legacy `MOOVIES_REMOTE_DIR` / `LASGO_REMOTE_DIR`, then defaults `/TAPE_Film/Moovies/Inventory` and `/TAPE_Film/Lasgo/Incoming`.

Catalog mode **never** uses those inventory defaults: without `*_CATALOG_REMOTE_DIR` it uses `/TAPE_Film/Moovies/Catalog` and `/TAPE_Film/Lasgo/Catalog`.

Example **catalog** FTP paths (`--mode catalog`):

```env
MOOVIES_CATALOG_REMOTE_DIR=/TAPE_Film/Moovies/Catalog
LASGO_CATALOG_REMOTE_DIR=/TAPE_Film/Lasgo/Catalog
```

| FTP credentials (shared) | `FTP_HOST`, `FTP_USER`, `FTP_PASSWORD`, `FTP_PORT`, `FTP_USE_TLS` | in `.env`; legacy `SFTP_*` aliases for host/user/password |
| FTP per supplier (optional) | `MOOVIES_FTP_*`, `LASGO_FTP_*` | Each field falls back to shared `FTP_*` / `SFTP_*`, then **Lasgo inherits `MOOVIES_FTP_*`** (and Moovies can inherit `LASGO_FTP_*`) so one server is enough. You may put `host:port` in `*_FTP_HOST`; `*_FTP_PORT` overrides the inline port if set. |
| `.env` passwords | Values containing `#` must be **double-quoted** (e.g. `MOOVIES_FTP_PASSWORD="…#…"`) or the rest of the line is treated as a comment. |

Supports plain FTP and explicit FTP_TLS (auto-detect by default).

---

## Operational pipelines (cron-ready)

### A. Daily stock sync (`run_stock_sync.sh` / legacy `run_daily_stock_sync.sh`)

**Purpose:** Update stock quantities, prices, and availability for known products only. **No** harmonization, **no** `media_release_date` writes to `catalog_items`. No new titles, no TMDB, no films rebuild.

**Recommended cadence:** Daily (e.g. 00:30).

**Source files:** From local disk (after FTP fetch), auto-detected by latest modified file in configured directories.

**Sequence:**

| Step | Script | What it does |
|------|--------|-------------|
| 0 (pre) | `pipeline/00_fetch_supplier_files.py --mode stock` | Downloads latest **stock** files from FTP |
| 1 | `import_moovies_raw.py --mode stock_cost --existing-only-in-raw` | Updates Moovies raw: only known barcodes, only price/qty/status |
| 2 | `import_lasgo_raw.py --mode stock_cost --existing-only-in-raw` | Updates Lasgo raw: only known barcodes, only price/qty/stock |
| 3 | `normalize_supplier_products.py` | Re-normalises into `staging_supplier_offers` |
| 4 | `pipeline/05_upsert_to_catalog_items.py --existing-only` | Updates `catalog_items` commercial fields only (excludes `media_release_date`; no inserts) |

**Run command:**

```bash
MOOVIES_STOCK_DIR=/path/to/moovies/files \
LASGO_STOCK_DIR=/path/to/lasgo/files \
./scripts/run_daily_stock_sync.sh
```

### B. Catalog sync (`run_daily_catalog_sync.sh`)

**Purpose:** Ingest new supplier titles, enrich with TMDB, build/link films.

**Recommended cadence:** Every few days or on-demand (e.g. Mon/Wed/Fri 02:15).

**Source data:** Latest batches already in `staging_moovies_raw` and `staging_lasgo_raw` (you must import raw files first — see Section 1).

**Sequence:**

| Step | Script | What it does |
|------|--------|-------------|
| Baseline | inline Python | Snapshots catalog_items/films counts before run |
| 1 | `normalize_supplier_products.py` | Normalises latest raw batches → `staging_supplier_offers` |
| 1b | `harmonize_supplier_offers.py` | Cross-supplier harmonization on shared barcodes |
| 2 | `upsert_…_preserve_tmdb.py` | Inserts new + updates existing (commercial + harmonized identity) |
| 3 | `enrich_catalog_with_tmdb_v2.py --daily` | Enriches new unlinked rows with TMDB |
| 4 | `build_films_from_catalog.py` | Links only unlinked rows (`film_id IS NULL`) to `films` |
| Final | inline Python | Reports deltas: new catalog rows, new matches, new films |

**Run command:**

```bash
# First, import the latest raw files:
venv/bin/python import_moovies_raw.py "/path/to/moovies_catalog.xlsx" --mode full
venv/bin/python import_lasgo_raw.py "/path/to/LASGO_file.xlsx" --mode full

# Then run catalog sync:
./scripts/run_daily_catalog_sync.sh
```

**Environment tunables:**

| Var | Default | Meaning |
|-----|---------|---------|
| `TMDB_MAX_ROWS` | 4000 | Max catalog rows fetched for enrichment |
| `TMDB_MAX_GROUPS` | 1500 | Max barcode groups processed per run |
| `TMDB_SLEEP_MS` | 350 | Delay between TMDB API calls (ms) |
| `NORMALIZE_LATEST_RAW` | 1 | Set to 0 to skip normalisation step |
| `SYNC_FILMS_FROM_CATALOG` | 1 | Set to 0 to skip films build step |

### C. Unattended enrichment + films build

```bash
venv/bin/python scripts/run_enrichment_then_build_films.py
```

Loops enrichment until `pending_enrichment = 0`, then runs `build_films_from_catalog.py`.
Retries on failure with 60s backoff. Logs to `LOG_PATH` if set.

---

## Cron schedule (example for VM)

```cron
# Prefer pipeline wrappers (fetch is included inside each sync script).
# Daily stock sync (00:30) — fetches stock FTP + runs 00→03→04 upsert (no harmonize)
30 0 * * * MOOVIES_STOCK_DIR=/opt/tape-film/sftp/moovies/stock LASGO_STOCK_DIR=/opt/tape-film/sftp/lasgo/stock /opt/tape-film/pipeline/run_stock_sync.sh

# Catalog growth + enrichment (Mon/Wed/Fri 02:15) — fetches catalog FTP + runs 01–07
15 2 * * 1,3,5 MOOVIES_CATALOG_DIR=/opt/tape-film/sftp/moovies/catalog LASGO_CATALOG_DIR=/opt/tape-film/sftp/lasgo/catalog /opt/tape-film/pipeline/run_catalog_sync.sh
```

---

## Logging

All pipeline scripts log to `logs/` in the project root:

- `logs/daily_stock_sync_YYYYMMDD.log`
- `logs/daily_catalog_sync_YYYYMMDD.log`
- `logs/enrich_then_films_*.log` (for unattended runs)
- `logs/fetch_supplier_files_YYYYMMDD.log`

Locking prevents overlapping runs (`.locks/` directory).

---

## Quick reference: which script for which task

| I want to… | Run this |
|------------|----------|
| Load a new Moovies catalog file | `venv/bin/python import_moovies_raw.py <file> --mode full` |
| Load a new Lasgo catalog file | `venv/bin/python import_lasgo_raw.py <file> --mode full` |
| Update stock/prices from a Moovies file | `venv/bin/python import_moovies_raw.py <file> --mode stock_cost --existing-only-in-raw` |
| Update stock/prices from a Lasgo file | `venv/bin/python import_lasgo_raw.py <file> --mode stock_cost --existing-only-in-raw` |
| Normalise raw → supplier offers | `venv/bin/python normalize_supplier_products.py` |
| Harmonize across suppliers | `venv/bin/python harmonize_supplier_offers.py` |
| Move offers → catalog (catalog sync) | `venv/bin/python pipeline/05_upsert_to_catalog_items.py` |
| Move offers → catalog (stock sync) | `venv/bin/python pipeline/05_upsert_to_catalog_items.py --existing-only` |
| Enrich catalog with TMDB (daily) | `venv/bin/python pipeline/06_enrich_catalog_with_tmdb_daily.py` |
| Enrich catalog with TMDB (recovery) | `venv/bin/python pipeline/06_enrich_catalog_with_tmdb_recovery.py` |
| Link catalog → films (new only) | `venv/bin/python build_films_from_catalog.py` |
| Link catalog → films (full resync) | `venv/bin/python build_films_from_catalog.py --full-rebuild` |
| Run full daily stock pipeline | `./scripts/run_daily_stock_sync.sh` |
| Run full catalog pipeline | `./scripts/run_daily_catalog_sync.sh` |
| Fetch stock files from FTP | `venv/bin/python scripts/fetch_supplier_files.py --mode stock` |
| Fetch catalog files from FTP | `venv/bin/python scripts/fetch_supplier_files.py --mode catalog` |
| Publish barcodes to Shopify | `venv/bin/python publish_selected_barcodes_to_shopify.py --barcodes "…"` |
| Re-match specific not_found rows | `venv/bin/python maintenance_rematch_tmdb_from_csv.py --csv <file>` |
