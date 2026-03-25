# Tape Film — Architecture

## Overview

Tape Film is a physical film media business selling Blu-ray and 4K films.
The system ingests supplier catalogs, normalises and harmonises them into a
unified product catalog, enriches them with TMDB metadata, builds a canonical
film library, and publishes selected products to Shopify.

---

## Layer Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Shopify Storefront + Agent (app/ — Remix + TypeScript) │
│  Customer-facing search, recommendations, orders        │
└───────────────────────────┬─────────────────────────────┘
                            │ reads from
┌───────────────────────────▼─────────────────────────────┐
│  films                                                  │
│  Canonical film entity. One row per unique tmdb_id.     │
│  The agent's primary entity for search and display.     │
└───────────────────────────┬─────────────────────────────┘
                            │ linked from
┌───────────────────────────▼─────────────────────────────┐
│  catalog_items                                          │
│  Master product table. One row per (supplier, barcode). │
│  Contains identity, commercial, TMDB, and film-link     │
│  fields. Source of truth for availability and pricing.   │
└───────────────────────────┬─────────────────────────────┘
                            │ published from
┌───────────────────────────▼─────────────────────────────┐
│  staging_supplier_offers                                │
│  Normalised + harmonised supplier offers.               │
│  One row per (supplier, barcode). Includes both native  │
│  and harmonized_* identity fields.                      │
└───────────────────────────┬─────────────────────────────┘
                            │ normalised from
┌───────────────────────────▼─────────────────────────────┐
│  staging_moovies_raw  /  staging_lasgo_raw              │
│  staging_shopify_raw                                    │
│  Raw supplier data. Unmodified except for column mapping│
└─────────────────────────────────────────────────────────┘
```

---

## Table Ownership

Each table has a clear owner — the script/step that is allowed to write to it.

| Table | Write owner | Read consumers |
|---|---|---|
| `staging_moovies_raw` | Step 01 (import_moovies_raw) | Step 03 |
| `staging_lasgo_raw` | Step 02 (import_lasgo_raw) | Step 03 |
| `staging_shopify_raw` | Shopify sync (separate process) | Step 03 |
| `staging_supplier_offers` | Step 03 (normalize) + Step 04 (harmonize) | Step 05 |
| `catalog_items` | Step 05 (upsert) + Step 06 (TMDB) + Step 07 (film link) | Agent, Shopify publisher |
| `films` | Step 07 (build_films) | Agent, storefront |

---

## Write Boundaries

The most critical architectural constraint is **field-level write isolation**.
Different pipeline steps own different fields on the same table, and must
never overwrite each other's data.

### catalog_items field ownership

| Field group | Written by | Never written by |
|---|---|---|
| **Commercial** (stock, price, availability; **release date** catalog sync only) | Step 05 — full set in catalog mode; stock mode omits `media_release_date` | Step 06, Step 07 |
| **Identity** (title, format, director, studio) | Step 05 (catalog sync only) | Step 06, Step 07 |
| **TMDB metadata** (tmdb_id, tmdb_title, genres, cast, poster, etc.) | Step 06 only | Step 05, Step 07 |
| **Film linkage** (film_id, film_link_status, film_link_method) | Step 07 only | Step 05, Step 06 |

These boundaries are enforced programmatically in:
- `app/rules/catalog_update_rules.py` — whitelists for Step 05
- `app/rules/tmdb_rules.py` — TMDB field definitions for Step 06

### staging_supplier_offers field ownership

| Field group | Written by | Never written by |
|---|---|---|
| **Native** (title, format, director, studio, costs) | Step 03 (normalize) | Step 04 |
| **Harmonized** (harmonized_*, media_release_date) | Step 03 (defaults) + Step 04 (cross-supplier) | Step 05 |

These boundaries are enforced in:
- `app/rules/harmonization_rules.py` — writable/protected field sets

---

## Source-of-Truth Rules

### Identity fields
- **harmonized_title**: Lasgo title leads (cleaner, no format noise)
- **harmonized_format**: Moovies format leads (richer format descriptions)
- **harmonized_studio**: Moovies studio leads
- **harmonized_director**: Moovies director leads
- **media_release_date**: Lasgo release date leads (trusted daily source)

### TMDB matching
- **Match once, never rematch**: Once `tmdb_last_refreshed_at` is stamped,
  the row is permanently locked — whether matched or not_found.
- **Daily mode**: Only enriches rows where `film_id IS NULL` (new, unlinked).
  Safe for cron.
- **Recovery mode**: Enriches all unattempted rows regardless of film_id.
  For one-off rebuilds only.

### Film identity
- **One film per tmdb_id**: The `films` table is a canonical deduplicated entity.
  Multiple catalog_items with different barcodes/formats can link to the same film.
- **Film creation**: Only happens in Step 07 (build_films_from_catalog).
  The most metadata-rich catalog row is chosen as the representative.

### Supplier precedence
- **Tape Film** (priority 0) — own stock, always preferred
- **Moovies** (priority 1) — primary third-party supplier
- **Lasgo** (priority 2) — secondary third-party supplier

### Pricing
- Sale price: GBP cost → AUD conversion → landed markup → margin → GST → .99 rounding
- Shopify cost: GBP cost → AUD conversion → landed markup (no margin, no GST)
- Tape Film products: price and cost taken as-is from Shopify (already in AUD)

---

## Pipeline Modes

### Catalog Sync (full pipeline)

```
fetch files → import raw → normalize → harmonize → upsert (catalog mode)
→ TMDB enrich (daily) → build films
```

Runs periodically. Processes new titles, updates identity fields, enriches
with TMDB, creates/links films.

### Stock Sync (operational)

```
fetch files → import raw → normalize → upsert (stock mode, --existing-only)
```

Runs frequently (e.g. daily). Updates **commercial** fields only on existing rows
(price, stock, availability, `supplier_last_seen_at`). **Skips** harmonization and
**does not** write `media_release_date` to `catalog_items` (release dates refresh on
catalog sync only). Never inserts new rows. Never touches identity, TMDB, or film-link fields.

### Shopify Publish (manual)

```
select barcodes → fetch catalog → pick best offer → create Shopify product
```

Not part of the automated pipeline. Triggered manually when new products
are ready to go live.

---

## Module Structure

```
app/
├── rules/                    # Pure business logic (no I/O, no DB)
│   ├── harmonization_rules   # Field ownership, pick_best_* functions
│   ├── catalog_update_rules  # Update whitelists, protected fields
│   ├── tmdb_rules            # Match-once logic, enrichment filters
│   ├── supplier_precedence   # Priority ranking, best-offer selection
│   ├── pricing_rules         # Margins, conversion, sale price calc
│   └── content_classification_rules  # film | tv | unknown (health + TMDB routing)
│
├── clients/                  # External service wrappers (I/O boundary)
│   ├── supabase_client       # Retry-aware DB client, pagination
│   ├── tmdb_client           # TMDB API with rate limiting + backoff
│   ├── shopify_client        # OAuth + GraphQL
│   └── ftp_client            # FTP/FTPS with TLS auto-detection
│
├── helpers/                  # Shared utilities
│   ├── retry_helpers         # Generic retry with exponential backoff
│   ├── text_helpers          # clean_text, slugify, parse_*, chunked
│   ├── tmdb_match_helpers    # Title cleaning, search type detection
│   └── catalog_match_helpers # Film/catalog linking utilities
│
├── services/                 # Pipeline step implementations (I/O + rules)
│   ├── supplier_fetch_service.py
│   ├── moovies_import_service.py
│   ├── lasgo_import_service.py
│   ├── normalize_offers_service.py
│   ├── harmonize_offers_service.py
│   ├── catalog_upsert_service.py
│   ├── catalog_offer_mapping.py   # map_offer_to_catalog_row for upsert + legacy publish
│   ├── tmdb_enrichment_service.py
│   └── film_builder_service.py
│
├── observability/            # Metrics, log parsing, pipeline run history, schema
│   ├── catalog_metrics.py
│   ├── pipeline_history_schema.py
│   ├── pipeline_log_parser.py
│   └── pipeline_run_history.py
│
├── models/                   # Data models / types
│   └── (future: pydantic models)
│
└── config/                   # Configuration
    └── (future: pydantic settings)

scripts/
├── pipeline/                 # Numbered step wrappers + orchestrators
├── publish/                  # Shopify product publisher (standalone)
├── maintenance/              # One-off corrective tools
└── observability/            # Health reports and audits

tests/
└── rules/                    # Unit tests for all rules modules
    ├── test_harmonization_rules
    ├── test_catalog_update_rules
    ├── test_tmdb_rules
    ├── test_supplier_precedence_rules
    ├── test_pricing_rules
    └── test_content_classification_rules
```

### Root-level Python shims (same CLI)

These repo-root entry points remain supported for cron and muscle memory.
Each adds the project root to `sys.path` and delegates to `app.services`:

| Root script | Service |
|---|---|
| `import_moovies_raw.py` | `moovies_import_service.run_from_argv` |
| `import_lasgo_raw.py` | `lasgo_import_service.run_from_argv` |
| `normalize_supplier_products.py` | `normalize_offers_service.run_from_argv` |
| `harmonize_supplier_offers.py` | `harmonize_offers_service.run_from_argv` |
| `upsert_supplier_offers_to_catalog_items_preserve_tmdb.py` | `catalog_upsert_service.run_from_argv` |
| `enrich_catalog_with_tmdb_v2.py` | `tmdb_enrichment_service.run_from_argv` |
| `build_films_from_catalog.py` | `film_builder_service.run_from_argv` |

`pipeline/00–07_*.py` wrappers each call **one** `run_from_argv()` in-process
(no subprocess). `scripts/fetch_supplier_files.py` delegates to
`supplier_fetch_service.run_from_argv` (`--mode stock` for inventory paths,
`--mode catalog` for full-catalog FTP folders; see `supplier_fetch_service.py`).

---

## Health Observability

Observability scripts provide structured health reporting and **trendability**
via `logs/pipeline_run_history.json` (appended after each catalog/stock sync).
All follow the same standard interface:

```
--format text|json|csv       Output format (default: text)
--output <path>              Write to file instead of stdout
--since-days N               Time window where relevant
Non-zero exit code           1 = WARNING, 2 = CRITICAL
```

Thresholds are configurable via environment variables. Non-zero exit codes
make these scripts usable as cron health checks or CI gates.

Catalog content classification paginates all active `catalog_items` in pages of
`HEALTH_CLASSIFICATION_PAGE_SIZE` (default **1000**, aligned with PostgREST
`max-rows`). If you raise API limits, you may increase the page size.
`HEALTH_CLASSIFICATION_MAX_ROWS` caps total rows processed (default 250000);
exceeding it raises a clear error instead of silent truncation.

### catalog_health_report.py

The primary health check. Queries `catalog_items` and `films` live.

| Section | Metrics |
|---|---|
| Coverage | Active catalog items, total films, breakdown by supplier |
| Linkage | Content classification from **catalog_items** fields only (`title`, `format`, `media_type`, `category`, `notes`, `source_type`, TMDB status — no `harmonized_*`); film linked/unlinked + % **among film-classified rows**; all-active film_id %; TMDB matched/not_found/pending, match rate % |
| Commercial | Missing sale price (count + %), missing cost price |
| Freshness | Latest and oldest `supplier_last_seen_at` |
| Exceptions | Null barcode rows, duplicate films by tmdb_id, null title rows |

**Thresholds:**

| Condition | Level | Default | Env var |
|---|---|---|---|
| Film linkage < N% (primary KPI) | CRITICAL | 70% | `HEALTH_FILM_LINK_CRITICAL_PCT` |
| Film linkage < N% | WARNING | 85% | `HEALTH_FILM_LINK_MIN_PCT` |
| Stale TMDB pending > N rows (>7 days) | WARNING | 50 | `HEALTH_TMDB_STALE_MAX` |
| Missing sale price > N% | WARNING | 5% | `HEALTH_MISSING_PRICE_MAX_PCT` |
| Null barcode rows > 0 | CRITICAL | 0 | `HEALTH_NULL_BARCODE_MAX` |
| Duplicate films by tmdb_id > 0 | CRITICAL | 0 | `HEALTH_DUPLICATE_FILMS_MAX` |

### tmdb_match_audit.py

Deep TMDB-specific quality audit.

| Section | Metrics |
|---|---|
| Overall | matched, not_found, no_clean_title, pending, match rate % |
| Match rate by window | 7-day and 30-day match rates (trendable over time) |
| Stale pending | Rows pending enrichment for >N days |
| Not-found patterns | Top 20 cleaned title patterns among not_found rows |
| Rematch candidates | not_found rows with stock >0, sorted by stock (manual review priority) |
| Low-quality matches | Matched rows missing poster, genres, or cast |

**Thresholds:**

| Condition | Level | Default | Env var |
|---|---|---|---|
| Match rate < N% | WARNING | 70% | `TMDB_MATCH_RATE_MIN_PCT` |
| Stale pending > N rows (>7 days) | WARNING | 50 | `TMDB_PENDING_STALE_MAX` |
| Low-quality matches > N | WARNING | 20 | `TMDB_LOW_QUALITY_MAX` |

### pipeline_run_report.py

Parses pipeline log files to report on run health.

| Section | Metrics |
|---|---|
| Run summary | Start/end time, duration, pipeline type, completed status |
| Per-step | Rows processed, inserts vs updates, retries, failures |
| Totals | Aggregate inserts, updates, retries, failures |
| Environment | Supplier file names used, lock encountered |

Non-zero exit code if any run has failures or did not complete.

Log parsing lives in `app/observability/pipeline_log_parser.py` (shared with
history appender).

### append_pipeline_run_history.py

Runs at the end of `run_catalog_sync.sh` and `run_stock_sync.sh` (non-fatal).
Reads the **current** pipeline log plus a **live** `gather_metrics()` snapshot,
then atomically appends one object to `logs/pipeline_run_history.json`:

| Field | Source |
|---|---|
| `inserts` / `updates` | `Operational sync complete. inserted=… updated=…` in the log |
| `tmdb_matched_pct` / `film_linked_pct` | Post-run DB snapshot |
| `failures` | ERROR/FAIL lines attributed to steps in the log |
| `health_exit_code` | Same thresholds as `catalog_health_report` at append time |

Cap retained runs with `PIPELINE_HISTORY_MAX_RUNS` (default `500`) or override
path with `PIPELINE_HISTORY_FILE`.

Records are validated with `app.observability.pipeline_history_schema` before append.

```bash
# Inspect last few runs (requires jq)
jq '.runs[-5:]' logs/pipeline_run_history.json

# Trend summary + schema check
venv/bin/python scripts/observability/pipeline_trend_report.py --last 10
venv/bin/python scripts/observability/pipeline_trend_report.py --validate-only
```

### Usage examples

```bash
# Quick text health check (exits non-zero on problems)
venv/bin/python scripts/observability/catalog_health_report.py

# Machine-readable JSON for monitoring / dashboards
venv/bin/python scripts/observability/catalog_health_report.py --format json

# TMDB audit to CSV for spreadsheet review
venv/bin/python scripts/observability/tmdb_match_audit.py --format csv --output tmdb_audit.csv

# Pipeline run comparison (last 5 runs)
venv/bin/python scripts/observability/pipeline_run_report.py --last 5 --format json

# Dry-run: show JSON record that would be appended to history
venv/bin/python scripts/observability/append_pipeline_run_history.py --dry-run --log-file logs/catalog_sync_20260322.log

# Write health report to file for cron alerting
venv/bin/python scripts/observability/catalog_health_report.py --format json --output /tmp/health.json
echo $?  # 0 = healthy, 1 = warning, 2 = critical
```

---

## Maintenance Scripts

One-off corrective tools in `scripts/maintenance/`. Not part of the
automated pipeline. Each script has a structured header documenting:

- **Purpose**: What it fixes
- **Tables/fields mutated**: Exact write scope
- **Safe mode**: Whether `--dry-run` is supported
- **Cron-safe**: Always NO — manual use only

| Script | Purpose |
|---|---|
| `maintenance_rematch_tmdb_from_csv.py` | Re-attempt TMDB matching from a CSV review file |
| `clear_bad_links_opt.py` | Clear incorrect film links so rows can be re-enriched |
| `relink_suspicious_catalog_rows.py` | Fix suspicious film links via TMDB re-search |
| `relink_unlinked_catalog_rows.py` | Link orphaned catalog rows to existing/new films |
| `refresh_tmdb_metadata_in_catalog.py` | Refresh stale TMDB metadata (violates match-once — use carefully) |

---

## Testing Strategy

The highest-value tests target **rules**, not scripts:

| Test area | What to verify |
|---|---|
| Harmonization | Correct field ownership for mixed Moovies/Lasgo barcode groups |
| Catalog updates | Correct whitelist applied in stock-sync vs catalog-sync mode |
| TMDB rules | Match-once lock is respected; daily vs recovery filters work |
| Film builder | Correct representative selection; one film per tmdb_id |
| Supplier precedence | Tape Film preferred; correct ranking by availability/stock/price |
| Pricing | Margin tiers correct; .99 rounding; GBP→AUD conversion |

Rules modules are pure functions with no I/O dependencies, making them
trivial to unit test without mocking.
