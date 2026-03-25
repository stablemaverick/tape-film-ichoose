# Tape Film — Scripts Inventory

All scripts currently live in the project root at:
`/Users/simonpittaway/Dropbox/tape-film-ichoose/`

This document describes every script, its purpose, dependencies, and how it fits
into the data pipeline.

---

## Pipeline Scripts (Data Flow)

These run sequentially. Each step depends on the output of the previous step.

```
Supplier Files → Raw → Normalize → Harmonize → Catalog → Enrich → Films
```

### Step 00 — Fetch Supplier Files

| | |
|---|---|
| **File** | `scripts/fetch_supplier_files.py` |
| **Wrapper** | `pipeline/00_fetch_supplier_files.py` |
| **Purpose** | Downloads latest supplier files from FTP (Lasgo + Moovies); `--mode stock` vs `--mode catalog` for remote/local paths |
| **Input** | FTP in `.env`: `MOOVIES_FTP_*` alone can serve both suppliers (Lasgo falls back to Moovies after `FTP_*`); or set `LASGO_FTP_*` for a separate host; optional `*_STOCK_*` / `*_CATALOG_*` path overrides |
| **Output** | Supplier files saved to local directories |
| **Dependencies** | `ftplib`, `dotenv` |
| **Idempotent** | Yes — overwrites local files with latest from FTP |

### Step 01 — Import Moovies Raw

| | |
|---|---|
| **File** | `import_moovies_raw.py` |
| **Wrapper** | `pipeline/01_import_moovies_raw.py` |
| **Purpose** | Loads Moovies supplier catalog file into `staging_moovies_raw` |
| **Input** | Excel/CSV/TXT file (e.g. `Feed-21-03-2026.xlsx`) |
| **Output** | Rows in `staging_moovies_raw` table |
| **Modes** | `--mode full` (all columns) or `--mode stock_cost` (price/qty only) |
| **Flags** | `--existing-only-in-raw` (only update known barcodes, for stock sync) |
| **Dependencies** | `pandas`, `supabase`, `dotenv` |
| **Idempotent** | Yes — upserts by barcode |

### Step 02 — Import Lasgo Raw

| | |
|---|---|
| **File** | `import_lasgo_raw.py` |
| **Wrapper** | `pipeline/02_import_lasgo_raw.py` |
| **Purpose** | Loads Lasgo supplier catalog file into `staging_lasgo_raw` (Blu-ray only) |
| **Input** | Excel file (e.g. `LASGO_20-Mar-2026-1201.xlsx`) |
| **Output** | Rows in `staging_lasgo_raw` table |
| **Modes** | `--mode full` or `--mode stock_cost` |
| **Flags** | `--existing-only-in-raw` |
| **Dependencies** | `pandas`, `supabase`, `dotenv`, `xlrd` |
| **Idempotent** | Yes — upserts by barcode |
| **Note** | Filters to Blu-ray Format L2 only, skips all other media types |

### Step 03 — Normalize Supplier Products

| | |
|---|---|
| **File** | `normalize_supplier_products.py` |
| **Wrapper** | `pipeline/03_normalize_supplier_products.py` |
| **Purpose** | Maps raw supplier data into unified `staging_supplier_offers` table |
| **Input** | `staging_moovies_raw`, `staging_lasgo_raw`, `staging_shopify_raw` |
| **Output** | Rows in `staging_supplier_offers` (one row per supplier + barcode) |
| **Flags** | `--moovies-batch`, `--lasgo-batch`, `--shopify-batch` (batch IDs to process) |
| **Dependencies** | `supabase`, `dotenv` |
| **Idempotent** | Yes — upserts by (supplier, barcode) |
| **Note** | Pure normalization only. No cross-supplier logic. Each supplier's `harmonized_*` fields default to its own native values. |

### Step 04 — Harmonize Supplier Offers

| | |
|---|---|
| **File** | `harmonize_supplier_offers.py` |
| **Wrapper** | `pipeline/04_harmonize_supplier_offers.py` |
| **Purpose** | Cross-supplier identity enrichment on shared barcodes |
| **Input** | `staging_supplier_offers` |
| **Output** | Updated `harmonized_*` fields in `staging_supplier_offers` |
| **Flags** | `--dry-run` (preview changes), `--barcode` (single barcode debug) |
| **Dependencies** | `supabase`, `dotenv`, `httpx` (for retry logic) |
| **Idempotent** | Yes — recalculates and only updates if changed |
| **Rules** | `harmonized_title` from Lasgo, `harmonized_format`/`studio`/`director` from Moovies, `media_release_date` from Lasgo |
| **Scope** | Only touches `harmonized_*` fields and `media_release_date`. Never touches commercial or TMDB fields. |

### Step 05 — Upsert to Catalog Items

| | |
|---|---|
| **File** | `upsert_supplier_offers_to_catalog_items_preserve_tmdb.py` |
| **Wrapper** | `pipeline/05_upsert_to_catalog_items.py` |
| **Purpose** | Moves supplier offers into `catalog_items` master product table |
| **Input** | `staging_supplier_offers` |
| **Output** | Rows in `catalog_items` |
| **Modes** | Default (catalog sync) or `--existing-only` (stock sync) |
| **Dependencies** | `supabase`, `dotenv`, `httpx` (for retry logic) |
| **Idempotent** | Yes — upserts by (supplier, barcode) |
| **Update rules** | |
| — Stock sync (`--existing-only`) | Commercial fields only (**excludes** `media_release_date`): `supplier_stock_status`, `availability_status`, `cost_price`, `calculated_sale_price`, `supplier_last_seen_at` |
| — Catalog sync (default) | Full commercial set (including `media_release_date`) + identity: `title`, `format`, `director`, `studio` (from harmonized staging after step 04) |
| — Never touched | `tmdb_id`, `tmdb_title`, `tmdb_match_status`, `tmdb_last_refreshed_at`, `film_id`, `film_linked_at`, `film_link_method`, `genres`, `top_cast`, `country_of_origin`, `film_released` |

### Step 06 — TMDB Enrichment

| | |
|---|---|
| **File** | `enrich_catalog_with_tmdb_v2.py` |
| **Daily wrapper** | `pipeline/06_enrich_catalog_with_tmdb_daily.py` |
| **Recovery wrapper** | `pipeline/06_enrich_catalog_with_tmdb_recovery.py` |
| **Purpose** | Enriches catalog_items with TMDB metadata (poster, cast, genres, etc.) |
| **Input** | `catalog_items` rows needing enrichment |
| **Output** | Updated TMDB fields in `catalog_items` |
| **Helper** | `app/helpers/tmdb_match_helpers.py` (root `tmdb_match_helpers.py` shim for legacy imports) |
| **Dependencies** | `supabase`, `requests`, `dotenv` |
| **Daily mode** (`--daily`) | Filters: `tmdb_last_refreshed_at IS NULL` AND `film_id IS NULL`. Safe for cron. |
| **Recovery mode** (default) | Filters: `tmdb_last_refreshed_at IS NULL` only. For bulk rebuilds. |
| **Flags** | `--max-rows`, `--max-groups`, `--sleep-ms` |
| **Critical rule** | Once `tmdb_last_refreshed_at` is set (matched or not_found), the row is never re-attempted. Match once, never rematch. |
| **Rate limiting** | 350ms between groups + exponential backoff on 429/5xx/timeouts |

### Step 07 — Build Films from Catalog

| | |
|---|---|
| **File** | `build_films_from_catalog.py` |
| **Wrapper** | `pipeline/07_build_films_from_catalog.py` |
| **Purpose** | Groups TMDB-matched catalog_items by `tmdb_id` and creates/links entries in `films` table |
| **Input** | `catalog_items` (matched rows) |
| **Output** | Rows in `films` table, `film_id` set on `catalog_items` |
| **Modes** | Default (only `film_id IS NULL` rows) or `--full-rebuild` (all matched rows) |
| **Dependencies** | `supabase`, `dotenv`, `httpx` (for retry logic) |
| **Idempotent** | Yes — checks existing films by tmdb_id before creating |
| **Note** | `films` is the canonical entity the agent uses. One film per unique `tmdb_id`. |

---

## Orchestrator Shell Scripts

| File | Purpose | Steps |
|---|---|---|
| `pipeline/run_catalog_sync.sh` | Full pipeline for new titles + enrichment + films | 00 → 01 → 02 → 03 → 04 → 05 → 06 (daily) → 07 |
| `pipeline/run_stock_sync.sh` | Operational commercial updates only (no harmonize, no catalog `media_release_date`) | 00 → 01 → 02 → 03 → 04 upsert (`--existing-only`) → 05 history |

Both include locking (`.locks/`), logging (`logs/`), and env var overrides.

---

## Shopify Product Publisher

| | |
|---|---|
| **File** | `publish_selected_barcodes_to_shopify.py` |
| **Also in** | `shopify_publish/` folder (with `.env.prod`, `barcodes.csv`, `README.md`) |
| **Purpose** | Creates draft Shopify products from catalog_items by barcode |
| **Input** | Barcodes (CSV file or comma-separated), `catalog_items` data |
| **Output** | Products created in Shopify via GraphQL `productSet` mutation |
| **Flags** | `--barcodes`, `--barcodes-file`, `--supplier`, `--status`, `--env`, `--dry-run` |
| **Dependencies** | `supabase`, `requests`, `dotenv` |
| **Features** | Duplicate check by barcode, best-offer supplier selection, GBP→AUD cost conversion with landed markup, full metafield population |
| **Not part of pipeline** | Run manually / on-demand only |

---

## Maintenance Scripts

| File | Purpose |
|---|---|
| `maintenance_rematch_tmdb_from_csv.py` | Re-attempt TMDB matching for specific rows from a CSV review file |
| `clear_bad_links_opt.py` | Clear bad film links (manual cleanup) |
| `relink_suspicious_catalog_rows.py` | Relink suspicious catalog rows |
| `relink_unlinked_catalog_rows.py` | Relink unlinked catalog rows |
| `refresh_tmdb_metadata_in_catalog.py` | Refresh TMDB metadata (older version) |

---

## Shared Helper Modules

| File | Used by | Purpose |
|---|---|---|
| `app/helpers/tmdb_match_helpers.py` | Enrichment + maintenance scripts | Title cleaning, TMDB search type detection, API search with fallbacks |
| `catalog_match_helpers.py` | Various | Catalog matching utilities |

---

## Observability Scripts

`catalog_health_report`, `tmdb_match_audit`, and `pipeline_run_report` support
`--format text|json|csv`, `--output <path>`, and return non-zero exit codes when
thresholds are breached (1 = WARNING, 2 = CRITICAL). `append_pipeline_run_history`
appends JSON only (see its section). `pipeline_trend_report` uses text/json and
`--validate-only` exits 2 on schema errors.

### catalog_health_report.py

| | |
|---|---|
| **File** | `scripts/observability/catalog_health_report.py` |
| **Purpose** | Comprehensive catalog health check: coverage, linkage, commercial, freshness, exceptions |
| **Input** | Live Supabase queries against `catalog_items` and `films` |
| **Output** | Structured report (text / JSON / CSV) |
| **Flags** | `--format`, `--output`, `--since-days`, `--env` |
| **Dependencies** | `supabase`, `dotenv` |
| **Cron-safe** | Yes — read-only, non-destructive |
| **Thresholds** | Film linkage < 70% / < 85% among **film-classified** rows only (CRITICAL/WARNING); null barcodes > 0 (CRITICAL); duplicate films > 0 (CRITICAL); missing price > 5% (WARNING); stale TMDB pending > 50 (WARNING) |

### pipeline_trend_report.py

| | |
|---|---|
| **File** | `scripts/observability/pipeline_trend_report.py` |
| **Purpose** | Summarise last N runs in `pipeline_run_history.json` with first→last trends |
| **Input** | History JSON file |
| **Output** | Text or JSON (`--format json` includes window runs) |
| **Flags** | `--history-file`, `--last`, `--format`, `--validate-only` |
| **Dependencies** | `app.observability.pipeline_history_schema` |
| **Cron-safe** | Yes — read-only (use `--validate-only` as a schema gate) |

### append_pipeline_run_history.py

| | |
|---|---|
| **File** | `scripts/observability/append_pipeline_run_history.py` |
| **Purpose** | Append one trend snapshot to `logs/pipeline_run_history.json` after each sync |
| **Input** | Pipeline log path + live Supabase metrics |
| **Output** | Updates JSON file (`schema_version`, `runs[]`); optional `--dry-run` prints record |
| **Flags** | `--log-file`, `--log-dir`, `--history-file`, `--pipeline-type`, `--dry-run`, `--max-runs`, `--env` |
| **Dependencies** | `supabase`, `dotenv`; uses `app.observability.*` |
| **Cron-safe** | Yes — invoked by `run_catalog_sync.sh` / `run_stock_sync.sh` (non-fatal) |

### tmdb_match_audit.py

| | |
|---|---|
| **File** | `scripts/observability/tmdb_match_audit.py` |
| **Purpose** | TMDB enrichment quality audit: match rates, not_found patterns, rematch candidates, low-quality matches |
| **Input** | Live Supabase queries against `catalog_items` |
| **Output** | Structured report (text / JSON / CSV) |
| **Flags** | `--format`, `--output`, `--since-days`, `--env` |
| **Dependencies** | `supabase`, `dotenv` |
| **Cron-safe** | Yes — read-only, non-destructive |
| **Thresholds** | Match rate < 70% (WARNING), stale pending > 50 (WARNING), low-quality matches > 20 (WARNING) |

### pipeline_run_report.py

| | |
|---|---|
| **File** | `scripts/observability/pipeline_run_report.py` |
| **Purpose** | Parse pipeline log files and report duration, rows, retries, failures per step |
| **Input** | Log files in `logs/` directory |
| **Output** | Structured report (text / JSON / CSV) |
| **Flags** | `--format`, `--output`, `--log-dir`, `--log-file`, `--last N` |
| **Dependencies** | `app.observability.pipeline_log_parser` |
| **Cron-safe** | Yes — read-only |

---

## Cron Configuration

| File | Purpose |
|---|---|
| `pipeline/cron_jobs.example` | Ready-to-paste crontab entries for stock sync (daily) and catalog sync (periodic) |

---

## Database Tables (data flow order)

```
staging_moovies_raw      ← Step 01
staging_lasgo_raw        ← Step 02
staging_shopify_raw      ← (Shopify GraphQL pull, separate process)
        ↓
staging_supplier_offers  ← Steps 03 + 04
        ↓
catalog_items            ← Steps 05 + 06
        ↓
films                    ← Step 07
```

---

## Environment Files

| File | Purpose |
|---|---|
| `.env` | Dev/default credentials (Supabase, TMDB, FTP, Shopify dev store) |
| `.env.prod` | Production Shopify credentials + exchange rates |
| `shopify_publish/.env.prod` | Copy of prod env for the publish workflow |

---

## Folder Structure Summary

```
tape-film-ichoose/
├── pipeline/                          # Numbered wrappers + orchestrators
│   ├── 00_fetch_supplier_files.py
│   ├── 01_import_moovies_raw.py
│   ├── 02_import_lasgo_raw.py
│   ├── 03_normalize_supplier_products.py
│   ├── 04_harmonize_supplier_offers.py
│   ├── 05_upsert_to_catalog_items.py
│   ├── 06_enrich_catalog_with_tmdb_daily.py
│   ├── 06_enrich_catalog_with_tmdb_recovery.py
│   ├── 07_build_films_from_catalog.py
│   ├── run_catalog_sync.sh
│   ├── run_stock_sync.sh
│   ├── cron_jobs.example
│   └── README.md
│
├── app/                                # Extracted business logic modules
│   ├── rules/                          # Pure rules (no I/O)
│   │   ├── harmonization_rules.py      #   Field ownership, pick_best_* functions
│   │   ├── catalog_update_rules.py     #   Update whitelists, protected fields
│   │   ├── tmdb_rules.py              #   Match-once logic, enrichment filters
│   │   ├── supplier_precedence_rules.py #  Priority ranking, best-offer selection
│   │   └── pricing_rules.py           #   Margins, conversion, sale price calc
│   │
│   ├── clients/                        # External service wrappers
│   │   ├── supabase_client.py          #   Retry-aware DB client, pagination
│   │   ├── tmdb_client.py             #   TMDB API with rate limiting + backoff
│   │   ├── shopify_client.py          #   OAuth + GraphQL
│   │   └── ftp_client.py             #   FTP/FTPS with TLS auto-detection
│   │
│   ├── services/                       # Pipeline step implementations (I/O + rules)
│   │   ├── supplier_fetch_service.py
│   │   ├── moovies_import_service.py
│   │   ├── lasgo_import_service.py
│   │   ├── normalize_offers_service.py
│   │   ├── harmonize_offers_service.py
│   │   ├── catalog_upsert_service.py
│   │   ├── catalog_offer_mapping.py
│   │   ├── tmdb_enrichment_service.py
│   │   └── film_builder_service.py
│   │
│   ├── observability/                  # Metrics, log parsing, run history, schema
│   │   ├── catalog_metrics.py
│   │   ├── pipeline_history_schema.py
│   │   ├── pipeline_log_parser.py
│   │   └── pipeline_run_history.py
│   │
│   └── helpers/                        # Shared utilities
│       ├── retry_helpers.py            #   Generic retry with exponential backoff
│       ├── text_helpers.py            #   clean_text, slugify, parse_*, chunked
│       ├── tmdb_match_helpers.py      #   Title cleaning, search type detection
│       └── catalog_match_helpers.py   #   Film/catalog linking utilities
│
├── scripts/                            # Operational scripts
│   ├── pipeline/                       #   Numbered step wrappers + orchestrators
│   ├── publish/                        #   Shopify product publisher (standalone)
│   │   ├── publish_selected_barcodes_to_shopify.py
│   │   └── barcodes.csv
│   ├── maintenance/                    #   One-off corrective tools
│   │   ├── maintenance_rematch_tmdb_from_csv.py
│   │   ├── clear_bad_links_opt.py
│   │   ├── relink_suspicious_catalog_rows.py
│   │   ├── relink_unlinked_catalog_rows.py
│   │   └── refresh_tmdb_metadata_in_catalog.py
│   └── observability/                  #   Health reports and audits
│       ├── catalog_health_report.py
│       ├── append_pipeline_run_history.py
│       ├── pipeline_trend_report.py
│       ├── tmdb_match_audit.py
│       └── pipeline_run_report.py
│
├── tests/                              # Unit tests
│   └── rules/
│       ├── test_harmonization_rules.py
│       ├── test_catalog_update_rules.py
│       ├── test_tmdb_rules.py
│       ├── test_supplier_precedence_rules.py
│       ├── test_pricing_rules.py
│       └── test_content_classification_rules.py
│
├── shopify_publish/                   # Shopify publish workflow (standalone)
│   ├── publish_selected_barcodes_to_shopify.py
│   ├── .env.prod
│   ├── barcodes.csv
│   └── README.md
│
├── supplier_files/                    # Local supplier file storage
│   ├── moovies/
│   └── lasgo/
│
├── docs/                              # Documentation
│   ├── architecture.md                #   Layers, write boundaries, source-of-truth
│   ├── data-pipeline-operations.md
│   ├── scripts-inventory.md
│   └── shopify_product_template.csv
│
├── logs/                              # Cron/pipeline log output
├── .locks/                            # Pipeline run locks
│
├── import_moovies_raw.py              # Root shims → app.services.* (same CLI)
├── import_lasgo_raw.py
├── normalize_supplier_products.py
├── harmonize_supplier_offers.py
├── upsert_supplier_offers_to_catalog_items_preserve_tmdb.py
├── enrich_catalog_with_tmdb_v2.py
├── build_films_from_catalog.py
├── tmdb_match_helpers.py              # Shim → app/helpers/tmdb_match_helpers.py
├── publish_selected_barcodes_to_shopify.py
├── maintenance_rematch_tmdb_from_csv.py
│
├── .env                               # Dev credentials
└── .env.prod                          # Prod credentials
```
