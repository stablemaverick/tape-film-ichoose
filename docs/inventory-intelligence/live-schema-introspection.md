# Live schema introspection — inventory intelligence Phase 3a

**Inspected:** 2026-08-06  
**Environment:** production Supabase project referenced by `.env.prod` (`SUPABASE_URL`)  
**Method:** PostgREST OpenAPI (`/rest/v1/`) for column definitions; service-role table scans for counts and value patterns; Shopify Admin GraphQL for live inventory quantity semantics.  
**Limitation:** Direct `pg_catalog` / `information_schema` SQL is not exposed via PostgREST (no `rpc` SQL helpers). Unique indexes and CHECK constraints are therefore **inferred** from OpenAPI required/defaults, repository migrations, and application `on_conflict` targets. They should be confirmed with a Postgres console before any destructive migration work.

No schema objects were created or altered during introspection.

---

## 1. Live tables exposed via PostgREST

| Table | Approx. rows | In repo migrations? | Role |
|-------|-------------:|---------------------|------|
| `catalog_items` | 24 980 | Partial alters only | Master commercial + identity + TMDB + film link |
| `films` | 9 086 | No CREATE in repo | Canonical film (`tmdb_id`) |
| `shopify_listings` | 680 | Yes | Shopify variant/inventory snapshot |
| `pipeline_runs` | 519 | Yes | Pipeline observability |
| `catalog_health_snapshots` | 519 | Yes | Health metrics JSON |
| `staging_moovies_raw` | 14 208 | No CREATE in repo | Moovies raw import |
| `staging_lasgo_raw` | large (count times out) | No CREATE in repo | Lasgo raw import (append-heavy) |
| `staging_shopify_raw` | 501 | No CREATE in repo | Shopify-as-supplier raw |
| `staging_supplier_offers` | 24 864 | No CREATE in repo | Normalised offers |
| `supplier_orders` | 17 | No CREATE in repo | Customer paid lines needing supplier buy |
| `supplier_import_raw` | 0 | No CREATE in repo | **Legacy empty** Moovies-shaped import table |
| `film_popularity` | 0 | No CREATE in repo | Popularity scores (unused / empty) |
| `wishlist_items` | 3 | Yes | Wishlist |
| `catalog_items_shopify_link_backup_20260401` | (backup) | No | One-off backup; not operational |

### Inventory / PO / history objects **not** present live

Confirmed absent from the OpenAPI catalog (no PostgREST paths):

- `purchase_orders`, `purchase_order_lines`
- `tape_inventory_levels`, `release_variants`, `variant_identifiers`
- `supplier_offers` (canonical), `supplier_offer_observations`, `supplier_sku_resolutions`
- `inventory_events`, `sales_order_lines`, `demand_events`, `editions`
- Any materialised availability view

Open PO coverage today lives in **filesystem CSVs** (`supplier_orders_report` + inbound PO folder), not in Postgres.

---

## 2. Confirmed column definitions (OpenAPI)

Formats below are Postgres types as reported by PostgREST (`format` field).

### 2.1 `catalog_items` (49 columns)

Grain in application code: one row per `(supplier, barcode)` for Moovies/Lasgo; Tape Film prefers `(supplier, shopify_variant_id)`.

| Column | Type | Notes |
|--------|------|-------|
| `id` | uuid PK | `gen_random_uuid()` |
| `title` | text NOT NULL | |
| `edition_title`, `format`, `director`, `studio` | text | Edition is a column, not an entity |
| `film_released`, `media_release_date` | date | |
| `barcode`, `sku`, `supplier`, `supplier_sku` | text | Barcode nullable in practice |
| `supplier_currency` | text | default `GBP` |
| `cost_price`, `calculated_sale_price` | numeric | |
| `pricing_source` | text | |
| `availability_status` | text | default `supplier_stock` |
| `supplier_stock_status` | **text** | Always numeric strings in live data (see §3) |
| `source_type` | text | default `catalog`; live sample all `catalog` |
| `shopify_product_id`, `shopify_variant_id` | text | |
| `notes` | text | |
| `active` | boolean | default true |
| `created_at`, `updated_at` | timestamptz | |
| `country_of_origin`, `category`, `region_code` | text | |
| `no_of_discs` | integer | |
| `supplier_priority` | integer | default 100 |
| `supplier_last_seen_at` | timestamptz | Stock freshness signal today |
| TMDB fields | various | `tmdb_id`, title, match status, poster/backdrop, votes, popularity, genres, cast, `film_released` overlap |
| Film link | uuid/text/tstz | `film_id`, `film_link_status`, `film_link_method`, `film_linked_at` |
| `media_type` | text | default `film` |
| `published_to_shopify`, `shopify_published_at` | boolean/tstz | From repo migration |

**Inferred unique constraint (from upserts):** `(supplier, barcode)` via `on_conflict="supplier,barcode"`.

### 2.2 `films` (20 columns)

| Column | Type | Notes |
|--------|------|-------|
| `id` | uuid PK | |
| `title` | text NOT NULL | |
| `original_title`, `director`, `genres`, `top_cast`, `country_of_origin` | text | |
| `film_released` | date | |
| `tmdb_id` | bigint | Live sample: no nulls, no duplicate `tmdb_id` in full table scan |
| TMDB media/score fields | text/numeric/int | |
| `metadata_source` | text | |
| `popularity_score`, `orders_count`, `recent_orders` | numeric/int | defaults 0 |
| `created_at` | timestamptz | |

**Inferred unique:** `tmdb_id` (`on_conflict="tmdb_id"`).

### 2.3 `shopify_listings` (35 columns)

Matches repo migration `20260328130000_shopify_listings.sql` (+ snapshot upgrade). Key inventory fields:

| Column | Type | Notes |
|--------|------|-------|
| `shop`, `shopify_product_id`, `shopify_variant_id` | text | Unique `(shop, shopify_variant_id)` |
| `barcode`, `sku` | text | |
| `inventory_quantity` | integer | Single Shopify “available-ish” mirror — **not** split on_hand/committed |
| `inventory_policy` | text | Live: `DENY` 566, `CONTINUE` 114 |
| `tracks_inventory` | boolean | |
| `shopify_inventory_item_id` | text | |
| `catalog_item_id` | uuid FK → `catalog_items` | |
| `match_method`, `match_status`, `match_value` | text | See §3 |
| sync timestamps / errors | timestamptz/text | |

### 2.4 `pipeline_runs` (20 columns)

Observability only (inserts/updates/failures, health metrics). **No** feed-level record counts, supplier id, or freshness timestamps beyond `started_at` / `ended_at`.

### 2.5 `supplier_orders` (17 columns)

Customer-order → supplier buy list (webhook), **not** purchase orders to suppliers.

| Column | Type |
|--------|------|
| `shopify_order_id`, `shopify_order_name`, `customer_email` | text |
| `supplier`, `title`, `barcode`, `sku`, `product_code` | text |
| `quantity` | integer |
| `cost_price`, `unit_sale_price` | numeric |
| `supplier_currency`, `status` | text (live all `pending`) |
| `exported_to_supplier`, `exported_at` | boolean/tstz |
| `created_at` | timestamptz |

### 2.6 Staging tables

**`staging_moovies_raw`:** `supplier`+`upsert_key` unique (inferred); qty in `raw_qty` (text); `raw_status` mostly empty, rare `"1"`/`"2"`.

**`staging_lasgo_raw`:** batch insert (no upsert key in code); qty in `raw_free_stock` (text); EAN in `raw_ean`; count(*) times out (table large / unindexed scans).

**`staging_shopify_raw`:** unique `(supplier, shopify_variant_id)` inferred; inventory in `raw_inventory_qty`.

**`staging_supplier_offers`:** unique `(supplier, barcode)` documented in normalize service; **`supplier_stock_status` is integer** here (unlike text on `catalog_items`); availability enums match catalog.

**`supplier_import_raw`:** legacy Moovies-like columns; **0 rows**.

---

## 3. Representative value patterns

### 3.1 `catalog_items.availability_status` (full table)

| Value | Count |
|-------|------:|
| `supplier_stock` | 21 679 |
| `supplier_out` | 2 685 |
| `store_stock` | 372 |
| `store_out` | 236 |
| `preorder` | 8 |

No `in_stock`, `low_stock`, `backorder`, or `discontinued` values in production catalog today.

### 3.2 Suppliers

| Supplier | `catalog_items` | `staging_supplier_offers` |
|----------|----------------:|--------------------------:|
| `moovies` | 14 209 | 14 209 |
| `lasgo` | 10 155 | 10 155 |
| `Tape Film` | 616 | 500 |

### 3.3 `supplier_stock_status`

- `catalog_items`: column type **text**, but **100% numeric strings** in a 24 980-row scan (no `"In stock"` literals stored).
- `staging_supplier_offers`: column type **integer**.
- Moovies/Lasgo normalize paths write qty → `supplier_stock` / `supplier_out` only.

### 3.4 Barcode identity (20 000-row sample)

| Metric | Value |
|--------|------:|
| Null/empty barcode | 157 |
| Distinct barcodes | 13 904 |
| Barcodes with multiple catalog rows | 5 846 (typically multi-supplier same EAN) |

Barcode is a shared join key across suppliers, not a unique product id.

### 3.5 `shopify_listings` inventory mirror

| `inventory_quantity` | Count (of 680) |
|----------------------|---------------:|
| Positive | 334 |
| Zero | 297 |
| Negative | 49 (44 with `CONTINUE`) |

Match status: `matched` 538, `ambiguous` 120, `unmatched` 22.  
Match method: `shopify_variant_id` 538, `barcode` 103, `title` 39.

### 3.6 Live Shopify Admin quantity semantics (706 variants)

Queried `available`, `committed`, `on_hand`, `incoming` at the configured fulfilment location.

| Observation | Count / note |
|-------------|--------------|
| `available == on_hand - committed` | 705 / 705 with levels present |
| `available > on_hand` | **0** |
| `available < 0` | **49** (oversell / preorder CONTINUE pattern: typically `on_hand=0`, `committed>0`) |
| `on_hand < 0` | **0** |
| `committed < 0` | **0** |
| `committed > on_hand` | **49** (same oversell set) |
| `incoming > 0` | **0** in this sample |
| Preorder metafield + `on_hand > 0` | 3 (2 look like test products; 1 real title) |

**Implication for constraints:** Shopify’s identity is effectively `available = on_hand - committed`, which allows **negative available**. A hard DB check `available <= on_hand` would hold on this sample, but `available >= 0` and `committed <= on_hand` would **not**. Phase 3a therefore stores raw Shopify facts and treats quantity relationship checks as **reconciliation warnings / tests**, not CHECK constraints—except where noted as universally safe (non-negative committed/on_hand if we later choose to harden after more sampling).

---

## 4. Current-state field mapping (inventory concepts → live columns)

| Desired inventory fact | Current source | Mapping notes |
|------------------------|----------------|---------------|
| TAPE on_hand | Shopify GraphQL `on_hand` (report only); **not** persisted | `shopify_listings.inventory_quantity` ≈ available, not on_hand |
| TAPE committed | Shopify GraphQL `committed` (report only) | Not in DB |
| TAPE available | Shopify GraphQL `available` / listing `inventory_quantity` | Negative allowed |
| Shopify incoming reported | Shopify GraphQL `incoming` (report only; 0 in sample) | Not in DB |
| PO incoming confirmed | Inbound PO CSV + report cover logic | Not in DB |
| Supplier qty | `catalog_items.supplier_stock_status` (text numeric) / staging integer | Co-located with Tape Film rows too |
| Supplier availability | `availability_status` | `supplier_stock` / `supplier_out` / `store_*` / `preorder` |
| Supplier last seen | `supplier_last_seen_at` | No confidence / stale derivation |
| Film link | `catalog_items.film_id` → `films.id` | |
| Shopify variant link | `shopify_variant_id` on catalog + listings | |
| Customer sales signal | `supplier_orders` (partial); `film_popularity` empty | |
| Pipeline freshness | `pipeline_runs` + file logs | Not linked to offer rows |

### Ownership today (still true)

| Fact class | Owner |
|------------|--------|
| TAPE inventory quantities | Shopify (mirrored imperfectly into `shopify_listings`) |
| Supplier availability / cost | Supplier feeds → staging → `catalog_items` commercial fields |
| Film metadata | TMDB enrichment + film builder |
| Open PO cover | CSV tooling (not SoT in DB) |

---

## 5. Indexes / constraints (inferred)

| Object | Evidence |
|--------|----------|
| `shopify_listings (shop, shopify_variant_id)` UNIQUE | Repo migration |
| `shopify_listings` barcode / catalog_item_id / dates indexes | Repo migration |
| `wishlist_items` unique shop+customer+catalog | Repo migration |
| `pipeline_runs.created_at` index | Repo migration |
| `staging_supplier_offers (supplier, barcode)` UNIQUE | Normalize service comment + upsert |
| `staging_moovies_raw (supplier, upsert_key)` UNIQUE | Moovies import upsert |
| `staging_shopify_raw (supplier, shopify_variant_id)` UNIQUE | Shopify raw upsert |
| `catalog_items (supplier, barcode)` UNIQUE | Catalog upsert |
| `films.tmdb_id` UNIQUE | Film builder upsert |

**Not verified via `pg_indexes`:** any additional live-only indexes, FKs beyond OpenAPI hints, or CHECK constraints on core tables.

---

## 6. Confirmed gaps for Phase 3a foundation

1. No durable TAPE inventory grain with on_hand / committed / available / separate incoming facts.  
2. No canonical supplier offer entity separate from `catalog_items`.  
3. No observation history or typed inventory events.  
4. No supplier SKU resolution / review queue.  
5. No purchase order tables.  
6. No controlled availability enum (production uses operational strings only).  
7. `supplier_stock_status` type mismatch (text vs integer) across tables.  
8. Shopify negative `available` is normal for CONTINUE/preorder oversolds — do not constrain `available >= 0`.

---

## 7. Artifacts generated during introspection (local, not committed)

- `tmp/live_openapi_definitions.json`
- `tmp/live_value_patterns.json`
- `tmp/shopify_inventory_semantics.json`

These may contain shop titles/barcodes; keep them out of git.
