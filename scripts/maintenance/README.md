# Maintenance scripts

This folder holds **one-off** and operational tools. Unless a script’s docstring says otherwise, they are **not** substitutes for scheduled `catalog_sync`, stock sync, or daily TMDB enrichment.

The sections below document two TMDB-focused maintenance tools that share logic with production enrichment.

## Prerequisites

- Run from the **repository root** with the project venv activated (paths below use `./venv/bin/python`).
- **Environment:** `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, and `TMDB_API_KEY` (for any script that calls TMDB). Default dotenv path is repo `.env`; override with `--env` where supported.

## Shared implementation

Both tools reuse `app/services/tmdb_enrichment_service.py`:

| Piece | Role |
| --- | --- |
| `run_enrichment_for_rows` | Targeted retry: same path as recovery (`detect_tmdb_search_type`, `search_tmdb_movie_safe`, `build_tmdb_update`, grouping by barcode). |
| `build_tmdb_update` / `fetch_tmdb_details_and_credits` | Force-apply path for known TMDB ids. |

Scheduled pipelines and `enrich_catalog_with_tmdb_v2.py` are unchanged by these scripts.

---

## `retry_tmdb_targeted_catalog.py`

**Purpose:** Re-run TMDB enrichment for a **narrow, explicit** subset of `catalog_items` (for example after you cleared `tmdb_last_refreshed_at` on specific rows).

**Not for:** Daily or cron execution. The script refuses an unscoped “all rows with null refresh” query unless you pass `--allow-wide-query`.

**Selection (always):** `active = true` and `tmdb_last_refreshed_at IS NULL`, plus at least one narrowing criterion unless `--allow-wide-query`:

- `--media-type` (e.g. `tv`)
- `--title-contains` / `--title-ilike`
- `--ids` / `--barcodes` (comma-separated)
- `--allow-wide-query` (explicit opt-in)

**Behaviour:** Loads matching rows, then calls `run_enrichment_for_rows` with maintenance-style logging (`search_type`, `tmdb_id`, etc.). `--dry-run` performs TMDB calls but **does not** write to Supabase.

### Examples

```bash
# Typical “requeued TV-like” batch
./venv/bin/python scripts/maintenance/retry_tmdb_targeted_catalog.py \
  --media-type tv --limit 500

# Specific rows (must still be active + null tmdb_last_refreshed_at)
./venv/bin/python scripts/maintenance/retry_tmdb_targeted_catalog.py \
  --ids "uuid-a,uuid-b"

# Dry-run (no DB updates)
./venv/bin/python scripts/maintenance/retry_tmdb_targeted_catalog.py \
  --media-type tv --limit 20 --dry-run

# Title narrowing
./venv/bin/python scripts/maintenance/retry_tmdb_targeted_catalog.py \
  --media-type tv --title-contains "Season" --limit 100
```

See `--help` for `--page-size`, `--max-groups`, `--sleep-ms`, and `--stats`.

---

## `correct_catalog_tmdb_match.py`

**Purpose:** When a row is **wrongly matched** to TMDB, clear stored TMDB fields and/or **force** the correct TMDB id using the same column updates as automatic enrichment.

**Not for:** Scheduled runs; it does not alter the daily enrichment entrypoints.

**Targeting:** `--id` is required (seed `catalog_items` row). Optional `--barcode` must match that row’s barcode. `--apply-to-barcode-group` applies the operation to **all active** rows with the same barcode (requires a non-empty barcode on the seed row).

### Subcommands

| Subcommand | What it does |
| --- | --- |
| `clear` | Nulls TMDB match payload fields, sets `tmdb_match_status` to `not_found`, sets `tmdb_last_refreshed_at` to NULL (row can be picked up by enrichment again). Optional `--clear-film-id`, `--clear-director-and-release`. |
| `apply` | Fetches TMDB details + credits for `--tmdb-id` and `--media-type` (`movie` / `film` / `tv`), then writes `build_tmdb_update` output per target row. |

**Dry-run:** Logs per row; no Supabase writes. For `apply`, TMDB is still called so logs reflect the real payload.

### Fields touched by `clear`

Cleared by default: `tmdb_id`, `tmdb_title`, `tmdb_poster_path`, `tmdb_backdrop_path`, `tmdb_vote_average`, `tmdb_vote_count`, `tmdb_popularity`, `top_cast`, `genres`, `country_of_origin`, plus `tmdb_match_status` → `not_found` and `tmdb_last_refreshed_at` → NULL.

`director` and `film_released` are only cleared if you pass `--clear-director-and-release`. `film_id` is cleared only with `--clear-film-id`.

### Examples

```bash
# Dry-run clear
./venv/bin/python scripts/maintenance/correct_catalog_tmdb_match.py clear --id <UUID> --dry-run

# Clear a bad match (single row)
./venv/bin/python scripts/maintenance/correct_catalog_tmdb_match.py clear --id <UUID>

# Clear match and unlink film
./venv/bin/python scripts/maintenance/correct_catalog_tmdb_match.py clear --id <UUID> --clear-film-id

# Force movie
./venv/bin/python scripts/maintenance/correct_catalog_tmdb_match.py apply --id <UUID> \
  --tmdb-id 27205 --media-type movie

# Force TV
./venv/bin/python scripts/maintenance/correct_catalog_tmdb_match.py apply --id <UUID> \
  --tmdb-id 1396 --media-type tv

# Same correction for every active row in the barcode group
./venv/bin/python scripts/maintenance/correct_catalog_tmdb_match.py apply --id <UUID> \
  --tmdb-id 1396 --media-type tv --apply-to-barcode-group
```

Per-row logs include: `id`, `title`, existing/new `tmdb_id`, `media_type`, `action`, and `success` / `failure` / `dry_run`.

---

## Choosing a tool

| Goal | Tool |
| --- | --- |
| Re-run **search-based** enrichment on a filtered set of requeued rows | `retry_tmdb_targeted_catalog.py` |
| **Remove** wrong TMDB data or **pin** a known correct TMDB id | `correct_catalog_tmdb_match.py` |
| Drain a **large** generic backlog via standard `run_enrich` | `burn_down_tmdb_backlog.py` (separate docstring in that file) |

---

## Other scripts in this directory

Older or specialised maintenance utilities (`clear_bad_links_opt.py`, `relink_*.py`, `refresh_tmdb_metadata_in_catalog.py`, etc.) keep their behaviour in each file’s module docstring; refer there before running.
