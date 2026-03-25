# Catalog enrichment → `films` table

## When is `films` ready to build?

`build_films_from_catalog.py` only uses rows that are:

- `active = true`
- `tmdb_match_status = 'matched'`
- `tmdb_id` is not null

It **does not** require every catalog row to be matched. Unmatched / `not_found` rows are skipped.

Full “enrichment finished” for the **never-attempted** queue means:

- `catalog_items` where `tmdb_last_refreshed_at IS NULL` → count is **0**

(That is the rule for the recovery script: first attempt only.)

## Films only (no enrichment)

**Default (daily / operational)** — only links rows that are safe and still need a link:

- `active`, `tmdb_match_status = matched`, `tmdb_id` set, **`film_id IS NULL`**
- Does **not** touch already-linked rows (no relink drift, no `film_linked_at` churn).

```bash
venv/bin/python build_films_from_catalog.py
```

**Recovery / explicit resync** — rewrites links for all matched rows (use sparingly):

```bash
venv/bin/python build_films_from_catalog.py --full-rebuild
```

Use this whenever `catalog_items` is already TMDB-enriched; the script never calls TMDB.

## Unattended: enrich until queue empty, then build films

From the repo root, with the machine awake (plug in + `caffeinate` in another terminal if needed):

```bash
mkdir -p logs
export LOG_PATH="logs/enrich_then_films_$(date -u +%Y%m%d_%H%M%S).log"
venv/bin/python scripts/run_enrichment_then_build_films.py
```

Optional tuning:

| Env | Default | Meaning |
|-----|---------|---------|
| `ENRICH_MAX_ROWS` | 50000 | Rows fetched per enrichment batch |
| `ENRICH_MAX_GROUPS` | 25000 | Barcode groups processed per batch |
| `ENRICH_SLEEP_MS` | 350 | Delay between TMDB groups (ms) |
| `ENRICH_MAX_BATCHES` | 500 | Safety cap on how many times enrichment is re-run |

If enrichment crashes (e.g. network), the orchestrator waits 60s and retries until pending is zero or `ENRICH_MAX_BATCHES` is hit.

## Fields passed into `films`

`build_films_from_catalog.py` reads from `catalog_items` and inserts/updates `films` with:

`title`, `original_title` (null), `film_released`, `director`, `tmdb_id`, `tmdb_title`, `genres`, `top_cast`, `country_of_origin`, `tmdb_poster_path`, `tmdb_backdrop_path`, `tmdb_vote_average`, `tmdb_vote_count`, `tmdb_popularity`, `metadata_source` (`tmdb`).

Then it sets on linked `catalog_items`: `film_id`, `film_link_status`, `film_link_method`, `film_linked_at`.

## TMDB timeouts

`enrich_catalog_with_tmdb_v2.py` uses a **60s** read timeout and retries transient failures (timeouts, 5xx, 429) so short network blips should not kill a long run.
