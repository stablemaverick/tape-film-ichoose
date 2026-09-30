# Product description drafting — plan

Status (2026-09-30): Phases 1–3 built and tested locally — research/extraction/verification, guarded
Shopify writer, publish integration (on by default; `--no-descriptions` to skip), Australian classification
metafields on new DRAFT products (`--no-classification` to skip). Migrations
`20260930120000_product_description_drafts.sql` and `20260930130000_product_description_drafts_classification.sql`
applied in production. Not yet done: weekly TBC refresh (phase 4).

## Phases 2–3 — what exists

- `app/services/product_description_writer_service.py` — `apply_description` (guards: DRAFT only, barcode
  match, manual-edit protection) and `run_description_stage` (draft → apply → audit row → JSON under
  `logs/descriptions/`).
- Manual-edit protection uses product metafield `custom.description_hash` (sha256 prefix of the
  `descriptionHtml` Shopify returns after saving), so it works without the Supabase table. Also written:
  `custom.description_source`, `custom.description_features_status`.
- `product_description_drafts` is an audit log only; if the migration is not applied the stage logs a
  warning and carries on.
- `jobs/publish_catalog_to_shopify.py`: descriptions run by default after publish for newly created
  products (with `--dry-run` drafts JSON only; `--no-descriptions` to skip), `--descriptions-only` (existing products by barcode),
  `--force-descriptions` (overwrite manual edits; still DRAFT only). Description failures never change the
  publish exit code.
- Note: local `.env` targets the dev Shopify store but the production Supabase project; use
  `--env-file .env.prod` for production publishes.

## Phase 1 — what exists

| Piece | File |
| --- | --- |
| Distributor registry, blocklist, film-page URL patterns, fetch mirrors | `app/rules/distributor_sources.py` |
| OpenAI Responses client (httpx) | `app/clients/openai_client.py` |
| Page finder (Shopify barcode lookup, URL patterns, domain-restricted web search, page text) | `app/services/description_sources_service.py` |
| Extraction + own-summary prompts, TMDB facts | `app/services/description_extraction_service.py` |
| Verifier | `app/services/description_verification_service.py` |
| Orchestrator + HTML builder | `app/services/product_description_drafting_service.py` |
| CLI (dry run, `--compare` against a reference JSON) | `jobs/draft_product_descriptions.py` |
| Unit tests | `tests/services/test_product_description_drafting.py` |

Changes from the original plan, found while building:

- Model is `gpt-5-mini`: `gpt-4.1-mini` does not support `allowed_domains` on the web search tool.
- No `beautifulsoup4`: the stdlib HTML parser is enough. Arrow/Toy Robot pages render product copy from
  embedded JSON, so long embedded strings mentioning the film title are included in page text.
- criterion.com blocks automated fetches (Cloudflare) and the web-search model refuses to quote pages
  verbatim. Criterion pages are read from Criterion's own backend host (`FETCH_MIRRORS`); the
  criterion.com URL is what gets cited.
- Web search indexes StudioCanal / Warner / Sony / Disney title pages poorly, so known URL patterns are
  tried first (e.g. `studiocanal.co.uk/title/{slug}-{year}/`, trying year and year − 1).
- studiocanal.co.uk serves an incomplete TLS chain; certificate verification is relaxed for that domain only.

Golden-set result (the 20 titles researched manually on 2026-09-30), run 3:
19/20 match on source type + features status (target ≥ 18), 0 blocklisted or off-allowlist URLs,
invented format lines dropped by the verifier. ~1.5 US cents per title, ~90 s for 20 titles with 4 workers.
Known gaps: the verifier cannot catch thematic padding in a synopsis that adds no new names or facts;
titles with no catalogue year and an ambiguous TMDB title (e.g. The Man Who Fell to Earth) get no synopsis.

## Goal

When `jobs.publish_catalog_to_shopify` creates new DRAFT products, automatically research and write a
product description for each one, following the same rules used manually on 2026-09-30:

1. Official distributor page first (Arrow, Criterion, StudioCanal, Eureka, Radiance, Second Sight …).
2. Otherwise an official fallback (studio film site, official press kit, another official territory page).
3. Otherwise a short factual summary written from data we already hold. Nothing invented.
4. Format and special features are only ever copied from official sources. Retailer, review and fan
   sites (Zavvi, HMV, Amazon, Blu-ray.com, HiDefNinja …) are never used as sources.
5. Descriptions are written onto the DRAFT product. The product is never published or activated by
   this stage — publishing remains a manual decision in Shopify.

Decisions taken:

| Question | Decision |
| --- | --- |
| How do drafted descriptions reach Shopify? | Written straight onto the DRAFT product; reviewed in Shopify before publishing |
| When does it run? | Automatically after every publish run that creates products (`--with-descriptions`, on by default once proven) |

## Where it fits

```
publish_catalog_to_shopify
  └─ run_catalog_shopify_publish()          (unchanged: creates DRAFT products, returns results)
       └─ outcome == "created" rows
            └─ run_product_description_drafting(results)   (new)
                 1. resolve distributor + official domains
                 2. find official page(s)
                 3. extract synopsis / contents / format / features (LLM, source text only)
                 4. verify every extracted line against the source text
                 5. fall back (official film page → own factual summary)
                 6. save record to Supabase
                 7. write descriptionHtml + SEO onto the DRAFT product (guarded)
```

A failure in the description stage never fails or rolls back the publish. It is reported separately
in the job summary and exit output.

## Components

### 1. Distributor registry — `app/rules/distributor_sources.py`

A static map from the catalogue `studio` / label value to the distributor's official domains and how
to search them. Covers every label seen so far plus the common UK boutique and major labels:

| Label (catalogue value examples) | Official domains | Search method |
| --- | --- | --- |
| Arrow, Arrow Video, Arrow Films | arrowfilms.com | Shopify barcode search |
| Toy Robot (Arrow sub-label) | toyrobotvideo.co.uk | Shopify barcode search |
| Eureka, Masters of Cinema | eurekavideo.co.uk | Shopify barcode search |
| Radiance, Transmission | radiancefilms.co.uk | Shopify barcode search |
| Second Sight | secondsightfilms.co.uk | Shopify barcode search |
| Criterion | criterion.com | Site search by title |
| StudioCanal | studiocanal.co.uk, studiocanal.com | Web search, domain-restricted |
| Sony Pictures | sonypictures.co.uk, sonypictures.com | Web search, domain-restricted |
| Disney | disney.co.uk, press.disney.co.uk | Web search, domain-restricted |
| Warner Bros | warnerbros.co.uk, warnerbros.com | Web search, domain-restricted |
| 88 Films, Indicator/Powerhouse, BFI, Vinegar Syndrome, Imprint, Umbrella, Severin, Kino Lorber | their own shops | Shopify barcode search where available |

Also holds a **blocklist** of domains that may never be cited (retailers, review and fan sites), used
as a second guard in verification.

Unknown labels: logged as `distributor_unmapped` so the registry can be extended; the title falls
through to the official-fallback step.

### 2. Page finder — `app/services/description_sources_service.py`

In order, stopping at the first confident match:

1. **Shopify store barcode lookup** (most boutique labels run on Shopify):
   `https://<domain>/search?q=<barcode>&type=product`, then the product's `.json` endpoint to read
   variant barcodes. A barcode match sets `edition_match_confirmed = true`.
2. **Title + format match on the same store** when the barcode is not listed yet (pre-orders often
   appear before barcodes are added). Match on normalised title, year and format (4K / Blu-ray /
   steelbook / limited). Sets `edition_match_confirmed = false` unless the page states the barcode or
   catalogue number.
3. **Domain-restricted web search** for majors without shops: OpenAI Responses API `web_search` tool
   with `allowed_domains` set to the registry domains only. Returns candidate URLs; pages are then
   fetched directly.
4. **Official fallback**: studio film site / press kit / official page from another territory (e.g.
   Criterion US for a UK Criterion release), marked `official_fallback`.

Page text is fetched with `httpx` and reduced to readable text with the stdlib HTML parser.
Fetched HTML is cached under `tmp/description_cache/<barcode>/` for audit and re-runs.

### 3. Extractor — `app/services/description_extraction_service.py`

One LLM call per title with **only the fetched official page text** as input and a strict JSON schema
(the same shape as `tmp/descriptions/new_drafts_20260930_descriptions.json`):

- `synopsis` — 2 paragraphs, 90–140 words, British English, spoiler-free, lightly edited from the
  distributor's own copy (no new facts).
- `edition_contents`, `technical_format`, `special_features` — copied line by line, not reworded.
- `features_status` — `announced` or `tbc`.
- `notes` — anything uncertain (edition mismatch, conflicting dates, split lines).

Model: `gpt-5-mini` via `OPENAI_API_KEY` (already used by the web app), called with `httpx`
so no new SDK dependency is required. Temperature 0.

### 4. Verifier — `app/services/description_verification_service.py`

Deterministic checks, no LLM:

- Every `special_features`, `edition_contents` and `technical_format` line must appear in the source
  text (normalised, fuzzy ratio ≥ 0.9). Lines that fail are **dropped** and listed in `notes`.
- Every `source_url` must be on the registry allowlist and not on the blocklist.
- Proper nouns in the synopsis (cast, director, character names) must appear in the source text or in
  our own catalogue/TMDB data for that title. Otherwise the record is marked `needs_review`.
- Synopsis length and paragraph count within bounds.

If verification removes all features, `features_status` becomes `tbc`.

### 5. Fallbacks

- **No official edition page, but an official film page exists**: synopsis from the film page;
  features `tbc`.
- **Nothing official found**: a short factual summary written from our own data only — catalogue
  title/format, and director, cast, year, genre and country from `catalog_items` / TMDB enrichment.
  TMDB overview text is **not** copied (it is user-contributed). Marked `synopsis_source = own_summary`.

### 6. Storage — new table `product_description_drafts`

Migration `supabase/migrations/2026MMDD_product_description_drafts.sql`:

| Column | Notes |
| --- | --- |
| `barcode`, `catalog_item_id`, `shopify_product_id` | keys |
| `distributor`, `source_type`, `source_urls`, `edition_match_confirmed` | provenance |
| `synopsis`, `synopsis_source`, `edition_contents`, `technical_format`, `special_features`, `features_status`, `notes` | content |
| `description_html`, `description_hash` | exactly what was written to Shopify |
| `status` | `drafted`, `applied`, `skipped_manual_edit`, `needs_review`, `failed` |
| `attempts`, `last_checked_at`, `applied_at`, `error` | operations |

### 7. Shopify writer (guarded)

Same approach as `scripts/publish/update_new_drafts_20260930_descriptions.py`:

- Only writes if the product is still `DRAFT` and the variant barcode matches.
- Only writes if the current `descriptionHtml` is empty **or** equals the last hash this stage wrote —
  so manual edits in Shopify are never overwritten (recorded as `skipped_manual_edit`).
- Sets `descriptionHtml` and `seo.description` only. Never sends `status`.
- Records `description_source` / `description_features_status` as product metafields so the state is
  visible in Shopify admin.

### 8. Weekly refresh for "to be confirmed" titles

`jobs/refresh_tbc_product_descriptions.py`, weekly cron: re-runs steps 2–7 for records with
`features_status = tbc` (or `source_type = none`) whose product is still DRAFT or ACTIVE, up to
~120 days after release. Uses the same manual-edit guard. When features are newly announced, the
description is updated and the change is logged.

## Publish job changes

`jobs/publish_catalog_to_shopify.py`:

- `--with-descriptions` (default on after rollout) and `--no-descriptions`.
- `--descriptions-only --barcodes …` to run the stage on existing products (backfill / re-run).
- Summary line gains `descriptions: announced=N tbc=N own_summary=N needs_review=N failed=N`.
- `--dry-run` runs research and prints the drafted JSON without writing to Shopify or Supabase.

Runtime: ~15–40s per title, run with a small worker pool (4). A 20-title publish adds ~3–5 minutes.
Cost: roughly 1–3p per title.

## Configuration

- `OPENAI_API_KEY` on the VM `.env` (currently only used by the web app).
- `DESCRIPTION_MODEL` (default `gpt-5-mini`), `DESCRIPTION_MAX_WORKERS` (default 4).
- No new Python dependencies (`httpx` is already installed).

## Testing

- Unit tests: registry lookup, Shopify barcode search parsing, verifier (feature-line matching,
  domain allow/blocklist, proper-noun check), HTML builder, Shopify writer guards (non-DRAFT, barcode
  mismatch, manual edit).
- **Golden set evaluation**: the 20 titles researched on 2026-09-30
  (`tmp/descriptions/new_drafts_20260930_descriptions.json`) become the regression set. Target:
  same source type and features status on ≥ 18/20, zero features not present on the source page,
  zero blocklisted URLs.

## Rollout

1. Build registry, page finder, extractor, verifier; run against the golden set in dry-run.
2. Add table + writer; run `--descriptions-only` on the next publish batch and review in Shopify.
3. Turn on `--with-descriptions` by default for publish runs.
4. Add the weekly TBC refresh cron.

## Risks

- **Distributor site changes** break a store adapter → falls back to web search; unmapped/failed
  lookups are visible in the summary.
- **Wrong edition** (e.g. standard vs limited) → barcode match preferred; title-only matches are
  flagged `edition_match_confirmed = false` in notes and metafields.
- **LLM rewording features** → verifier drops any line not found in the source text.
- **Copyright** → only official distributor/studio copy is used, as today.
