# Agent fuzzy retrieval harness (isolated)

Offline harness that scores **fixed catalog fixtures** against **agent-style user queries** using shared helpers from `app.helpers.tmdb_match_helpers` (`normalize_match_title`, `title_tokens`, `build_search_query_variants`). It does **not** import or change `tape-film-agent` production stubs.

## Run

From repo root:

```bash
python3 tests/agent_retrieval_harness/run_agent_retrieval_harness.py
python3 tests/agent_retrieval_harness/run_agent_retrieval_harness.py --top-k 1
```

- Default `--top-k 15`: pass if the expected row appears in the top 15 results.
- `--top-k 1`: strict mode (must be rank 1).

## Ranking / disambiguation workstream (v2 isolated)

Evaluates **baseline lexical** scoring vs **v2** boosts/penalties (franchise, year-marked film, rare tokens, season/part, collection, creator, anime stylization) with **per-component score breakdowns** on each result.

```bash
python3 tests/agent_retrieval_harness/run_agent_ranking_workstream.py
python3 tests/agent_retrieval_harness/run_agent_ranking_workstream.py --stress
```

- **`TOP1_EVAL_CASES`** (15 queries): expected top-1 set for v1.
- **`--stress`**: appends `ghost in the shell` → `ghost_sac_2045` to demonstrate baseline vs v2 when a year-marked film SKU competes.

Outputs (gitignored): `tests/agent_retrieval_harness/ranking_workstream_output/` — `rank_query_NN.json`, `ranking_workstream_summary.json`, `before_after_ranking.json`, `RANKING_WORKSTREAM_REPORT.md`.

Implementation: `media_ranking_v2.py`, `ranking_failure_buckets.py`, `run_agent_ranking_workstream.py`.

## Outputs (gitignored)

`tests/agent_retrieval_harness/output/`:

- `query_NN.json` — per query: ranked results, rank, pass/fail, bucket/fixability when failed
- `agent_retrieval_summary.json` / `.csv`
- `AGENT_RETRIEVAL_HARNESS_REPORT.md`

## Editing

- Catalog rows: `catalog_sample_rows.py`
- Queries → expected ids: `query_cases.py`
- Scoring only: `retrieval_engine.py` (harness-local; not TMDB production thresholds)
- V2 ranking experiments: `media_ranking_v2.py` (isolated; not wired to production agent)
