# tape-film-agent

Scaffold for Tape Film agent services, jobs, and integrations.

## Run locally

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

## Job Entrypoints

These wrappers call the existing pipeline scripts from the legacy project root.
Set `TAPE_FILM_LEGACY_ROOT` if needed (defaults to parent of this repo).

```bash
python -m jobs.run_daily_stock_sync
python -m jobs.run_catalog_ingest
```
