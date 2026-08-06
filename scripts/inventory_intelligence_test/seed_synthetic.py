#!/usr/bin/env python3
"""
Load synthetic inventory-intelligence SQL into a temporary Supabase/Postgres target.

Uses DATABASE_URL (preferred) or SUPABASE_DB_URL. Does not use production credentials
from the repo; pass an explicit --env-file for the temporary project.

Example:
  DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:54322/postgres \\
    venv/bin/python scripts/inventory_intelligence_test/seed_synthetic.py

Or apply the SQL files via Supabase SQL editor / psql (see docs).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _read_sql(name: str) -> str:
    path = Path(__file__).resolve().parent / name
    return path.read_text(encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=None, help="Optional env file for temporary project")
    parser.add_argument(
        "--bootstrap",
        action="store_true",
        help="Also apply 00_bootstrap_prereq_tables.sql before seed",
    )
    parser.add_argument(
        "--apply-foundation",
        action="store_true",
        help="Also apply inventory foundation migration before seed",
    )
    args = parser.parse_args()

    repo = _repo_root()
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))

    if args.env_file:
        env_path = Path(args.env_file)
        if not env_path.is_absolute():
            env_path = repo / env_path
        load_dotenv(env_path, override=True)

    db_url = (
        os.getenv("DATABASE_URL")
        or os.getenv("SUPABASE_DB_URL")
        or os.getenv("POSTGRES_URL")
        or ""
    ).strip()
    if not db_url:
        print(
            "Missing DATABASE_URL / SUPABASE_DB_URL.\n"
            "Apply SQL files manually via psql or the Supabase SQL editor instead:\n"
            "  00_bootstrap_prereq_tables.sql\n"
            "  ../../supabase/migrations/20260806120000_inventory_intelligence_foundation.sql\n"
            "  02_seed_synthetic.sql",
            file=sys.stderr,
        )
        return 2

    try:
        import psycopg2  # type: ignore
    except ImportError:
        print(
            "psycopg2 is required for seed_synthetic.py. Install with:\n"
            "  venv/bin/pip install psycopg2-binary\n"
            "Or apply the .sql files with psql / Supabase SQL editor.",
            file=sys.stderr,
        )
        return 2

    statements: list[tuple[str, str]] = []
    if args.bootstrap:
        statements.append(("bootstrap", _read_sql("00_bootstrap_prereq_tables.sql")))
    if args.apply_foundation:
        foundation = repo / "supabase/migrations/20260806120000_inventory_intelligence_foundation.sql"
        statements.append(("foundation", foundation.read_text(encoding="utf-8")))
    statements.append(("seed", _read_sql("02_seed_synthetic.sql")))

    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            for label, sql in statements:
                print(f"Applying {label}…")
                cur.execute(sql)
        conn.commit()
        print("Synthetic seed applied successfully.")
        return 0
    except Exception as exc:
        conn.rollback()
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
