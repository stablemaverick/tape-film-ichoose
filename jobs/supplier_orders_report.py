#!/usr/bin/env python3
"""
Daily supplier purchase tally from live Shopify inventory (read-only).

Writes CSVs under ``--out-dir`` (cron uses tapester FTP:
``/srv/ftps/data/tapester/reports/supplier_orders``) and prints a Slack-ready
summary on stdout when ``--print-slack`` is set.

Usage::

    ./venv/bin/python -m jobs.supplier_orders_report --env-file .env.prod
    ./venv/bin/python -m jobs.supplier_orders_report --env-file .env.prod \\
        --out-dir /srv/ftps/data/tapester/reports/supplier_orders --print-slack
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _setup_logging(log_dir: Path, stem: str) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    log_path = log_dir / f"job_{stem}_{stamp}.log"
    fmt = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
    sh = logging.StreamHandler(sys.stderr)
    sh.setLevel(logging.INFO)
    sh.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
    root.addHandler(fh)
    root.addHandler(sh)
    return log_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Supplier orders needed report (Shopify read-only)")
    parser.add_argument("--env-file", default=".env", help="Path to .env (default: .env under repo root)")
    parser.add_argument("--out-dir", default=None, help="CSV output directory (default: tmp/)")
    parser.add_argument(
        "--print-slack",
        action="store_true",
        help="Print Slack summary text to stdout (for notify wrapper)",
    )
    args = parser.parse_args(argv)

    repo = _repo_root()
    os.chdir(repo)
    log_path = _setup_logging(repo / "logs", "supplier_orders_report")
    log = logging.getLogger("jobs.supplier_orders_report")
    log.info("Starting supplier_orders_report repo_root=%s log_file=%s", repo, log_path)

    from dotenv import load_dotenv

    env_path = repo / args.env_file if not os.path.isabs(args.env_file) else Path(args.env_file)
    if env_path.is_file():
        load_dotenv(env_path)
        log.info("Loaded environment from %s", env_path)

    try:
        from app.services.supplier_orders_report_service import run_supplier_orders_report

        result = run_supplier_orders_report(env_file=str(env_path), out_dir=args.out_dir)
        slack_text = result.pop("slack_text", "")
        log.info("supplier_orders_report finished: %s", result)
        print(f"[jobs.supplier_orders_report] SUCCESS — {json.dumps(result, default=str)}")
        if args.print_slack and slack_text:
            print(slack_text)
        return 0
    except SystemExit as e:
        code = e.code if isinstance(e.code, int) else 1
        log.error("Exited with code %s", code)
        return code
    except Exception:
        log.error("supplier_orders_report failed:\n%s", traceback.format_exc())
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
