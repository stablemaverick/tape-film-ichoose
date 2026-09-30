#!/usr/bin/env python3
"""
Ad hoc publish: barcodes from ``catalog_items`` → new Shopify products (separate from store/inventory sync).

Usage::

    ./venv/bin/python -m jobs.publish_catalog_to_shopify --barcodes 123,456
    ./venv/bin/python -m jobs.publish_catalog_to_shopify --barcodes-file path.txt --dry-run
    ./venv/bin/python -m jobs.publish_catalog_to_shopify --barcodes-file path.txt --no-descriptions
    ./venv/bin/python -m jobs.publish_catalog_to_shopify --barcodes 123 --descriptions-only

Descriptions are drafted from official distributor sources and saved onto newly created DRAFT products by
default (``--no-descriptions`` to skip). The same stage fills the Australian classification metafields when
empty (``--no-classification`` to skip).
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


def _parse_barcodes_from_args(args: argparse.Namespace) -> list[str]:
    from app.services.catalog_shopify_publish_service import normalize_barcodes

    raw: list[str] = []
    if args.barcodes:
        raw.extend([b.strip() for b in args.barcodes.split(",") if b.strip()])
    if args.barcodes_file:
        p = Path(args.barcodes_file)
        text = p.read_text(encoding="utf-8")
        raw.extend(text.splitlines())
    return normalize_barcodes(raw)


def _run_descriptions(
    args: argparse.Namespace,
    repo: Path,
    log: logging.Logger,
    product_ids: dict[str, str | None],
    *,
    apply: bool,
) -> dict:
    from dotenv import load_dotenv

    from app.clients.shopify_client import ShopifyClient
    from app.clients.supabase_client import create_fresh_client
    from app.services.product_description_writer_service import run_description_stage

    load_dotenv(args.env_file)
    tmdb = None
    if os.getenv("TMDB_API_KEY"):
        from app.clients.tmdb_client import TmdbClient

        tmdb = TmdbClient()

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = repo / "logs" / "descriptions" / f"descriptions_{stamp}.json"
    log.info("description stage: barcodes=%d apply=%s", len(product_ids), apply)
    stage = run_description_stage(
        product_ids_by_barcode=product_ids,
        supabase=create_fresh_client(args.env_file),
        shopify=ShopifyClient(api_version=args.api_version),
        apply=apply,
        out_path=out_path,
        tmdb=tmdb,
        force=args.force_descriptions,
        classify=args.with_classification,
    )
    for record in stage["records"]:
        cls = record.get("au_classification") or {}
        line = (
            f"DESCRIPTION {record.get('apply_status')} barcode={record['barcode']} "
            f"source={record.get('source_type')} features={record.get('features_status')} "
            f"classification={cls.get('rating')}:{record.get('classification_status')} "
            f"review={','.join(record.get('review_reasons') or [])} {record.get('apply_message') or ''}"
        )
        log.info("%s", line)
        print(line)
    summary = stage["summary"]
    log.info("description summary=%s", summary)
    print(f"[jobs.publish_catalog_to_shopify] DESCRIPTIONS — {summary}")
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Publish selected catalog barcodes to Shopify (ad hoc flow; not store sync)."
    )
    parser.add_argument("--barcodes", default=None, help="Comma-separated barcodes")
    parser.add_argument("--barcodes-file", default=None, help="Newline-separated barcodes file")
    parser.add_argument(
        "--supplier",
        default="best_offer",
        help="best_offer (default) or supplier name (e.g. moovies, lasgo, Tape Film)",
    )
    parser.add_argument(
        "--status",
        choices=["draft", "active", "archived"],
        default="draft",
        help="Shopify product status for created products",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--api-version", default="2026-04")
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to .env (default .env under repo root)",
    )
    parser.add_argument(
        "--json-summary",
        action="store_true",
        help="Print per-barcode results as JSON to stdout after the run",
    )
    parser.add_argument(
        "--no-publish-flags",
        action="store_true",
        help="Only write shopify_product_id / shopify_variant_id; skip published_to_shopify / shopify_published_at",
    )
    parser.add_argument(
        "--no-descriptions",
        dest="with_descriptions",
        action="store_false",
        help="Skip the description stage. By default, after publishing, descriptions are drafted from official "
        "distributor sources and saved onto the newly created DRAFT products (with --dry-run: JSON only)",
    )
    parser.add_argument("--with-descriptions", dest="with_descriptions", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-classification",
        dest="with_classification",
        action="store_false",
        help="Skip the Australian classification lookup (custom.classification_description / custom.classification). "
        "By default it runs with the description stage and only fills products where it is empty",
    )
    parser.add_argument(
        "--descriptions-only",
        action="store_true",
        help="Skip publishing; draft and save descriptions (and empty AU classifications) for existing DRAFT "
        "products with these barcodes",
    )
    parser.add_argument(
        "--force-descriptions",
        action="store_true",
        help="Overwrite existing descriptions even if they were edited outside this stage (still DRAFT only)",
    )
    args = parser.parse_args(argv)

    repo = _repo_root()
    os.chdir(repo)
    log_path = _setup_logging(repo / "logs", "publish_catalog_to_shopify")
    log = logging.getLogger("jobs.publish_catalog_to_shopify")
    log.info("Starting publish_catalog_to_shopify repo_root=%s log_file=%s", repo, log_path)

    barcodes = _parse_barcodes_from_args(args)
    if not barcodes:
        log.error("Provide --barcodes and/or --barcodes-file")
        return 1

    try:
        if args.descriptions_only:
            if args.dry_run:
                product_ids = {b: None for b in barcodes}
            else:
                from dotenv import load_dotenv

                from app.clients.shopify_client import ShopifyClient

                load_dotenv(args.env_file)
                from app.services.product_description_writer_service import resolve_product_ids

                product_ids = resolve_product_ids(ShopifyClient(api_version=args.api_version), barcodes)
            summary = _run_descriptions(args, repo, log, product_ids, apply=not args.dry_run)
            failed = summary["outcomes"].get("failed", 0)
            return 0 if failed == 0 else 2

        from app.services.catalog_shopify_publish_service import run_catalog_shopify_publish

        result = run_catalog_shopify_publish(
            barcodes=barcodes,
            supplier_mode=args.supplier,
            shopify_status=args.status,
            dry_run=args.dry_run,
            env_file=args.env_file,
            api_version=args.api_version,
            set_publish_flags=not args.no_publish_flags,
        )
        for row in result["results"]:
            log.info("%s", row)
        log.info("summary=%s", result["summary"])
        print(f"[jobs.publish_catalog_to_shopify] SUCCESS — {result['summary']}")
        if args.json_summary:
            print(json.dumps(result["results"], indent=2, default=str))

        if args.with_descriptions:
            wanted = "dry_run" if args.dry_run else "created"
            product_ids = {
                row["barcode"]: row.get("shopify_product_id")
                for row in result["results"]
                if row.get("outcome") == wanted
            }
            if product_ids:
                try:
                    _run_descriptions(args, repo, log, product_ids, apply=not args.dry_run)
                except Exception:
                    log.error("description stage failed (publish unaffected):\n%s", traceback.format_exc())
                    print("[jobs.publish_catalog_to_shopify] DESCRIPTIONS FAILED — publish unaffected; see log")
            else:
                print("[jobs.publish_catalog_to_shopify] descriptions: no newly created products")
        return 0 if result["summary"].get("failed", 0) == 0 else 2
    except SystemExit as e:
        code = e.code if isinstance(e.code, int) else 1
        log.error("Exited with code %s", code)
        return code
    except Exception:
        log.error("publish_catalog_to_shopify failed:\n%s", traceback.format_exc())
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
