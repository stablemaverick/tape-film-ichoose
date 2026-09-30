#!/usr/bin/env python3
"""One-off: load researched descriptions onto the 2026-09-30 DRAFT products (status left unchanged)."""

from __future__ import annotations

import json
import os
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from dotenv import load_dotenv

load_dotenv(".env.prod", override=True)

from app.clients.shopify_client import ShopifyClient

SOURCE = ROOT / "tmp/descriptions/new_drafts_20260930_descriptions.json"

PRODUCT_UPDATE = """
mutation ProductUpdate($input: ProductInput!) {
  productUpdate(input: $input) {
    product { id title status }
    userErrors { field message }
  }
}
"""

PRODUCT_QUERY = """
query($id: ID!) {
  product(id: $id) {
    id title status
    variants(first: 1) { nodes { barcode } }
  }
}
"""


def _gid(value: str | int) -> str:
    value = str(value)
    return value if value.startswith("gid://") else f"gid://shopify/Product/{value}"


def _seo_description(synopsis: str) -> str:
    text = " ".join(synopsis.split())
    if len(text) <= 320:
        return text
    cut = text[:317]
    cut = cut[: cut.rfind(" ")] if " " in cut else cut
    return re.sub(r"[,;:\s]+$", "", cut) + "..."


def main() -> int:
    dry_run = "--dry-run" in sys.argv
    records = json.loads(SOURCE.read_text())
    shopify = ShopifyClient(api_version="2026-04")

    ok = failed = 0
    for rec in records:
        barcode = rec["barcode"]
        product_id = _gid(rec["shopify_product_id"])
        try:
            live = shopify.graphql(PRODUCT_QUERY, {"id": product_id})["product"]
            if not live:
                raise RuntimeError("product not found")
            if live.get("status") != "DRAFT":
                raise RuntimeError(f"expected DRAFT, got {live.get('status')}")
            live_barcode = ((live["variants"]["nodes"] or [{}])[0]).get("barcode")
            if live_barcode and live_barcode != barcode:
                raise RuntimeError(f"barcode mismatch: live={live_barcode}")

            print(f"{'DRY ' if dry_run else ''}UPDATE {barcode} {live['title'][:60]}")
            if dry_run:
                ok += 1
                continue

            upd = shopify.graphql(
                PRODUCT_UPDATE,
                {
                    "input": {
                        "id": product_id,
                        "descriptionHtml": rec["description_html"],
                        "seo": {"title": live["title"], "description": _seo_description(rec["synopsis"])},
                    }
                },
            )
            payload = upd.get("productUpdate") or {}
            errs = payload.get("userErrors") or []
            if errs:
                raise RuntimeError(f"productUpdate: {errs}")
            status = (payload.get("product") or {}).get("status")
            if status != "DRAFT":
                raise RuntimeError(f"status after update is {status}")
            print(f"  OK product={product_id} status={status}")
            ok += 1
            time.sleep(0.2)
        except Exception as exc:
            print(f"FAIL {barcode}: {exc}")
            failed += 1
            time.sleep(0.3)

    print(f"\nDone. ok={ok} failed={failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
