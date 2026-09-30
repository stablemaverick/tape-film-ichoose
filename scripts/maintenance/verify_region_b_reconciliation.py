#!/usr/bin/env python3
"""Verify Region B remediation products match live Shopify + II snapshots."""

from __future__ import annotations

import os
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from dotenv import load_dotenv

from app.clients.shopify_client import ShopifyClient
from app.clients.supabase_client import create_fresh_client
from app.helpers.text_helpers import clean_text

CHECKS = {
    "5061088921248": ("Pee Wees Big Adventure UK", 85.99),
    "5050629306033": ("Infernal Affairs Trilogy", 152.99),
    "5050629184334": ("Moonrise Kingdom", 74.99),
    "5050629947038": ("Before Trilogy", 158.99),
}

REACTIVATE_BARCODES = {
    "5050629184334",
    "5050629306033",
    "5027035026466",
    "5028836042433",
    "5028836041825",
    "5028836042594",
    "5028836042266",
    "5028836042662",
    "5028836042631",
    "5027035027661",
}

VARIANT_QUERY = """
query VerifyVariant($q: String!) {
  productVariants(first: 1, query: $q) {
    nodes {
      id
      barcode
      price
      inventoryPolicy
      product { id status }
    }
  }
}
"""


def main() -> int:
    env = os.getenv("ENV_FILE", ".env.prod")
    load_dotenv(_REPO / env, override=True)
    shop = os.getenv("SHOPIFY_SHOP", "").strip()
    sb = create_fresh_client()
    client = ShopifyClient()

    print(f"shop={shop}")
    ok = True
    for bc, (label, expected) in CHECKS.items():
        data = client.graphql(VARIANT_QUERY, {"q": f"barcode:{bc}"})
        nodes = (data.get("productVariants") or {}).get("nodes") or []
        if not nodes:
            print(f"FAIL {label} barcode={bc}: Shopify variant not found")
            ok = False
            continue
        v = nodes[0]
        live_price = float(v.get("price") or 0)
        vid = v.get("id")
        sl = (
            sb.table("shopify_listings")
            .select("price_amount,inventory_policy,product_status")
            .eq("shopify_variant_id", vid)
            .limit(1)
            .execute()
            .data
            or []
        )
        ii_price = sl[0].get("price_amount") if sl else None
        rsl = (
            sb.table("release_shopify_listings")
            .select("release_variant_id,is_primary")
            .eq("shopify_variant_id", vid)
            .execute()
            .data
            or []
        )
        price_ok = abs(live_price - expected) < 0.011
        ii_ok = ii_price is not None and abs(float(ii_price) - expected) < 0.011
        map_ok = len(rsl) > 0
        status = "OK" if price_ok and ii_ok and map_ok else "FAIL"
        if status == "FAIL":
            ok = False
        print(
            f"{status} {label} live={live_price:.2f} expected={expected:.2f} "
            f"ii={ii_price} mapping={len(rsl)} policy={v.get('inventoryPolicy')}"
        )

    for bc in sorted(REACTIVATE_BARCODES):
        data = client.graphql(VARIANT_QUERY, {"q": f"barcode:{bc}"})
        nodes = (data.get("productVariants") or {}).get("nodes") or []
        if not nodes:
            print(f"FAIL reactivate barcode={bc}: not found in Shopify")
            ok = False
            continue
        v = nodes[0]
        vid = v.get("id")
        status = (v.get("product") or {}).get("status")
        rsl = (
            sb.table("release_shopify_listings")
            .select("release_variant_id")
            .eq("shopify_variant_id", vid)
            .execute()
            .data
            or []
        )
        line_ok = status == "ACTIVE" and len(rsl) > 0
        if not line_ok:
            ok = False
        print(
            f"{'OK' if line_ok else 'FAIL'} reactivate bc={bc} status={status} "
            f"rsl_rows={len(rsl)} policy={v.get('inventoryPolicy')}"
        )

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
