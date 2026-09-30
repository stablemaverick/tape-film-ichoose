#!/usr/bin/env python3
"""Supplier offer → release_variant resolution coverage (read-only)."""

from __future__ import annotations

import json
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dotenv import load_dotenv
from supabase import create_client


def main() -> int:
    load_dotenv(".env", override=True)
    load_dotenv(".env.prod", override=True)
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])

    total = sb.table("supplier_offers").select("id", count="exact").eq("active", True).limit(0).execute().count
    resolved = (
        sb.table("supplier_offers")
        .select("id", count="exact")
        .eq("active", True)
        .not_.is_("release_variant_id", "null")
        .limit(0)
        .execute()
        .count
    )
    unresolved = (
        sb.table("supplier_offers")
        .select("id", count="exact")
        .eq("active", True)
        .is_("release_variant_id", "null")
        .limit(0)
        .execute()
        .count
    )

    by_supplier = {}
    for sid in ("moovies", "lasgo"):
        t = sb.table("supplier_offers").select("id", count="exact").eq("active", True).eq("supplier_id", sid).limit(0).execute().count
        r = (
            sb.table("supplier_offers")
            .select("id", count="exact")
            .eq("active", True)
            .eq("supplier_id", sid)
            .not_.is_("release_variant_id", "null")
            .limit(0)
            .execute()
            .count
        )
        # sample sku shapes
        sample = (
            sb.table("supplier_offers")
            .select("supplier_sku")
            .eq("active", True)
            .eq("supplier_id", sid)
            .limit(200)
            .execute()
            .data
            or []
        )
        barcode_keyed = sum(1 for x in sample if str(x.get("supplier_sku") or "").startswith("barcode:"))
        by_supplier[sid] = {
            "total": t,
            "resolved": r,
            "unresolved": (t or 0) - (r or 0),
            "sample_barcode_keyed_pct": round(100.0 * barcode_keyed / max(len(sample), 1), 1),
        }

    out = {
        "total_supplier_offers_active": total,
        "resolved_to_release_variant": resolved,
        "unresolved": unresolved,
        "by_supplier": by_supplier,
        "note": "Ambiguous SKU counts not computed (unique identity is supplier_id+supplier_sku).",
    }
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
