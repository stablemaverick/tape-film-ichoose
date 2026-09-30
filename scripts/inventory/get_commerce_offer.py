#!/usr/bin/env python3
"""CLI for Commerce Offer V1."""

from __future__ import annotations

import argparse
import json
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dotenv import load_dotenv
from supabase import create_client

from app.services.commerce_offer_service import CommerceOfferService
from app.services.stock_availability_service import StockAvailabilityError


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--env-file", default=".env")
    p.add_argument("--release-variant-id")
    p.add_argument("--barcode")
    p.add_argument("--shopify-variant-id")
    p.add_argument("--public-only", action="store_true")
    args = p.parse_args()
    load_dotenv(args.env_file, override=True)
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])
    try:
        out = CommerceOfferService(sb).get_commerce_offer(
            release_variant_id=args.release_variant_id,
            barcode=args.barcode,
            shopify_variant_id=args.shopify_variant_id,
            include_internal=not args.public_only,
        )
    except StockAvailabilityError as e:
        print(json.dumps(e.to_dict()))
        return 1
    if args.public_only:
        print(json.dumps(out["public"], default=str))
    else:
        print(json.dumps(out, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
