#!/usr/bin/env python3
"""
Repair script-created preorder inventory at the Tape fulfilment location.

Fixes the broken pattern: available=0, on_hand=committed (should be
available=-committed, on_hand=0 for CONTINUE preorders with no stock received).

Usage::

    ./venv/bin/python scripts/maintenance/repair_preorder_inventory.py --dry-run
    ./venv/bin/python scripts/maintenance/repair_preorder_inventory.py --barcodes 5027035030159,5027035030333
    ./venv/bin/python scripts/maintenance/repair_preorder_inventory.py --barcodes-file barcodes.txt
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from app.clients.shopify_client import ShopifyClient
from app.services.catalog_shopify_publish_service import shopify_inventory_location_id

DEFAULT_BARCODES = """
5027035030159 5027035030333 5027035030357 5027035030371 5027035030395
5027035030487 5027035030524 5028836042990 5028836043034 5051429990873
5051892257688 5051892257923 5055201854582 5056453209069 5056453209144
5056719202100 5056719202162 5060710975956 5061088922948
""".split()

QUERY = """
query ($q: String!, $locId: ID!) {
  productVariants(first: 1, query: $q) {
    nodes {
      barcode
      inventoryItem {
        id
        inventoryLevel(locationId: $locId) {
          quantities(names: ["available", "committed", "on_hand"]) {
            name
            quantity
          }
        }
      }
    }
  }
}
"""

MUTATION = """
mutation SetQty($input: InventorySetQuantitiesInput!, $idempotencyKey: String!) {
  inventorySetQuantities(input: $input) @idempotent(key: $idempotencyKey) {
    userErrors {
      field
      message
    }
  }
}
"""


def _parse_barcodes(args: argparse.Namespace) -> List[str]:
    out: List[str] = []
    if args.barcodes:
        out.extend(b.strip() for b in args.barcodes.split(",") if b.strip())
    if args.barcodes_file:
        text = Path(args.barcodes_file).read_text(encoding="utf-8")
        out.extend(line.strip() for line in text.splitlines() if line.strip())
    if not out and not args.barcodes and not args.barcodes_file:
        out = list(DEFAULT_BARCODES)
    seen: set[str] = set()
    uniq: List[str] = []
    for b in out:
        if b not in seen:
            seen.add(b)
            uniq.append(b)
    return uniq


def _qty_map(level: Dict[str, Any] | None) -> Dict[str, int]:
    if not level:
        return {}
    return {q["name"]: int(q["quantity"]) for q in level.get("quantities") or []}


def _fetch_level(client: ShopifyClient, barcode: str, tape_loc: str) -> tuple[str, Dict[str, int]]:
    data = client.graphql(QUERY, {"q": f"barcode:{barcode}", "locId": tape_loc})
    nodes = data.get("productVariants", {}).get("nodes") or []
    if not nodes:
        raise LookupError(f"variant not found for barcode={barcode}")
    inv_id = nodes[0]["inventoryItem"]["id"]
    level = nodes[0]["inventoryItem"].get("inventoryLevel")
    return inv_id, _qty_map(level)


def _set_qty(
    client: ShopifyClient,
    *,
    barcode: str,
    inv_id: str,
    tape_loc: str,
    name: str,
    target: int,
    current: int,
    ignore_compare: bool,
    dry_run: bool,
) -> None:
    if target == current:
        print(f"  skip {name} (already {current})")
        return
    print(f"  {name}: {current} -> {target}")
    if dry_run:
        return
    row: Dict[str, Any] = {
        "inventoryItemId": inv_id,
        "locationId": tape_loc,
        "quantity": target,
    }
    if ignore_compare:
        row["ignoreCompareQuantity"] = True
    else:
        row["changeFromQuantity"] = current
    idem = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"preorder-repair|{barcode}|{name}|{current}|{target}|{ignore_compare}",
        )
    )
    payload = client.graphql(
        MUTATION,
        {
            "idempotencyKey": idem,
            "input": {
                "name": name,
                "reason": "correction",
                "referenceDocumentUri": "tape-film://maintenance/repair_preorder_inventory",
                "quantities": [row],
            },
        },
    )
    errs = (payload.get("inventorySetQuantities") or {}).get("userErrors") or []
    if errs:
        raise RuntimeError(f"{name}: {errs}")


def repair_barcode(
    client: ShopifyClient,
    barcode: str,
    tape_loc: str,
    *,
    dry_run: bool,
) -> Dict[str, int]:
    inv_id, q = _fetch_level(client, barcode, tape_loc)
    committed = q.get("committed", 0)
    if committed <= 0:
        print(f"SKIP {barcode}: committed=0")
        return q

    target_avail = -committed
    print(
        f"{barcode}: before available={q.get('available', 0)} "
        f"committed={committed} on_hand={q.get('on_hand', 0)}"
    )

    _set_qty(
        client,
        barcode=barcode,
        inv_id=inv_id,
        tape_loc=tape_loc,
        name="available",
        target=target_avail,
        current=q.get("available", 0),
        ignore_compare=False,
        dry_run=dry_run,
    )

    inv_id, q = _fetch_level(client, barcode, tape_loc)
    _set_qty(
        client,
        barcode=barcode,
        inv_id=inv_id,
        tape_loc=tape_loc,
        name="on_hand",
        target=0,
        current=q.get("on_hand", 0),
        ignore_compare=True,
        dry_run=dry_run,
    )

    if not dry_run:
        _, q = _fetch_level(client, barcode, tape_loc)
    print(f"  after: {q}")
    return q


def main() -> int:
    parser = argparse.ArgumentParser(description="Repair script-created preorder inventory levels.")
    parser.add_argument("--barcodes", help="Comma-separated barcodes")
    parser.add_argument("--barcodes-file", help="Newline-separated barcodes file")
    parser.add_argument("--dry-run", action="store_true", help="Print planned changes only")
    parser.add_argument("--env", default=".env", help="Env file path (default: .env)")
    args = parser.parse_args()

    env_path = args.env if os.path.isabs(args.env) else str(_REPO / args.env)
    load_dotenv(env_path)

    barcodes = _parse_barcodes(args)
    if not barcodes:
        print("No barcodes to process.", file=sys.stderr)
        return 1

    tape_loc = shopify_inventory_location_id()
    if not tape_loc:
        print("SHOPIFY_INVENTORY_LOCATION_ID missing from .env", file=sys.stderr)
        return 1

    client = ShopifyClient()
    failed = 0
    for barcode in barcodes:
        try:
            repair_barcode(client, barcode, tape_loc, dry_run=args.dry_run)
        except Exception as exc:
            failed += 1
            print(f"ERROR {barcode}: {exc}")

    mode = "dry-run" if args.dry_run else "applied"
    print(f"\nDone ({mode}). processed={len(barcodes)} failed={failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
