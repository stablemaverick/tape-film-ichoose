#!/usr/bin/env python3
"""Studio-label supplier-backed inventoryPolicy sync (dry-run by default).

Default labels: Arrow, Second Sight, Criterion Collection.
Does not mutate Shopify unless --apply is explicitly passed.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from app.services.arrow_inventory_policy_sync_service import (
    ACTION_SET_CONTINUE,
    ACTION_SET_DENY,
    DEFAULT_ELIGIBLE_STUDIO_LABELS,
    PolicyDecision,
    run_studio_inventory_policy_sync,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Sync supplier-backed Shopify inventoryPolicy for configured studio labels. "
            "Dry-run by default. Mutations change inventoryPolicy only."
        )
    )
    parser.add_argument("--env", default=".env", help="Env file path")
    parser.add_argument("--api-version", default="2026-04")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply Shopify inventoryPolicy mutations (dry-run by default)",
    )
    parser.add_argument("--product-query", default="status:active")
    parser.add_argument(
        "--studios",
        default=",".join(DEFAULT_ELIGIBLE_STUDIO_LABELS),
        help="Comma-separated canonical labels (Arrow, Second Sight, Criterion Collection)",
    )
    parser.add_argument(
        "--extra-continue-protections",
        action="store_true",
        default=True,
        help="Skip CONTINUE→DENY for preorder/backorder metafields (default on)",
    )
    parser.add_argument(
        "--no-extra-continue-protections",
        action="store_false",
        dest="extra_continue_protections",
        help="Use legacy Arrow classifier only (no extra preorder/backorder overlay)",
    )
    parser.add_argument("--csv", default="", help="CSV output path")
    parser.add_argument("--json", default="", help="JSON output path")
    parser.add_argument("--changes-csv", default="", help="Optional CSV of proposed policy changes only")
    return parser


def _label_rollup(decisions: list[PolicyDecision], label: str) -> dict:
    rows = [d for d in decisions if d.normalized_studio == label]
    qty_pos = [d for d in rows if (d.shopify_qty or 0) > 0]
    qty_zero = [d for d in rows if d.shopify_qty is not None and d.shopify_qty <= 0]
    zero_sup = [d for d in qty_zero if d.available_suppliers]
    zero_no = [d for d in qty_zero if not d.available_suppliers]
    return {
        "label": label,
        "variants_examined": len(rows),
        "tape_stock_gt_0": len(qty_pos),
        "tape_stock_le_0": len(qty_zero),
        "zero_tape_supplier_available": len(zero_sup),
        "zero_tape_no_supplier_available": len(zero_no),
        "currently_continue": sum(1 for d in rows if d.inventory_policy == "CONTINUE"),
        "currently_deny": sum(1 for d in rows if d.inventory_policy == "DENY"),
        "proposed_deny_to_continue": sum(1 for d in rows if d.action == ACTION_SET_CONTINUE),
        "proposed_continue_to_deny": sum(1 for d in rows if d.action == ACTION_SET_DENY),
        "already_correct": sum(1 for d in rows if d.safety_classification == "already_correct"),
        "preorder_protected": sum(1 for d in rows if d.safety_classification == "preorder_protected"),
        "backorder_protected": sum(1 for d in rows if d.safety_classification == "backorder_protected"),
        "ambiguous_or_manual": sum(
            1
            for d in rows
            if d.safety_classification
            in {"ambiguous_requires_review", "deliberate_manual_continue_suspected"}
        ),
        "data_quality_flagged": sum(1 for d in rows if d.data_quality),
        "arrow_legacy_mismatches": sum(
            1 for d in rows if d.legacy_action and d.action != d.legacy_action
        ),
    }


def write_changes_csv(path: Path, decisions: list[PolicyDecision]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "title",
        "barcode",
        "studio",
        "normalized_studio",
        "shopify_qty",
        "tape_on_hand",
        "tape_available",
        "available_suppliers",
        "supplier_qty",
        "inventory_policy",
        "proposed_inventory_policy",
        "action",
        "reason",
        "safety_classification",
        "pre_order",
        "backorder",
        "media_release_date",
        "product_id",
        "variant_id",
    ]
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for d in decisions:
            if d.action not in {ACTION_SET_CONTINUE, ACTION_SET_DENY}:
                continue
            w.writerow({k: getattr(d, k) if k != "studio" else d.studio for k in fields})


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    labels = tuple(x.strip() for x in args.studios.split(",") if x.strip())
    decisions, summary = run_studio_inventory_policy_sync(
        env_file=args.env,
        api_version=args.api_version,
        apply=args.apply,
        product_query=args.product_query,
        csv_path=args.csv or None,
        json_path=args.json or None,
        labels=labels,
        extra_continue_protections=bool(args.extra_continue_protections),
    )

    rollups = [_label_rollup(decisions, lab) for lab in labels]
    deny_to_continue = [d for d in decisions if d.action == ACTION_SET_CONTINUE]
    continue_to_deny = [d for d in decisions if d.action == ACTION_SET_DENY]

    if args.changes_csv:
        write_changes_csv(Path(args.changes_csv), decisions)

    # Enrich JSON with rollups / change lists if written by the service.
    if summary.json_path:
        payload = json.loads(Path(summary.json_path).read_text(encoding="utf-8"))
        payload["label_rollups"] = rollups
        payload["deny_to_continue"] = [
            {
                "title": d.title,
                "barcode": d.barcode,
                "studio": d.studio,
                "normalized_studio": d.normalized_studio,
                "shopify_qty": d.shopify_qty,
                "tape_on_hand": d.tape_on_hand,
                "available_suppliers": d.available_suppliers,
                "supplier": d.supplier,
                "supplier_qty": d.supplier_qty,
                "inventory_policy": d.inventory_policy,
                "proposed_inventory_policy": d.proposed_inventory_policy,
                "reason": d.reason,
            }
            for d in deny_to_continue
        ]
        payload["continue_to_deny"] = [
            {
                "title": d.title,
                "barcode": d.barcode,
                "studio": d.studio,
                "normalized_studio": d.normalized_studio,
                "shopify_qty": d.shopify_qty,
                "available_suppliers": d.available_suppliers,
                "supplier_qty": d.supplier_qty,
                "inventory_policy": d.inventory_policy,
                "proposed_inventory_policy": d.proposed_inventory_policy,
                "reason": d.reason,
                "safety_classification": d.safety_classification,
                "pre_order": d.pre_order,
                "backorder": d.backorder,
                "media_release_date": d.media_release_date,
                "legacy_action": d.legacy_action,
            }
            for d in continue_to_deny
        ]
        Path(summary.json_path).write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    print("=== Studio Inventory Policy Sync ===")
    print(f"dry_run: {summary.dry_run}")
    print(f"apply: {args.apply}")
    print(f"labels: {list(labels)}")
    print(f"extra_continue_protections: {summary.extra_continue_protections}")
    print(f"products_scanned: {summary.products_scanned}")
    print(f"eligible_products: {summary.eligible_products}")
    print(f"arrow_products: {summary.arrow_products}")
    print(f"variants_examined: {summary.variants_examined}")
    print(f"zero_stock_variants: {summary.zero_stock_variants}")
    print(f"set_continue: {summary.set_continue}")
    print(f"set_deny: {summary.set_deny}")
    print(f"no_change: {summary.no_change}")
    print(f"skipped: {summary.skipped}")
    print(f"arrow_decision_mismatches: {summary.arrow_decision_mismatches}")
    print(f"applied_ok: {summary.applied_ok}")
    print(f"applied_failed: {summary.applied_failed}")
    print(f"csv_path: {summary.csv_path}")
    print(f"json_path: {summary.json_path}")
    print("LABEL_ROLLUPS=" + json.dumps(rollups))
    status = (
        f"{'failed' if summary.applied_failed else 'success'} "
        f"dry_run={1 if summary.dry_run else 0} "
        f"set_continue={summary.set_continue} set_deny={summary.set_deny} "
        f"applied_ok={summary.applied_ok} applied_failed={summary.applied_failed}"
    )
    print(f"STUDIO_INVENTORY_POLICY_SYNC_STATUS={status}")
    # Compatibility with stock-sync log parsers expecting the Arrow status key.
    print(f"ARROW_INVENTORY_POLICY_SYNC_STATUS={status}")
    return 2 if summary.applied_failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
