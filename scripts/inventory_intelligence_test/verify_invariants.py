#!/usr/bin/env python3
"""
Verify Phase 3a/3b inventory intelligence invariants against a temporary seeded database.

Read-oriented for schema checks; performs one intentional duplicate-observation insert
attempt to prove dedupe (rolls back / expects unique violation — does not leave duplicates).

Requires SUPABASE_URL + SUPABASE_SERVICE_KEY for the temporary project
(or DATABASE_URL for psycopg2 path).

Never enable dual-write flags as part of this script.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


@dataclass
class CheckResult:
    scenario: str
    ok: bool
    detail: str


@dataclass
class Report:
    generated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    checks: List[CheckResult] = field(default_factory=list)
    flags: Dict[str, Any] = field(default_factory=dict)

    def add(self, scenario: str, ok: bool, detail: str) -> None:
        self.checks.append(CheckResult(scenario, ok, detail))

    @property
    def failed(self) -> List[CheckResult]:
        return [c for c in self.checks if not c.ok]


RELEASE_A = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
RELEASE_B = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2"
RELEASE_C = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3"
RELEASE_D = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa4"
RELEASE_E = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa5"
RELEASE_F1 = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa6"
RELEASE_F2 = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa7"
OFFER_MOOV_DELTA = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb1"
DEDUPE_KEY = "seed:obs:bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb1:in_stock|14|1|1|12.5000|GBP"


class SupabaseClient:
    def __init__(self, url: str, key: str):
        from supabase import create_client

        self.sb = create_client(url, key)

    def rows(self, table: str, **eq: Any) -> List[Dict[str, Any]]:
        q = self.sb.table(table).select("*")
        for k, v in eq.items():
            q = q.eq(k, v)
        return list(q.execute().data or [])

    def count(self, table: str, **eq: Any) -> int:
        q = self.sb.table(table).select("id", count="exact")
        for k, v in eq.items():
            q = q.eq(k, v)
        return int(q.limit(1).execute().count or 0)

    def insert(self, table: str, payload: Dict[str, Any]) -> Any:
        return self.sb.table(table).insert(payload).execute()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=".env.inventory-test")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    args = parser.parse_args()

    repo = _repo_root()
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))

    env_path = Path(args.env_file)
    if not env_path.is_absolute():
        env_path = repo / env_path
    if env_path.exists():
        load_dotenv(env_path, override=True)
    elif os.getenv("SUPABASE_URL") and os.getenv("SUPABASE_SERVICE_KEY"):
        # Allow CI/local where vars are exported without a file.
        print(f"Note: {env_path} not found; using process environment.", file=sys.stderr)
    else:
        print(
            f"Missing env file {env_path} and no SUPABASE_URL/SUPABASE_SERVICE_KEY in the environment.\n"
            "Create .env.inventory-test from scripts/inventory_intelligence_test/env.inventory-test.example "
            "(temporary project only).",
            file=sys.stderr,
        )
        return 2

    # Refuse known production project ref if somehow configured.
    url_early = (os.getenv("SUPABASE_URL") or "").strip()
    if "zdvjokkslhpoftimvdis" in url_early and (
        os.getenv("INVENTORY_TEST_ALLOW_PROD_URL") or ""
    ).strip() not in {"1", "true", "yes"}:
        print(
            "Refusing to run against the configured production Supabase URL.\n"
            "Use a temporary project (.env.inventory-test). "
            "Set INVENTORY_TEST_ALLOW_PROD_URL=1 only for an explicit emergency override.",
            file=sys.stderr,
        )
        return 2

    from app.config.inventory_dual_write import load_inventory_dual_write_flags
    from app.rules.availability_rules import (
        normalise_supplier_availability,
        observation_material_fingerprint,
    )
    from app.rules.inventory_invariant_rules import (
        validate_barcode_variant_conflicts,
        validate_preorder_no_positive_tape_on_hand,
        validate_purchase_order_line,
        validate_resolution_uniqueness,
        validate_supplier_must_not_update_tape,
        validate_supplier_only_has_no_tape_requirement,
        validate_tape_inventory_levels,
    )

    report = Report()
    flags = load_inventory_dual_write_flags()
    report.flags = {
        "enabled": flags.enabled,
        "shopify": flags.shopify_enabled,
        "supplier": flags.supplier_enabled,
        "purchase_orders": flags.po_enabled,
    }
    report.add(
        "dual_write_flags_off",
        not flags.enabled and not flags.shopify_enabled and not flags.supplier_enabled and not flags.po_enabled,
        f"flags={report.flags}",
    )

    url = (os.getenv("SUPABASE_URL") or "").strip()
    key = (os.getenv("SUPABASE_SERVICE_KEY") or "").strip()
    if not url or not key:
        print(
            "Missing SUPABASE_URL / SUPABASE_SERVICE_KEY for temporary project.",
            file=sys.stderr,
        )
        return 2

    # Refuse obvious production shop domains in this verifier context if set
    shop = (os.getenv("SHOPIFY_SHOP") or "").strip().lower()
    if shop and shop not in {"synthetic-test.myshopify.com", "127.0.0.1"} and "prod" in (
        os.getenv("INVENTORY_TEST_PROFILE") or ""
    ).lower():
        print("Refusing to run against a profile marked production.", file=sys.stderr)
        return 2

    db = SupabaseClient(url, key)

    # Schema presence
    for table in (
        "suppliers",
        "release_variants",
        "release_shopify_listings",
        "variant_identifiers",
        "tape_inventory_levels",
        "supplier_offers",
        "supplier_offer_observations",
        "supplier_sku_resolutions",
        "purchase_orders",
        "purchase_order_lines",
        "inventory_events",
    ):
        try:
            n = db.count(table)
            report.add(f"schema_{table}", True, f"count={n}")
        except Exception as exc:
            report.add(f"schema_{table}", False, str(exc)[:200])

    # --- Scenario checks ---
    def tape(release_id: str) -> Optional[Dict[str, Any]]:
        rows = db.rows("tape_inventory_levels", release_variant_id=release_id)
        return rows[0] if rows else None

    # 1) positive on-hand
    t_a = tape(RELEASE_A)
    ok = bool(t_a) and int(t_a["on_hand"]) == 5 and int(t_a["available"]) == 5
    report.add("shopify_linked_positive_on_hand", ok, str(t_a))

    # 2) committed
    t_b = tape(RELEASE_B)
    ok = bool(t_b) and int(t_b["committed"]) == 1 and int(t_b["available"]) == 2 and int(t_b["on_hand"]) == 3
    report.add("shopify_linked_committed", ok, str(t_b))
    if t_b:
        violations = validate_tape_inventory_levels(t_b)
        report.add(
            "committed_identity_rules",
            not any(v.severity == "error" for v in violations),
            f"warnings={[v.code for v in violations]}",
        )

    # 3) negative available preserved
    t_c = tape(RELEASE_C)
    ok = bool(t_c) and int(t_c["available"]) == -2 and int(t_c["on_hand"]) == 0 and int(t_c["committed"]) == 2
    report.add("negative_available_preserved", ok, str(t_c))
    if t_c:
        violations = validate_tape_inventory_levels(t_c)
        # Should warn committed_exceeds_on_hand but not error available_identity if consistent
        codes = {v.code for v in violations}
        report.add(
            "negative_available_is_warning_not_hard_fail",
            "available_identity_mismatch" not in codes and "committed_exceeds_on_hand" in codes,
            f"codes={sorted(codes)}",
        )

    # 4) both Moovies and Lasgo
    offers_d = db.rows("supplier_offers", release_variant_id=RELEASE_D)
    suppliers = {o.get("supplier_id") for o in offers_d}
    report.add(
        "dual_supplier_moovies_lasgo",
        suppliers >= {"moovies", "lasgo"},
        f"suppliers={sorted(suppliers)} count={len(offers_d)}",
    )

    # 5) supplier-only: no shopify listing, no tape inventory
    channels_e = db.rows("release_shopify_listings", release_variant_id=RELEASE_E)
    tape_e = tape(RELEASE_E)
    rel_e = db.rows("release_variants", id=RELEASE_E)
    status_e = (rel_e[0].get("publication_status") if rel_e else None)
    report.add(
        "supplier_only_no_shopify_listing",
        len(channels_e) == 0 and status_e == "supplier_only",
        f"channels={len(channels_e)} status={status_e}",
    )
    report.add("supplier_only_no_tape_inventory", tape_e is None, f"tape={tape_e}")
    so_viol = validate_supplier_only_has_no_tape_requirement(
        publication_status=status_e or "",
        has_tape_inventory_row=tape_e is not None,
    )
    report.add(
        "supplier_only_tape_requirement_rule",
        so_viol == [],
        f"violations={[v.code for v in so_viol]}",
    )

    # 6) missing barcode
    missing_bc = [
        o
        for o in db.rows("supplier_offers", supplier_sku="SYN-MOOV-NOBARCODE")
        if o.get("raw_barcode") in (None, "")
    ]
    report.add("offer_missing_barcode", len(missing_bc) == 1, f"rows={len(missing_bc)}")

    # 7) two offers sharing barcode
    shared = db.rows("supplier_offers", raw_barcode="9900000000006")
    report.add("two_offers_share_barcode", len(shared) == 2, f"count={len(shared)}")

    # 8) ambiguous identity resolution
    amb = db.rows("supplier_sku_resolutions", supplier_sku="SYN-MOOV-AMBIG")
    ok = (
        len(amb) == 1
        and amb[0].get("review_status") == "needs_review"
        and amb[0].get("match_method") == "barcode_ambiguous"
        and amb[0].get("resolved_release_variant_id") is None
    )
    report.add("ambiguous_identity_resolution", ok, str(amb[0] if amb else None))
    idents = db.rows("variant_identifiers", id_value="9900000000099")
    conflict_viol = validate_barcode_variant_conflicts(idents)
    report.add(
        "ambiguous_barcode_conflict_flagged",
        any(v.code == "barcode_maps_multiple_variants" for v in conflict_viol)
        and all(i.get("conflict_flag") for i in idents),
        f"idents={len(idents)} viol={[v.code for v in conflict_viol]}",
    )

    # 9) unresolved supplier SKU
    unr = db.rows("supplier_offers", supplier_sku="SYN-MOOV-UNRESOLVED")
    res_unr = db.rows("supplier_sku_resolutions", supplier_sku="SYN-MOOV-UNRESOLVED")
    ok = (
        len(unr) == 1
        and unr[0].get("release_variant_id") is None
        and len(res_unr) == 1
        and res_unr[0].get("review_status") == "needs_review"
    )
    report.add("unresolved_supplier_sku", ok, f"offer={unr[:1]} res={res_unr[:1]}")

    # 10) exact numeric quantity
    exact = db.rows("supplier_offers", id=OFFER_MOOV_DELTA)
    ok = (
        len(exact) == 1
        and exact[0].get("reported_quantity") == 14
        and exact[0].get("quantity_is_exact") is True
    )
    report.add("exact_numeric_quantity", ok, str(exact[0] if exact else None))

    # 11) non-numeric status, null quantity
    text_status = db.rows("supplier_offers", supplier_sku="SYN-LASGO-TEXTSTATUS")
    ok = (
        len(text_status) == 1
        and text_status[0].get("reported_quantity") is None
        and text_status[0].get("quantity_is_exact") is False
        and text_status[0].get("raw_status_text") == "In stock"
        and text_status[0].get("availability_status") == "in_stock"
    )
    report.add("non_numeric_status_null_qty", ok, str(text_status[0] if text_status else None))
    # Pure rule: never invent qty
    norm = normalise_supplier_availability(raw_status="In stock")
    report.add(
        "rule_never_invent_qty_from_text",
        norm.reported_quantity is None and norm.availability_status == "in_stock",
        str(norm),
    )

    # 12) open PO ordered + confirmed
    open_po = db.rows("purchase_orders", purchase_order_number="SYN-PO-OPEN-001")
    open_lines = db.rows("purchase_order_lines", purchase_order_id=open_po[0]["id"]) if open_po else []
    ok = (
        len(open_po) == 1
        and open_po[0].get("status") == "confirmed"
        and len(open_lines) == 1
        and open_lines[0].get("quantity_ordered") == 10
        and open_lines[0].get("quantity_confirmed") == 10
        and open_lines[0].get("quantity_received") == 0
    )
    report.add("open_po_ordered_confirmed", ok, str(open_lines[0] if open_lines else None))

    # 13) partially received PO
    part_po = db.rows("purchase_orders", purchase_order_number="SYN-PO-PARTIAL-002")
    part_lines = db.rows("purchase_order_lines", purchase_order_id=part_po[0]["id"]) if part_po else []
    ok = (
        len(part_po) == 1
        and part_po[0].get("status") == "partially_received"
        and len(part_lines) == 1
        and part_lines[0].get("quantity_ordered") == 6
        and part_lines[0].get("quantity_received") == 2
        and part_lines[0].get("quantity_received") < part_lines[0].get("quantity_ordered")
    )
    report.add("partially_received_po", ok, str(part_lines[0] if part_lines else None))
    if part_lines:
        pv = validate_purchase_order_line(part_lines[0])
        report.add(
            "partial_po_line_invariants",
            not any(v.severity == "error" for v in pv),
            f"viol={[v.code for v in pv]}",
        )

    # 14) unchanged observation dedupe
    before_obs = db.count("supplier_offer_observations", dedupe_key=DEDUPE_KEY)
    report.add("observation_seed_present", before_obs == 1, f"count={before_obs}")
    dup_blocked = False
    dup_error = ""
    try:
        db.insert(
            "supplier_offer_observations",
            {
                "supplier_offer_id": OFFER_MOOV_DELTA,
                "availability_status": "in_stock",
                "reported_quantity": 14,
                "quantity_is_exact": True,
                "supplier_can_supply": True,
                "unit_cost": 12.5,
                "currency": "GBP",
                "raw_payload": {"seed": True, "dup_attempt": True},
                "dedupe_key": DEDUPE_KEY,
            },
        )
    except Exception as exc:
        dup_blocked = True
        dup_error = str(exc)[:240]
    after_obs = db.count("supplier_offer_observations", dedupe_key=DEDUPE_KEY)
    report.add(
        "unchanged_observation_deduplicated",
        dup_blocked and after_obs == 1,
        f"blocked={dup_blocked} after_count={after_obs} err={dup_error}",
    )

    # Cross-cutting ownership / safety rules (pure)
    report.add(
        "supplier_must_not_update_tape_rule",
        any(
            v.code == "supplier_updates_tape_inventory"
            for v in validate_supplier_must_not_update_tape(
                tape_mutation_fields=["on_hand"], source="supplier_stock_sync"
            )
        ),
        "rule detects forbidden supplier→tape write",
    )
    report.add(
        "preorder_supplier_cannot_create_on_hand",
        any(
            v.severity == "error"
            for v in validate_preorder_no_positive_tape_on_hand(
                is_preorder=True, on_hand=2, source="supplier_feed"
            )
        ),
        "rule errors on supplier-created preorder on_hand",
    )

    # Resolution uniqueness for dual-supplier SKUs (each SKU once)
    resolutions = db.rows("supplier_sku_resolutions", active=True)
    ru = validate_resolution_uniqueness(resolutions)
    report.add(
        "active_resolution_uniqueness",
        not any(v.code == "supplier_sku_multiple_variants" for v in ru),
        f"viol={[v.code for v in ru]}",
    )

    # Event linked to observation
    ev = db.rows("inventory_events", dedupe_key="seed:evt:supplier_became_available:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa4")
    report.add(
        "inventory_event_links_observation",
        len(ev) == 1 and ev[0].get("observation_id") is not None,
        str(ev[0] if ev else None),
    )

    # Fingerprint stability for dedupe key material
    fp = observation_material_fingerprint(
        availability_status="in_stock",
        reported_quantity=14,
        quantity_is_exact=True,
        supplier_can_supply=True,
        unit_cost=12.5,
        currency="GBP",
    )
    report.add("observation_fingerprint_stable", "in_stock|14|1|1|12.5000|GBP" == fp, fp)

    # Output
    payload = {
        "generated_at": report.generated_at,
        "flags": report.flags,
        "passed": sum(1 for c in report.checks if c.ok),
        "failed": sum(1 for c in report.checks if not c.ok),
        "checks": [{"scenario": c.scenario, "ok": c.ok, "detail": c.detail} for c in report.checks],
    }
    if args.format == "json":
        print(json.dumps(payload, indent=2))
    else:
        print("=== Inventory intelligence invariant verification ===")
        print(f"generated_at: {report.generated_at}")
        print(f"flags: {report.flags}")
        print(f"passed={payload['passed']} failed={payload['failed']}")
        for c in report.checks:
            mark = "OK  " if c.ok else "FAIL"
            print(f"  [{mark}] {c.scenario}: {c.detail[:160]}")
        if report.failed:
            print("\nFailed scenarios:")
            for c in report.failed:
                print(f"  - {c.scenario}: {c.detail}")

    return 0 if not report.failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
