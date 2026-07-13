"""
Build a supplier purchase tally from live Shopify inventory at the Tape location.

Read-only: never mutates Shopify. Writes dated CSVs under ``tmp/`` (or ``out_dir``).

Produces:
  - **Outstanding** — full backlog still to buy (``supplier_orders_needed*.csv``)
  - **Daily delta** — vs previous snapshot (``supplier_orders_delta*.csv``), sorted by
    ``qty_delta`` descending (largest new/increased needs first; cleared are negative)

``qty_to_order`` ≈ units still owed to customers and not covered by on_hand/incoming.
"""

from __future__ import annotations

import csv
import os
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

from app.clients.shopify_client import ShopifyClient
from app.helpers.text_helpers import clean_text
from app.services.shopify_inventory_settings_audit import parse_shopify_bool_metafield

PRODUCTS_QUERY = """
query SupplierOrdersReport($cursor: String, $locId: ID!) {
  products(first: 25, after: $cursor, query: "status:active OR status:draft") {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      id
      title
      status
      preOrder: metafield(namespace: "custom", key: "pre_order") { value }
      preorderAlt: metafield(namespace: "custom", key: "preorder") { value }
      backorder: metafield(namespace: "custom", key: "backorder") { value }
      mediaRelease: metafield(namespace: "custom", key: "media_release_date") { value }
      variants(first: 50) {
        nodes {
          id
          sku
          barcode
          inventoryPolicy
          inventoryItem {
            tracked
            inventoryLevel(locationId: $locId) {
              quantities(names: ["available", "committed", "on_hand", "incoming"]) {
                name
                quantity
              }
            }
          }
        }
      }
    }
  }
}
"""

CSV_FIELDS = [
    "product_title",
    "barcode",
    "sku",
    "qty_to_order",
    "committed",
    "available",
    "on_hand",
    "incoming",
    "inventory_policy",
    "reason",
    "media_release_date",
    "pre_order_metafield",
    "backorder_metafield",
    "product_status",
    "shopify_product_id",
    "shopify_variant_id",
]

DELTA_CSV_FIELDS = [
    "change",
    "qty_delta",
    "qty_previous",
    "qty_to_order",
    "product_title",
    "barcode",
    "sku",
    "committed",
    "available",
    "on_hand",
    "incoming",
    "inventory_policy",
    "reason",
    "media_release_date",
    "product_status",
    "shopify_product_id",
    "shopify_variant_id",
]


@dataclass
class SupplierOrderRow:
    product_title: str
    barcode: str
    sku: str
    qty_to_order: int
    committed: int
    available: int
    on_hand: int
    incoming: int
    inventory_policy: str
    reason: str
    media_release_date: str
    pre_order_metafield: bool
    backorder_metafield: bool
    product_status: str
    shopify_product_id: str
    shopify_variant_id: str


@dataclass
class SupplierOrderDeltaRow:
    change: str  # new | increased | decreased | cleared
    qty_delta: int
    qty_previous: int
    qty_to_order: int
    product_title: str
    barcode: str
    sku: str
    committed: int
    available: int
    on_hand: int
    incoming: int
    inventory_policy: str
    reason: str
    media_release_date: str
    product_status: str
    shopify_product_id: str
    shopify_variant_id: str


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _env_path(env_file: str) -> Path:
    p = Path(env_file)
    if p.is_absolute():
        return p
    return _repo_root() / env_file


def qty_to_order(available: int, committed: int, on_hand: int, incoming: int = 0) -> int:
    need = max(0, committed - max(0, on_hand) - max(0, incoming))
    if need == 0 and available < 0:
        return -available
    return need


def parse_release_date(raw: Any) -> Optional[date]:
    text = clean_text(raw) if not isinstance(raw, str) else (raw or "").strip()
    if not text:
        return None
    s = text.strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _qty_map(level: Optional[Dict[str, Any]]) -> Dict[str, int]:
    if not level:
        return {}
    out: Dict[str, int] = {}
    for q in level.get("quantities") or []:
        name = clean_text(q.get("name"))
        if not name:
            continue
        try:
            out[name] = int(q.get("quantity") or 0)
        except (TypeError, ValueError):
            out[name] = 0
    return out


def _metafield_bool(product: Dict[str, Any], *keys: str) -> bool:
    for key in keys:
        mf = product.get(key)
        if isinstance(mf, dict) and mf.get("value") is not None:
            return bool(parse_shopify_bool_metafield(mf.get("value")))
    return False


def classify_supplier_need(
    *,
    is_preorder: bool,
    policy: str,
    backorder: bool,
    available: int,
    committed: int,
    on_hand: int,
    need: int,
) -> Optional[str]:
    """Return reason code if this variant should appear on the order list."""
    if need <= 0:
        return None
    if is_preorder:
        return "preorder"
    pol = (policy or "").upper()
    if pol == "CONTINUE" or backorder or available < 0 or committed > max(0, on_hand):
        if pol == "CONTINUE":
            return "continue_oos"
        if backorder:
            return "backorder"
        return "oversell_uncovered_committed"
    return None


def _write_csv(path: Path, rows: List[SupplierOrderRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        for r in sorted(rows, key=lambda x: (-x.qty_to_order, x.product_title or "")):
            w.writerow(asdict(r))


def _write_delta_csv(path: Path, rows: List[SupplierOrderDeltaRow]) -> None:
    """Write delta rows ordered by qty_delta descending (largest increases first)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=DELTA_CSV_FIELDS)
        w.writeheader()
        for r in sorted(rows, key=lambda x: (-x.qty_delta, x.product_title or "")):
            w.writerow(asdict(r))


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def load_previous_qty_by_variant(path: Path) -> Dict[str, Dict[str, Any]]:
    """Load prior snapshot keyed by shopify_variant_id."""
    if not path.is_file():
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            vid = clean_text(row.get("shopify_variant_id")) or ""
            if not vid:
                continue
            out[vid] = {
                "qty_to_order": _safe_int(row.get("qty_to_order")),
                "product_title": row.get("product_title") or "",
                "barcode": row.get("barcode") or "",
                "sku": row.get("sku") or "",
                "reason": row.get("reason") or "",
                "committed": _safe_int(row.get("committed")),
                "available": _safe_int(row.get("available")),
                "on_hand": _safe_int(row.get("on_hand")),
                "incoming": _safe_int(row.get("incoming")),
                "inventory_policy": row.get("inventory_policy") or "",
                "media_release_date": row.get("media_release_date") or "",
                "product_status": row.get("product_status") or "",
                "shopify_product_id": row.get("shopify_product_id") or "",
            }
    return out


def find_baseline_snapshot(out_dir: Path, stamp: str) -> Optional[Path]:
    """
    Prefer yesterday's dated file, else the most recent older dated snapshot,
    else ``supplier_orders_needed.csv`` from the previous run.
    """
    try:
        as_of = datetime.strptime(stamp, "%Y%m%d").date()
        yesterday = date.fromordinal(as_of.toordinal() - 1).strftime("%Y%m%d")
    except ValueError:
        yesterday = None

    if yesterday:
        ypath = out_dir / f"supplier_orders_needed_{yesterday}.csv"
        if ypath.is_file():
            return ypath

    dated = sorted(
        out_dir.glob("supplier_orders_needed_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9].csv")
    )
    older = [p for p in dated if p.stem.rsplit("_", 1)[-1] < stamp]
    if older:
        return older[-1]

    latest = out_dir / "supplier_orders_needed.csv"
    if latest.is_file():
        return latest
    return None


def compute_daily_delta(
    current: List[SupplierOrderRow],
    previous_by_variant: Dict[str, Dict[str, Any]],
) -> List[SupplierOrderDeltaRow]:
    """Diff current outstanding vs previous snapshot."""
    deltas: List[SupplierOrderDeltaRow] = []
    current_ids = set()

    for r in current:
        current_ids.add(r.shopify_variant_id)
        prev = previous_by_variant.get(r.shopify_variant_id)
        prev_qty = int(prev["qty_to_order"]) if prev else 0
        if prev is None:
            change = "new"
            qty_delta = r.qty_to_order
        elif r.qty_to_order > prev_qty:
            change = "increased"
            qty_delta = r.qty_to_order - prev_qty
        elif r.qty_to_order < prev_qty:
            change = "decreased"
            qty_delta = r.qty_to_order - prev_qty  # negative
        else:
            continue  # unchanged — omit from delta file
        deltas.append(
            SupplierOrderDeltaRow(
                change=change,
                qty_delta=qty_delta,
                qty_previous=prev_qty,
                qty_to_order=r.qty_to_order,
                product_title=r.product_title,
                barcode=r.barcode,
                sku=r.sku,
                committed=r.committed,
                available=r.available,
                on_hand=r.on_hand,
                incoming=r.incoming,
                inventory_policy=r.inventory_policy,
                reason=r.reason,
                media_release_date=r.media_release_date,
                product_status=r.product_status,
                shopify_product_id=r.shopify_product_id,
                shopify_variant_id=r.shopify_variant_id,
            )
        )

    for vid, prev in previous_by_variant.items():
        if vid in current_ids:
            continue
        prev_qty = int(prev.get("qty_to_order") or 0)
        if prev_qty <= 0:
            continue
        deltas.append(
            SupplierOrderDeltaRow(
                change="cleared",
                qty_delta=-prev_qty,
                qty_previous=prev_qty,
                qty_to_order=0,
                product_title=str(prev.get("product_title") or ""),
                barcode=str(prev.get("barcode") or ""),
                sku=str(prev.get("sku") or ""),
                committed=int(prev.get("committed") or 0),
                available=int(prev.get("available") or 0),
                on_hand=int(prev.get("on_hand") or 0),
                incoming=int(prev.get("incoming") or 0),
                inventory_policy=str(prev.get("inventory_policy") or ""),
                reason=str(prev.get("reason") or ""),
                media_release_date=str(prev.get("media_release_date") or ""),
                product_status=str(prev.get("product_status") or ""),
                shopify_product_id=str(prev.get("shopify_product_id") or ""),
                shopify_variant_id=vid,
            )
        )

    return deltas


def collect_supplier_order_rows(
    client: ShopifyClient,
    location_id: str,
    *,
    today: Optional[date] = None,
    sleep_s: float = 0.2,
) -> Tuple[List[SupplierOrderRow], List[SupplierOrderRow]]:
    """Return (preorder_rows, other_oversell_rows)."""
    as_of = today or date.today()
    preorder_rows: List[SupplierOrderRow] = []
    other_rows: List[SupplierOrderRow] = []
    cursor: Optional[str] = None

    while True:
        data = client.graphql(PRODUCTS_QUERY, {"cursor": cursor, "locId": location_id})
        conn = (data or {}).get("products") or {}
        for product in conn.get("nodes") or []:
            pre = _metafield_bool(product, "preOrder", "preorderAlt")
            back = _metafield_bool(product, "backorder")
            release = parse_release_date(
                ((product.get("mediaRelease") or {}) if isinstance(product.get("mediaRelease"), dict) else {}).get(
                    "value"
                )
            )
            is_preorder = pre or (release is not None and release > as_of)
            title = clean_text(product.get("title")) or ""
            status = clean_text(product.get("status")) or ""
            product_id = clean_text(product.get("id")) or ""

            for variant in ((product.get("variants") or {}).get("nodes") or []):
                inv = variant.get("inventoryItem") or {}
                q = _qty_map(inv.get("inventoryLevel"))
                available = q.get("available", 0)
                committed = q.get("committed", 0)
                on_hand = q.get("on_hand", 0)
                incoming = q.get("incoming", 0)
                policy = (clean_text(variant.get("inventoryPolicy")) or "").upper()
                need = qty_to_order(available, committed, on_hand, incoming)
                reason = classify_supplier_need(
                    is_preorder=is_preorder,
                    policy=policy,
                    backorder=back,
                    available=available,
                    committed=committed,
                    on_hand=on_hand,
                    need=need,
                )
                if not reason:
                    continue
                row = SupplierOrderRow(
                    product_title=title,
                    barcode=clean_text(variant.get("barcode")) or "",
                    sku=clean_text(variant.get("sku")) or "",
                    qty_to_order=need,
                    committed=committed,
                    available=available,
                    on_hand=on_hand,
                    incoming=incoming,
                    inventory_policy=policy,
                    reason=reason,
                    media_release_date=release.isoformat() if release else "",
                    pre_order_metafield=pre,
                    backorder_metafield=back,
                    product_status=status,
                    shopify_product_id=product_id,
                    shopify_variant_id=clean_text(variant.get("id")) or "",
                )
                if reason == "preorder":
                    preorder_rows.append(row)
                else:
                    other_rows.append(row)

        page = conn.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            break
        cursor = page.get("endCursor")
        if sleep_s > 0:
            time.sleep(sleep_s)

    return preorder_rows, other_rows


def format_slack_summary(
    *,
    preorder_rows: List[SupplierOrderRow],
    other_rows: List[SupplierOrderRow],
    delta_rows: List[SupplierOrderDeltaRow],
    combined_path: Path,
    delta_path: Path,
    baseline_path: Optional[Path],
    host: str,
    top_n: int = 8,
) -> str:
    combined = preorder_rows + other_rows
    pre_units = sum(r.qty_to_order for r in preorder_rows)
    other_units = sum(r.qty_to_order for r in other_rows)
    total_units = pre_units + other_units

    # Largest positive deltas first (what to order today).
    delta_desc = sorted(delta_rows, key=lambda x: -x.qty_delta)
    new_or_up = [d for d in delta_desc if d.change in ("new", "increased")]
    cleared = [d for d in delta_rows if d.change == "cleared"]
    decreased = [d for d in delta_rows if d.change == "decreased"]
    delta_units = sum(d.qty_delta for d in new_or_up)
    cleared_units = sum(-d.qty_delta for d in cleared)

    lines = [
        f"📦 supplier orders needed host={host}",
        f"OUTSTANDING total: {len(combined)} lines / {total_units} units",
        f"  preorder: {len(preorder_rows)} lines / {pre_units} units",
        f"  continue/oversell: {len(other_rows)} lines / {other_units} units",
    ]
    if baseline_path is None:
        lines.append(
            "DELTA vs previous: no baseline yet (first run — treat outstanding as starting point)"
        )
    else:
        lines.append(
            f"DELTA since {baseline_path.name}: "
            f"+{delta_units} units to order "
            f"({len(new_or_up)} new/increased), "
            f"cleared {cleared_units} units ({len(cleared)} lines), "
            f"decreased {len(decreased)} lines"
        )
    lines.append(f"outstanding csv: {combined_path}")
    lines.append(f"delta csv: {delta_path}")

    if new_or_up:
        lines.append("delta to order (new/increased, largest first):")
        for d in new_or_up[:top_n]:
            bc = d.barcode or "(no barcode)"
            lines.append(
                f"  +{d.qty_delta}× {bc} — {d.product_title[:65]} "
                f"({d.change}, now {d.qty_to_order})"
            )
    elif combined:
        lines.append("outstanding top:")
        for r in sorted(combined, key=lambda x: -x.qty_to_order)[:top_n]:
            bc = r.barcode or "(no barcode)"
            lines.append(f"  {r.qty_to_order}× {bc} — {r.product_title[:70]}")
    return "\n".join(lines)


def run_supplier_orders_report(
    *,
    env_file: str = ".env",
    out_dir: Optional[str] = None,
) -> Dict[str, Any]:
    path = _env_path(env_file)
    load_dotenv(path)

    location_id = clean_text(os.getenv("SHOPIFY_INVENTORY_LOCATION_ID")) or ""
    if not location_id:
        # Tape fulfilment location — prefer env; keep a stable fallback for reports.
        location_id = "gid://shopify/Location/78213775584"
    if not clean_text(os.getenv("SHOPIFY_SHOP")):
        raise SystemExit("Missing SHOPIFY_SHOP")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    base = Path(out_dir) if out_dir else (_repo_root() / "tmp")
    base.mkdir(parents=True, exist_ok=True)

    # Diff against previous snapshot *before* overwriting latest.
    baseline_path = find_baseline_snapshot(base, stamp)
    previous_by_variant = load_previous_qty_by_variant(baseline_path) if baseline_path else {}

    client = ShopifyClient()
    preorder_rows, other_rows = collect_supplier_order_rows(client, location_id)
    combined: List[SupplierOrderRow] = []
    seen = set()
    for r in preorder_rows + other_rows:
        if r.shopify_variant_id in seen:
            continue
        seen.add(r.shopify_variant_id)
        combined.append(r)

    delta_rows = compute_daily_delta(combined, previous_by_variant)

    paths = {
        "preorder": base / f"supplier_orders_preorder_{stamp}.csv",
        "continue_oos": base / f"supplier_orders_continue_oos_{stamp}.csv",
        "combined": base / f"supplier_orders_needed_{stamp}.csv",
        "delta": base / f"supplier_orders_delta_{stamp}.csv",
        "latest_combined": base / "supplier_orders_needed.csv",
        "latest_preorder": base / "supplier_orders_preorder.csv",
        "latest_continue": base / "supplier_orders_continue_oos.csv",
        "latest_delta": base / "supplier_orders_delta.csv",
    }
    _write_csv(paths["preorder"], preorder_rows)
    _write_csv(paths["continue_oos"], other_rows)
    _write_csv(paths["combined"], combined)
    _write_delta_csv(paths["delta"], delta_rows)
    _write_csv(paths["latest_combined"], combined)
    _write_csv(paths["latest_preorder"], preorder_rows)
    _write_csv(paths["latest_continue"], other_rows)
    _write_delta_csv(paths["latest_delta"], delta_rows)

    pre_units = sum(r.qty_to_order for r in preorder_rows)
    other_units = sum(r.qty_to_order for r in other_rows)
    new_or_up = [d for d in delta_rows if d.change in ("new", "increased")]
    cleared = [d for d in delta_rows if d.change == "cleared"]

    return {
        "status": "ok",
        "job": "supplier_orders_report",
        "as_of_utc": datetime.now(timezone.utc).isoformat(),
        "baseline_path": str(baseline_path.resolve()) if baseline_path else None,
        "preorder_lines": len(preorder_rows),
        "preorder_units": pre_units,
        "continue_oos_lines": len(other_rows),
        "continue_oos_units": other_units,
        "combined_lines": len(combined),
        "combined_units": pre_units + other_units,
        "delta_new_or_increased_lines": len(new_or_up),
        "delta_new_or_increased_units": sum(d.qty_delta for d in new_or_up),
        "delta_cleared_lines": len(cleared),
        "delta_cleared_units": sum(-d.qty_delta for d in cleared),
        "paths": {k: str(v.resolve()) for k, v in paths.items()},
        "slack_text": format_slack_summary(
            preorder_rows=preorder_rows,
            other_rows=other_rows,
            delta_rows=delta_rows,
            combined_path=paths["latest_combined"].resolve(),
            delta_path=paths["latest_delta"].resolve(),
            baseline_path=baseline_path,
            host=os.uname().nodename if hasattr(os, "uname") else "unknown",
        ),
    }
