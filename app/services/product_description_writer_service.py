"""
Apply drafted descriptions to Shopify products (guarded) and record them in ``product_description_drafts``.

Guards, in order: product exists → status allowed (DRAFT by default) → a variant carries the barcode →
the current description is empty or is exactly what this stage last wrote (``custom.description_hash``
metafield), so manual edits in Shopify are never overwritten. Only ``descriptionHtml``, the SEO
description and ``custom.description_*`` metafields are written; product status is never sent.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from app.services.product_description_drafting_service import DEFAULT_MODEL, run_product_description_drafting

log = logging.getLogger(__name__)

METAFIELD_NAMESPACE = "custom"
DRAFTS_TABLE = "product_description_drafts"
_CLASSIFICATION_COLUMNS = ("au_classification", "au_classification_source", "au_classification_status")

PRODUCT_QUERY = """
query($id: ID!) {
  product(id: $id) {
    id
    title
    status
    descriptionHtml
    variants(first: 10) { nodes { barcode } }
    descriptionHash: metafield(namespace: "custom", key: "description_hash") { value }
    classificationDescription: metafield(namespace: "custom", key: "classification_description") { value }
  }
}
"""

PRODUCT_UPDATE = """
mutation ProductUpdate($input: ProductInput!) {
  productUpdate(input: $input) {
    product { id status descriptionHtml }
    userErrors { field message }
  }
}
"""

METAFIELDS_SET = """
mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields { key }
    userErrors { field message }
  }
}
"""


def description_hash(html: str) -> str:
    return hashlib.sha256((html or "").strip().encode("utf-8")).hexdigest()[:16]


def seo_description(synopsis: str) -> str:
    text = " ".join((synopsis or "").split())
    if len(text) <= 320:
        return text
    cut = text[:317]
    cut = cut[: cut.rfind(" ")] if " " in cut else cut
    return re.sub(r"[,;:\s]+$", "", cut) + "..."


def _source_label(record: Dict[str, Any]) -> str:
    return f"{record.get('source_type') or 'none'} / synopsis: {record.get('synopsis_source') or 'none'}"


def apply_description(
    shopify: Any,
    *,
    product_id: str,
    record: Dict[str, Any],
    allowed_statuses: Iterable[str] = ("DRAFT",),
    force: bool = False,
) -> Dict[str, Any]:
    """Write one drafted description (and AU classification, if empty) to Shopify.

    Returns ``{"status", "message", "hash", "classification_status", "classification_message"}``.
    """
    barcode = str(record["barcode"])
    live = (shopify.graphql(PRODUCT_QUERY, {"id": product_id}) or {}).get("product")
    if not live:
        return {"status": "failed", "message": f"product not found: {product_id}"}
    if live.get("status") not in set(allowed_statuses):
        return {"status": "skipped_not_draft", "message": f"product status is {live.get('status')}"}
    barcodes = {str(n.get("barcode") or "").strip() for n in (live.get("variants") or {}).get("nodes") or []}
    if barcode not in barcodes:
        return {"status": "skipped_barcode_mismatch", "message": f"variant barcodes {sorted(barcodes)}"}

    result = _apply_description_body(shopify, product_id=product_id, record=record, live=live, force=force)
    cls_status, cls_message = _apply_classification(shopify, product_id=product_id, record=record, live=live)
    result["classification_status"] = cls_status
    result["classification_message"] = cls_message
    return result


def _apply_classification(
    shopify: Any, *, product_id: str, record: Dict[str, Any], live: Dict[str, Any]
) -> tuple[str, str]:
    """Fill ``custom.classification_description`` + ``custom.classification`` only when currently empty."""
    cls = record.get("au_classification")
    if cls is None:
        return "not_requested", ""
    if not cls.get("choice") or not cls.get("image_gid"):
        return "not_found", f"no usable AU classification (rating={cls.get('rating')})"
    current = ((live.get("classificationDescription") or {}) or {}).get("value")
    if current:
        if current == cls["choice"]:
            return "unchanged", current.split(":")[0]
        return "skipped_existing", f"existing {current.split(':')[0]!r} kept (found {cls['rating']!r})"
    metafields = [
        {"ownerId": product_id, "namespace": METAFIELD_NAMESPACE, "key": "classification_description",
         "type": "single_line_text_field", "value": cls["choice"]},
        {"ownerId": product_id, "namespace": METAFIELD_NAMESPACE, "key": "classification",
         "type": "file_reference", "value": cls["image_gid"]},
    ]
    mf = shopify.graphql(METAFIELDS_SET, {"metafields": metafields})
    errs = (mf.get("metafieldsSet") or {}).get("userErrors") or []
    if errs:
        return "failed", f"metafieldsSet: {errs}"
    return "applied", f"{cls['rating']} ({cls.get('source')})"


def _apply_description_body(
    shopify: Any, *, product_id: str, record: Dict[str, Any], live: Dict[str, Any], force: bool
) -> Dict[str, Any]:
    if not (record.get("synopsis") or record.get("special_features")):
        return {"status": "skipped_empty", "message": "no synopsis or features drafted"}

    current = (live.get("descriptionHtml") or "").strip()
    stored_hash = ((live.get("descriptionHash") or {}) or {}).get("value")
    if current and not force and description_hash(current) != stored_hash:
        return {"status": "skipped_manual_edit", "message": "existing description was not written by this stage"}

    new_html = record["description_html"]
    if current and current == new_html.strip():
        return {"status": "unchanged", "message": "description already up to date", "hash": stored_hash}

    upd = shopify.graphql(
        PRODUCT_UPDATE,
        {
            "input": {
                "id": product_id,
                "descriptionHtml": new_html,
                "seo": {"title": live.get("title"), "description": seo_description(record.get("synopsis") or "")},
            }
        },
    )
    payload = upd.get("productUpdate") or {}
    errs = payload.get("userErrors") or []
    if errs:
        return {"status": "failed", "message": f"productUpdate: {errs}"}
    saved_html = ((payload.get("product") or {}).get("descriptionHtml") or new_html).strip()
    saved_hash = description_hash(saved_html)

    metafields = [
        {"ownerId": product_id, "namespace": METAFIELD_NAMESPACE, "key": key, "type": "single_line_text_field",
         "value": value}
        for key, value in (
            ("description_hash", saved_hash),
            ("description_source", _source_label(record)),
            ("description_features_status", record.get("features_status") or "tbc"),
        )
    ]
    mf = shopify.graphql(METAFIELDS_SET, {"metafields": metafields})
    mf_errs = (mf.get("metafieldsSet") or {}).get("userErrors") or []
    if mf_errs:
        return {"status": "failed", "message": f"description saved but metafieldsSet failed: {mf_errs}",
                "hash": saved_hash}
    return {"status": "applied", "message": "description saved", "hash": saved_hash}


def _draft_row(record: Dict[str, Any], *, status: str, model: Optional[str], error: Optional[str],
               applied: bool, description_hash_value: Optional[str]) -> Dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    row = {
        "barcode": record["barcode"],
        "catalog_item_id": record.get("catalog_item_id"),
        "shopify_product_id": record.get("shopify_product_id"),
        "product_title": record.get("product_title"),
        "distributor": record.get("distributor"),
        "source_type": record.get("source_type") or "none",
        "source_urls": record.get("source_urls") or [],
        "edition_match": record.get("edition_match"),
        "edition_match_confirmed": bool(record.get("edition_match_confirmed")),
        "synopsis_source": record.get("synopsis_source"),
        "synopsis": record.get("synopsis"),
        "edition_contents": record.get("edition_contents") or [],
        "technical_format": record.get("technical_format") or [],
        "special_features": record.get("special_features") or [],
        "features_status": record.get("features_status") or "tbc",
        "notes": record.get("notes"),
        "review_reasons": record.get("review_reasons") or [],
        "description_html": record.get("description_html"),
        "description_hash": description_hash_value,
        "status": status,
        "error": error,
        "model": model,
        "est_cost_usd": record.get("est_cost_usd"),
        "drafted_at": now,
        "updated_at": now,
    }
    cls = record.get("au_classification")
    if cls is not None:
        row["au_classification"] = cls.get("rating")
        row["au_classification_source"] = cls.get("source")
        row["au_classification_status"] = record.get("classification_status")
    if applied:
        row["applied_at"] = now
    return row


def save_draft_rows(supabase: Any, rows: List[Dict[str, Any]]) -> Optional[str]:
    """Upsert audit rows; returns an error string instead of raising (the table is optional)."""
    if not rows:
        return None
    try:
        supabase.table(DRAFTS_TABLE).upsert(rows, on_conflict="barcode").execute()
        return None
    except Exception as exc:
        if not any(col in str(exc) for col in _CLASSIFICATION_COLUMNS):
            return str(exc)
    try:
        trimmed = [{k: v for k, v in r.items() if k not in _CLASSIFICATION_COLUMNS} for r in rows]
        supabase.table(DRAFTS_TABLE).upsert(trimmed, on_conflict="barcode").execute()
        return "classification columns missing (apply migration 20260930130000); saved without them"
    except Exception as exc:
        return str(exc)


def run_description_stage(
    *,
    product_ids_by_barcode: Dict[str, Optional[str]],
    supabase: Any,
    shopify: Any,
    apply: bool,
    out_path: Path,
    tmdb: Any = None,
    model: Optional[str] = None,
    workers: Optional[int] = None,
    force: bool = False,
    classify: bool = True,
) -> Dict[str, Any]:
    """Draft descriptions (+ AU classification) for barcodes and, when ``apply``, write them to Shopify."""
    barcodes = list(product_ids_by_barcode)
    model = model or os.getenv("DESCRIPTION_MODEL") or DEFAULT_MODEL
    records = run_product_description_drafting(
        barcodes=barcodes, supabase=supabase, model=model, workers=workers, tmdb=tmdb, classify=classify
    )

    outcomes: Dict[str, int] = {}
    cls_outcomes: Dict[str, int] = {}
    rows: List[Dict[str, Any]] = []
    for record in records:
        barcode = record["barcode"]
        product_id = product_ids_by_barcode.get(barcode)
        record["shopify_product_id"] = product_id
        if record.get("error"):
            result = {"status": "failed", "message": record["error"]}
        elif not apply:
            result = {"status": "drafted", "message": "dry run"}
        elif not product_id:
            result = {"status": "failed", "message": "no Shopify product for barcode"}
        else:
            try:
                result = apply_description(shopify, product_id=product_id, record=record, force=force)
            except Exception as exc:
                result = {"status": "failed", "message": str(exc)}
        record["apply_status"] = result["status"]
        record["apply_message"] = result.get("message")
        outcomes[result["status"]] = outcomes.get(result["status"], 0) + 1
        if classify and not record.get("error"):
            cls = record.get("au_classification") or {}
            cls_status = result.get("classification_status") or ("found" if cls.get("choice") else "not_found")
            record["classification_status"] = cls_status
            record["classification_message"] = result.get("classification_message")
            cls_outcomes[cls_status] = cls_outcomes.get(cls_status, 0) + 1
        if not record.get("error"):
            rows.append(
                _draft_row(record, status=result["status"], model=model, error=None if result["status"] != "failed"
                           else result.get("message"), applied=result["status"] == "applied",
                           description_hash_value=result.get("hash"))
            )

    table_error = save_draft_rows(supabase, rows) if apply else None
    if table_error:
        log.warning("product_description_drafts not updated (apply migration 20260930120000?): %s", table_error)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(records, indent=2, ensure_ascii=False))

    drafted = [r for r in records if not r.get("error")]
    features: Dict[str, int] = {}
    for r in drafted:
        features[r["features_status"]] = features.get(r["features_status"], 0) + 1
    return {
        "records": records,
        "summary": {
            "barcodes": len(barcodes),
            "outcomes": outcomes,
            "classification": cls_outcomes,
            "features": features,
            "own_summary": sum(1 for r in drafted if r.get("synopsis_source") == "own_summary"),
            "needs_review": sum(1 for r in drafted if r.get("needs_review")),
            "cost_usd": round(sum(r.get("est_cost_usd") or 0 for r in drafted), 3),
            "table_error": table_error,
            "output": str(out_path),
        },
    }


def resolve_product_ids(shopify: Any, barcodes: List[str]) -> Dict[str, Optional[str]]:
    out: Dict[str, Optional[str]] = {}
    for barcode in barcodes:
        variant = shopify.variant_exists_by_barcode(barcode)
        out[barcode] = ((variant or {}).get("product") or {}).get("id")
    return out
