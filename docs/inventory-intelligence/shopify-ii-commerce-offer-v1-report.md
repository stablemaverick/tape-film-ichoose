# Shopify Inventory II + Commerce Offer V1 — Report

## 1. Executive summary

This phase adds the **commerce boundary** between TAPE’s curated Shopify catalogue and the wider supplier universe, plus readiness fixes for Shopify → Inventory Intelligence projection.

- **CommerceOfferService** returns Shopify-listed vs agent-only offers with a **customer-safe** public contract (no supplier identity/cost/qty).
- **Shopify retail price is authoritative** for listed products; supplier cost never mutates it (margin gate may mark supplier fulfilment unavailable).
- **Agent-only** offers use existing `pricing_rules.calculate_sale_price` (`gbp_formula_v1`).
- Shopify II mapping improved (catalog item + barcode identifiers; skip ambiguous); **production Shopify II flag was not enabled**.
- Tables remain empty until a controlled activation: `release_shopify_listings=0`, `tape_inventory_levels=0`.

## 2. Shopify II readiness

| Item | Status |
|------|--------|
| Dual-write code path | Ready (`shopify_store_sync` → `_maybe_dual_write_shopify_releases`) |
| Flags required | `INVENTORY_DUAL_WRITE_ENABLED=1` **and** `INVENTORY_DUAL_WRITE_SHOPIFY=1` |
| Location | `SHOPIFY_INVENTORY_LOCATION_ID` required for tape levels |
| Mapping | Deterministic: existing listing → catalog_item_id → primary_barcode / variant_identifiers; **ambiguous skipped** |
| Creates Shopify products? | **No** (regression asserted) |
| Production flag | **Still OFF** |

## 3. Why `release_shopify_listings` and `tape_inventory_levels` are empty

Simply because **Shopify dual-write is gated OFF**. Operational `shopify_listings` continues to sync (barcode, price, inventory_quantity). II projection returns immediately when `flags.shopify_enabled` is false — zero inserts into the two II tables.

## 4. Shopify mapping coverage (dry-run, 500 listings)

| Metric | Count |
|--------|------:|
| Inspected | 500 |
| Mapped (primary barcode) | 298 |
| Unmapped (barcode present, no release) | 64 |
| Ambiguous | 0 |
| Missing barcode | 138 |

Script: `scripts/inventory/run_shopify_mapping_dry_run.py`  
On activation, unmapped curated variants will create new `release_variants` (inbound II only); missing-barcode variants need data repair in Shopify.

## 5. Supplier release resolution coverage

| Metric | Count |
|--------|------:|
| Active supplier_offers | 22 686 |
| Resolved to release_variant | 22 686 |
| Unresolved | 0 |
| Moovies | 13 559 / 13 559 (100% `barcode:` SKU shape in sample) |
| Lasgo | 9 127 / 9 127 (100% `barcode:` SKU shape in sample) |

Ordering Agent can safely sell resolved releases; native catalogue codes (FCD*, etc.) still need barcode resolution.

## 6. Commerce architecture

```text
release
→ Stock Availability
→ listing exists in release_shopify_listings?
   → YES: Shopify price (shopify_listings) + customer status from TAPE/supplier eligibility
   → NO:  agent-only offer via calculate_sale_price(preferred supplier cost)
→ public serializer (customer-safe)
```

## 7. Pricing authority

```text
Shopify listing → Shopify retail price
Agent-only release → deterministic TAPE pricing engine (pricing_rules.gbp_formula_v1)
```

Pipeline: GBP cost → × `GBP_AUD_RATE` (default 2.0) → × `LANDED_COST_MARKUP` (1.12) → × margin tier → × GST 1.10 → round up to .99.

Margin gate (`COMMERCE_MIN_MARGIN_RATIO`, default 0.12): if Shopify retail cannot cover landed cost, status becomes `unavailable_for_supplier_order` — **price unchanged**.

## 8. Customer-safe availability contract

Public fields only: `release_variant_id`, `title`, `listing_type`, `sellable`, `availability`, `price`, `currency`.

Statuses include: `in_stock`, `available_from_supplier`, `available_to_order`, `preorder`, `out_of_stock`, `unavailable`, `unavailable_for_supplier_order`.

## 9. Supplier privacy controls

- `to_public_commerce_offer` + `assert_public_offer_has_no_supplier_leak`
- Tests forbid Lasgo/Moovies/supplier_id/sku/cost/qty in public payload
- Internal block retains fulfilment details for ops/agent tools

## 10. Tests

`tests/services/test_commerce_offer_service.py` + stock availability suite: **19 passed** including:

- Shopify II inbound-only / no productCreate
- dual-write noop when Shopify flag off
- public serializer privacy
- Shopify price unchanged when supplier cost changes
- margin gate
- mapping ambiguous / summary
- agent-only pricing path

## 11. Performance

| Path | Notes |
|------|--------|
| Supplier coverage counts | seconds (exact counts) |
| Mapping dry-run 500 | ~60s (many PostgREST round-trips; OK for offline report) |
| Commerce offer (agent-only barcode) | uses Stock Availability (~200–400ms) |

No caching added.

## 12. Risks

- 27%+ of sampled Shopify variants lack barcode → mapping weak until Shopify data cleaned
- Unmapped barcodes will create new releases on first dual-write (acceptable for curated catalogue; monitor duplicates)
- Agent-only prices use default FX unless `GBP_AUD_RATE` / `LANDED_COST_MARKUP` set in env
- TAPE inventory remains unknown until Shopify II activation

## 13. Controlled Shopify II rollout plan

```text
deploy code
→ run mapping dry-run (full shopify_listings)
→ review mapped / unmapped / missing_barcode / ambiguous
→ verify SHOPIFY_INVENTORY_LOCATION_ID
→ manually approve: INVENTORY_DUAL_WRITE_SHOPIFY=1 (master already ON for supplier)
→ run ONE shopify_store_sync (non dry-run)
→ validate release_shopify_listings > 0, tape_inventory_levels > 0
→ spot-check Shopify Admin quantities vs tape_inventory_levels
→ Stock Availability: tape.present true for samples
→ Commerce Offer: listing_type=shopify, price == Shopify retail
→ keep PO flag OFF; never auto-publish supplier catalogue
```

**Rollback:** set `INVENTORY_DUAL_WRITE_SHOPIFY=0`. Dual-write stops. Operational `shopify_listings` unaffected. Optional: leave II rows in place (additive) or delete channel/level rows by shop in a controlled SQL window.

## 14. Recommendation

```text
READY WITH PREREQUISITES
```

**Why:** Commerce architecture, privacy, pricing authority, mapping safety, and tests are in place. Activate Shopify II only after reviewing a full mapping dry-run and confirming location ID — **do not enable the flag automatically**.

---

### Files added/updated

| Path | Role |
|------|------|
| `app/services/commerce_offer_service.py` | Commerce offers + public serializer |
| `app/services/shopify_release_mapping.py` | Deterministic mapping classify |
| `app/services/shopify_release_dual_write_service.py` | Safer resolve; skip ambiguous |
| `scripts/inventory/get_commerce_offer.py` | CLI |
| `scripts/inventory/run_shopify_mapping_dry_run.py` | Mapping coverage |
| `scripts/inventory/run_supplier_resolution_coverage.py` | Supplier coverage |
| `tests/services/test_commerce_offer_service.py` | Tests |
| This report | Deliverable |
