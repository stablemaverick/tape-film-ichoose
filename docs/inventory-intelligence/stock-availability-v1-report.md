# Stock Availability V1 — Implementation Report

## 1. Executive summary

Stock Availability V1 is implemented as a **canonical deterministic read layer** over Inventory Intelligence tables. It answers release identity, TAPE stock (when present), per-supplier positions, freshness, and preferred supplier **without combining TAPE and supplier quantities**.

Production validation confirms supplier-side reads work against live `supplier_offers` (~22k). **`tape_inventory_levels` and `release_shopify_listings` remain empty** (Shopify II still OFF), so TAPE inventory correctly returns `status=unknown` with warning `TAPE_INVENTORY_ABSENT`.

## 2. Architecture

```text
identifier (release_variant_id | barcode | shopify_variant_id | supplier_id+sku)
    → StockAvailabilityService.resolve_*
    → release_variants (+ optional catalog_items for release date/preorder)
    → tape_inventory_levels (TAPE domain; may be absent)
    → supplier_offers (current projection; observations for history)
    → preferred supplier ranking (deterministic)
    → JSON contract (no combined quantity)
```

Tools: `get_inventory`, `search_inventory`, `get_inventory_history`  
API: `GET/POST /api/stock-availability` (auth via `PIPELINE_HEALTH_KEY`)  
CLI: `scripts/inventory/get_stock_availability.py`

## 3. Repository changes

| Path | Role |
|------|------|
| `app/services/stock_availability_service.py` | Canonical service + tools |
| `scripts/inventory/get_stock_availability.py` | CLI entry |
| `app/routes/api.stock-availability.ts` | Internal Remix API |
| `tests/services/test_stock_availability_service.py` | Unit tests (11) |
| This report | Deliverable |

No changes to supplier import, retention, catalog stock bulk write, or dual-write flags.

## 4. Database changes

**None.** Additive indexes already exist from II foundation. No migration required for V1 reads.

## 5. API/tool contract

### CLI

```bash
./venv/bin/python scripts/inventory/get_stock_availability.py get --barcode 5053083269203
./venv/bin/python scripts/inventory/get_stock_availability.py get --release-variant-id <uuid>
./venv/bin/python scripts/inventory/get_stock_availability.py get --supplier-id lasgo --supplier-sku 'barcode:5053083269203'
./venv/bin/python scripts/inventory/get_stock_availability.py search "Speak No Evil"
./venv/bin/python scripts/inventory/get_stock_availability.py history --release-variant-id <uuid>
```

### HTTP

`/api/stock-availability?key=$PIPELINE_HEALTH_KEY&barcode=...`  
Ops: `op=get|search|history`. Default includes costs; `include_costs=0` to hide.

### Errors

`INVALID_IDENTIFIER`, `RELEASE_NOT_FOUND`, `AMBIGUOUS_IDENTIFIER`, `SHOPIFY_LISTING_NOT_FOUND`, `SUPPLIER_MAPPING_UNRESOLVED`

## 6. Inventory semantics

| Domain | Semantics |
|--------|-----------|
| TAPE | `available = on_hand - committed` (negative allowed → `oversold`). Never clamped. |
| Supplier API status | `available` / `unavailable` / `unknown` / `stale` (+ optional `last_known_availability_status`) |
| Quantity type | `exact` / `capped` / `boolean_only` / `unknown` |
| Freshness | `derive_feed_freshness` + per-supplier env overrides (`INVENTORY_FRESHNESS_*_HOURS`) |
| Preferred supplier | fresh available → has SKU → lowest cost → supplier rank → id |
| Summary | `tape_available` + `supplier_available` boolean — **never** a summed total |

Moovies/Lasgo II identities today are largely `barcode:{ean}` (Lasgo has no native SKU).

## 7. Shopify readiness

```text
SHOPIFY INVENTORY II: NOT READY
```

- `tape_inventory_levels` = **0**
- `release_shopify_listings` = **0**
- Dual-write path exists (`shopify_release_dual_write_service`) but flags remain OFF
- **Do not enable** in this phase
- Activation later requires: master + `INVENTORY_DUAL_WRITE_SHOPIFY=1`, `SHOPIFY_INVENTORY_LOCATION_ID`, controlled store sync, then retest TAPE block

## 8. Validation results

| Check | Result |
|-------|--------|
| Unit tests | **11 passed** |
| Live get by `release_variant_id` | OK (~240ms) — dual supplier Speak No Evil example |
| Live get by barcode `5053083269203` | OK |
| Live get by `lasgo` + `barcode:5053083269203` | OK — preferred Lasgo |
| Live search “Speak No Evil” | OK |
| Live history | OK (observations/events) |
| Doc sample SKUs (FCD2923, etc.) | **Not present** in prod catalog/offers (as doc allowed) |
| TAPE inventory | Correctly absent / unknown |
| Downstream baselines | Untouched by this work |

Example live summary: tape unknown; Lasgo available qty 7 @ £11.75 preferred; Moovies unavailable qty 0 — **not** combined to 7.

## 9. Tests

Coverage: combined-qty regression, API status mapping, quantity types, tape oversold, preferred ranking, ambiguous/missing barcode, supplier-only response, invalid multi-id.

## 10. Performance

| Lookup | Observed |
|--------|----------|
| release_variant_id | ~240ms (few PostgREST round-trips) |
| barcode | similar |
| Fixed query count | resolve + release + tape + offers + suppliers registry |

No caching added. N+1 avoided for offers (single query per release).

## 11. Risks / unresolved mappings

- Shopify/TAPE levels empty until II Shopify enablement
- Native supplier catalogue SKUs (FCD*, OPTU*, etc.) often **not** stored as `supplier_offers.supplier_sku` (barcode-keyed) — resolve via barcode or `barcode:{ean}`
- `edition` always null until editions model exists
- Title search is `ilike` on `release_variants.title` only (no fuzzy merge)

## 12. Production rollout

1. Deploy app code (service + CLI + API route) — **no migration**
2. Ensure `PIPELINE_HEALTH_KEY` set for API auth
3. Smoke: CLI `get --barcode <known>`
4. Keep flags: Supplier ON; Shopify OFF; PO OFF
5. Optional later: Shopify II activation plan (separate approval)

## 13. Recommendation

```text
READY WITH PREREQUISITES
```

**Why:** Supplier inventory truth layer and agent-ready tools work in production for II-linked releases. Full “TAPE + supplier” answers need **Shopify Inventory II population** (`tape_inventory_levels`) before Inventory Agent V1 can answer on-hand/committed/available for store stock. Do not enable Shopify II without a controlled rollout.
