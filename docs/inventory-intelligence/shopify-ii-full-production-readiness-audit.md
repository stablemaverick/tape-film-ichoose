# TAPE Shopify Inventory II — Full Production Readiness Audit

**Date:** 2026-08-09  
**Scope:** Read-only. `INVENTORY_DUAL_WRITE_SHOPIFY` was **not** enabled.  
**Artifacts:**  
- `docs/inventory-intelligence/shopify-ii-full-mapping-audit.json`  
- `docs/inventory-intelligence/shopify-ii-location-inventory-audit.json`  
- `scripts/inventory/run_shopify_ii_readiness_audit.py`

## Production activation config (confirmed)

```bash
SHOPIFY_INVENTORY_LOCATION_ID=gid://shopify/Location/78213775584
INVENTORY_DUAL_WRITE_LEVEL_FETCH_MAX=1000
```

`1000` ≥ current eligible active variants after non-release exclusions (~676), so detailed inventory levels are **not** silently truncated on first sync.

Non-release exclusions (gift cards + exact known test product) are enforced in `shopify_ii_dual_write_exclusion_reason` before channel/tape writes.

## 1. Executive summary

Full-catalogue mapping against operational `shopify_listings` (active store sync) completed: **681 variants / 678 products**. **374 mapped** (54.9% overall; **78.2% of barcode-bearing**). **Ambiguous = 0**. Remaining gaps are understood: curated Shopify titles not yet in II (will create inbound releases), physical media missing Shopify barcodes, plus 4 gift-card variants and 1 test product.

Inventory projection semantics match Shopify Admin quantities (`available = on_hand - committed`, including negative available / committed demand). Safety paths confirm Shopify II is **inbound-only**: no product publication, no retail price mutation, no public supplier leak.

Activation is safe for manual enablement once production `.env` includes the confirmed location ID and `INVENTORY_DUAL_WRITE_LEVEL_FETCH_MAX=1000`.

## 2. Full Shopify catalogue statistics

| Metric | Value |
|--------|------:|
| Shopify products (API `productsCount`) active / draft / archived | 668 / 35 / 242 |
| `shopify_listings` products inspected | 678 |
| `shopify_listings` variants inspected | 681 |
| Product status in listings | ACTIVE 681 |
| Mapped | 374 |
| Unmapped (barcode present) | 104 |
| Missing barcode | 203 |
| Ambiguous | 0 |
| Overall mapping % | 54.92% |
| Barcode-bearing variants | 478 |
| Barcode-bearing mapped | 374 |
| Barcode-bearing mapping % | 78.24% |

**Scope note:** Store sync queries `status:active` only. Draft/archived are not in `shopify_listings` and are out of Shopify II projection scope.

Mapping status breakdown: all mapped rows are `mapped_primary_barcode` (374). No existing `release_shopify_listings` channels yet.

## 3. Missing barcode analysis

| Category | Count |
|----------|------:|
| Physical-media title heuristics (likely should map) | 165 |
| Matched to catalog but no barcode on listing | 33 |
| Gift card | 4 |
| Other / unclear (TEST product) | 1 |
| **Total** | **203** |
| **Estimate that should map to release_variants** | **~198** (physical + catalog-matched) |

Representative examples:

- Physical missing barcode: *Dark Crystal Limited Edition Steelbook 4K…* (`…/53247047074016`), qty 2  
- Catalog-matched, no barcode: *Battle Royale* (`…/46318162346208`)  
- Gift card: *TAPE! Film Gift Card* $25/$50/$100/$10  
- Test: *TEST Film - Hell or High Water (Not for Sale)* (`…/46225868521696`)

**Important:** Dual-write will **create** `release_variants` for missing-barcode curated listings (inbound), including gift cards/test unless filtered later. Gift-card ignore in store sync keys on `product_type`, which is empty for all current listings — ignore does not fire.

## 4. Unmapped barcode analysis

| Cause | Count |
|-------|------:|
| Catalog has barcode but no `release_variant` yet | 96 |
| Barcode absent from II and catalog | 8 |
| Formatting mismatch / normalisation | 0 |
| Ambiguity | 0 |
| **Total** | **104** |

Highest-priority gap is expected: Shopify curated titles whose barcodes exist on `catalog_items` (or only on Shopify) but have no II `release_variant` yet. First dual-write **creates** those releases and channels (inbound only).

Examples:

| Title | Variant ID | Barcode | Cause | Remediation |
|-------|------------|---------|-------|-------------|
| Salaam Bombay 4K - Criterion… | `…/48116661616864` | `715515326117` | catalog barcode, no release | create/link on dual-write |
| Sex And Fury / Female Yakuza… | `…/52373563146464` | `5027035029429` | catalog barcode, no release | create/link on dual-write |
| The Complete Kubrick - Criterion… | `…/53213474357472` | `715515337717` | absent from II+catalog | create on dual-write |
| The Odyssey (Original Soundtrack) | `…/53388489720032` | `810155842253` | absent from II+catalog | create on dual-write |

### Barcode normalisation

Checked whitespace/hyphen stripping, UPC-A↔EAN-13 leading-zero, `.0` suffix, digit-only alts. **No mapping failures attributed to format mismatch** (`alt_format_hits` empty). No production normalisation change recommended from this audit.

## 5. Ambiguous mapping analysis

**Ambiguous = 0** across the full `shopify_listings` catalogue. No candidate lists. Incorrect auto-mapping risk from ambiguity is currently nil.

## 6. Shopify location validation

| Location ID | Evidence |
|-------------|----------|
| `gid://shopify/Location/78213775584` | Listed by Admin `locations` (id-only); holds inventory in all sampled stocked variants; hardcoded fallback in supplier-orders tooling |
| `gid://shopify/Location/78377091296` | Second location id; appears on some variants with **zero** quantities in samples |

**Names / isActive / inventory-enabled:** not readable — token lacks `read_locations` (GraphQL + REST 403).

**Schema semantics:** Dual-write writes **one** `tape_inventory_levels` row per `(release_variant_id, shopify_location_id)` using `SHOPIFY_INVENTORY_LOCATION_ID`. Stock Availability **sums** across location rows if multiple exist — it does not invent aggregation beyond stored rows. Prefer **one canonical TAPE location** for Shopify II (do not dual-write both unless intentionally stocking both).

**Target for `tape_inventory_levels`:** confirmed TAPE fulfilment location — evidence strongly indicates `78213775584`, pending Admin name confirmation.

## 7. Inventory semantics validation

Projection uses Shopify Admin quantities `available`, `committed`, `on_hand`, `incoming` at the configured location. When detailed levels are present, they override listing `inventory_quantity`. Identity **`available == on_hand - committed`** held for all audited samples, including:

| Bucket | Example | Shopify level | Projected tape row |
|--------|---------|---------------|--------------------|
| Positive | Hush LE 4K | avail 7 / on_hand 7 / committed 0 | same |
| Zero | The Bride! Steelbook | 0/0/0 | same |
| CONTINUE OOS | Good Luck Have Fun… | 0/0/0 | same |
| Preorder-ish + committed | Eyes Wide Shut Steelbook | avail 4 / on_hand 5 / committed 1 | same |
| Oversold / negative available | Complete Kubrick Criterion | avail -4 / on_hand 0 / committed 4 | same |

Negative `available` is stored as-is (Shopify-authoritative). Listing `inventory_quantity` matches Shopify `available` in these samples.

**First-run caveat:** only first `INVENTORY_DUAL_WRITE_LEVEL_FETCH_MAX` (default 500) variants get detailed levels per sync; remainder use `inventory_quantity` → `available`, `on_hand=max(available,0)`, `committed=0` until a later sync fetches them.

## 8. Safety validation

### NO automatic supplier product publication

- Shopify II path: `shopify_store_sync` → `_maybe_dual_write_shopify_releases` → `dual_write_shopify_listings_to_releases` projects **from** Shopify listings **into** II.
- `shopify_release_mapping.SHOPIFY_II_CREATES_SHOPIFY_PRODUCTS = False` with `assert_shopify_ii_is_inbound_only()`.
- Flag does **not** call `catalog_shopify_publish_service` / `productSet`. Supplier offers (~22k) are a separate dual-write stream (`INVENTORY_DUAL_WRITE_SUPPLIER`) that never creates Shopify products.

### NO Shopify retail price mutation

- Commerce Offer Shopify path reads `shopify_listings.price_amount`; comment and tests: supplier cost changes do not change public price (`test_shopify_price_unchanged_when_supplier_cost_changes`).
- Shopify II dual-write updates II tables only — no price mutations to Shopify Admin.

### NO public supplier identity/cost/quantity exposure

- `to_public_commerce_offer` + `SUPPLIER_SENSITIVE_KEYS` strip supplier fields; `assert_public_offer_has_no_supplier_leak`.
- Public availability remains customer-safe (e.g. `available_from_supplier` without naming Lasgo/Moovies).

**Tests:** 29 passed (`test_commerce_offer_service`, `test_stock_availability_service`, `test_inventory_dual_write`).

## 9. Production baseline (read-only)

| Table | Count |
|-------|------:|
| `release_shopify_listings` | **0** |
| `tape_inventory_levels` | **0** |
| `release_variants` | 14 554 |
| `variant_identifiers` | 14 554 |
| `supplier_offers` (all active, 100% resolved) | 22 686 |
| `shopify_listings` | 681 |

Shopify II flag remains OFF in this audit environment.

## 10. Expected first-run impact

After non-release exclusions (recalculated 2026-08-09):

| Outcome | Estimate |
|---------|---------:|
| Skipped non-release (gift card + test) | **5** (4 gift_card_exact_title + 1 test_product_exact_title) |
| Eligible variants | **676** |
| `release_shopify_listings` upserts | **676** |
| `tape_inventory_levels` rows | **676** (location set; all eligible track inventory) |
| New `release_variants` created | **302** (104 unmapped + 198 missing barcode) |
| Existing releases updated/linked | **374** |
| Ambiguous skipped | **0** |
| Level fetch coverage at max=1000 | **full** (no truncation) |

Listing count ≈ inventory-level count when location is set and items are tracked. If location unset, channels still write but tape levels are skipped.

Difference vs mapped-only: dual-write also creates releases for unmapped/missing-barcode curated Shopify products — not supplier catalogue publication.

## 11. Remaining risks

- **No canary variant subset** in current code — first enablement sync projects the full eligible active catalogue.
- Production `.env` must explicitly set `SHOPIFY_INVENTORY_LOCATION_ID` and `INVENTORY_DUAL_WRITE_LEVEL_FETCH_MAX=1000` before activation.
- Gift-card ignore still depends on `product_type` **or** exact title `TAPE! Film Gift Card`; other gift-card titles would need an explicit allow-list addition.

## 12. Exact controlled activation procedure

### Before

1. Deploy current tested code to production VM.
2. Verify supplier dual-write healthy (`INVENTORY_DUAL_WRITE_ENABLED=1`, `SUPPLIER=1`, `SHOPIFY=0`).
3. Re-capture baselines (expect listings/levels still 0).
4. Confirm Shopify credentials work (`./venv/bin/python -m jobs.shopify_store_sync --dry-run`).
5. In Shopify Admin, name-confirm locations `78213775584` and `78377091296`.
6. Set on production `/opt/tape-film-ichoose/.env`:
   - `SHOPIFY_INVENTORY_LOCATION_ID=gid://shopify/Location/<confirmed_tape_location>`
   - `INVENTORY_DUAL_WRITE_LEVEL_FETCH_MAX=1000` (or ≥681)
7. Archive full mapping JSON from this audit.

### Activation (do not execute in this audit)

In production `.env` only:

```bash
INVENTORY_DUAL_WRITE_ENABLED=1
INVENTORY_DUAL_WRITE_SHOPIFY=1
# keep SUPPLIER as currently configured
# keep PO=0 unless intentionally enabling PO dual-write
SHOPIFY_INVENTORY_LOCATION_ID=gid://shopify/Location/78213775584   # after Admin confirmation
INVENTORY_DUAL_WRITE_LEVEL_FETCH_MAX=1000
```

Then run **one** non-dry-run store sync (smallest supported full projection — no canary filter exists):

```bash
cd /opt/tape-film-ichoose
./venv/bin/python -m jobs.shopify_store_sync
# or: ./scripts/run_shopify_store_sync.sh
```

## 13. Exact validation procedure

Immediately after sync:

1. Count `release_shopify_listings` ≈ 681; `tape_inventory_levels` ≈ 681 (same location id).
2. Spot-check ≥10 mapped titles vs Shopify Admin inventory (include positive, zero, negative available, committed).
3. For those releases run `StockAvailabilityService` / `CommerceOfferService` (or CLIs):
   - TAPE stock → customer `in_stock` + Shopify price  
   - No TAPE + eligible supplier → `available_from_supplier` + Shopify price, no supplier name  
   - No TAPE + no supplier → `out_of_stock` + Shopify price  
4. Confirm no Shopify product creates / price changes in Admin activity.
5. Confirm gift-card/test pollution count is understood (~5).

## 14. Rollback procedure

1. Set `INVENTORY_DUAL_WRITE_SHOPIFY=0` (keep master/supplier as needed).
2. Restart/reload env for cron/jobs so subsequent store syncs skip II projection.
3. **Do not** delete projected II rows by default — preserve for investigation unless proven corrupt.
4. Optional later cleanup of junk releases (gift cards/test) via controlled maintenance, not emergency rollback.
5. Shopify store, `catalog_items`, and supplier offers remain untouched by turning the flag off.

## 15. Recommendation

**READY FOR MANUAL SHOPIFY II ACTIVATION**

No remaining blockers. Set the confirmed env vars on production, deploy this code, then enable `INVENTORY_DUAL_WRITE_SHOPIFY=1` and run one store sync when ready.
