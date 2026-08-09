# Vinyl / Soundtrack — Film II Domain Exclusion

**Date:** 2026-08-09  
**Shopify II flag:** remains as currently configured (not changed by this task)  
**Production deletes:** **none performed**

## 1. Vinyl identification rule

**Authoritative:** Shopify collection handle `soundtracks`.

All soundtrack / vinyl merchandising products in the live store belong to this collection (verified via Admin GraphQL `collectionByHandle(handle: "soundtracks")`).

**Secondary (reinforcing):** Shopify metafield `custom.format`:
- `Vinyl` for 33/34 collection products (28/29 active)
- `CD` for 1 active product still in `soundtracks` (music domain — also excluded via collection)

**Not used:** `product_type` (empty for all listings), tags (empty), broad title matching (`title contains "vinyl"`).

Runtime gate (`is_vinyl_soundtrack_listing` / reason `vinyl_soundtrack`):

1. `collection_handles` contains `soundtracks`, or  
2. `media_format` ∈ {Vinyl, LP}, or  
3. `shopify_product_id` ∈ live soundtracks-collection product ID set (fetched each dual-write)

## 2. Shopify vinyl / soundtrack counts

| Metric | Count |
|--------|------:|
| Collection `soundtracks` products (all statuses) | 34 |
| **Active** soundtrack products | **29** |
| **Active** soundtrack variants | **29** |
| Active with `custom.format=Vinyl` | 28 |
| Active with `custom.format=CD` | 1 |

## 3. Existing II contamination

First Shopify II activation **did** project these into film II:

| II table | Contaminated rows |
|----------|------------------:|
| `release_shopify_listings` | **29** |
| `tape_inventory_levels` | **29** |
| `release_variants` | **29** (all `created_at` on 2026-08-09) |
| `supplier_offers` linked | **0** |
| `variant_identifiers` | 10 (barcode rows from Shopify dual-write) |

Artifacts:
- `docs/inventory-intelligence/vinyl-soundtrack-ii-contamination-audit.json`
- `docs/inventory-intelligence/vinyl-soundtrack-ii-cleanup-candidates.json`

## 4. Code changes

- `app/services/shopify_ii_product_domain.py` — domain helpers  
- `shopify_ii_dual_write_exclusion_reason` → `vinyl_soundtrack`  
- Store sync fetches `custom.format` + collection handles; dual-write also loads soundtracks collection product IDs  
- `StockAvailabilityService.search_inventory` filters music-domain candidates (film search / Ordering Agent universe)  
- Migration (additive, not applied in this task): `supabase/migrations/20260809120000_shopify_ii_product_domain_fields.sql`  
  - `shopify_listings.media_format`, `shopify_listings.collection_handles`  
  - `release_variants.product_domain` (nullable; null ⇒ film)

Shopify Admin products / inventory / prices are **not** mutated by this path.

## 5. Tests

36 passed including `tests/services/test_shopify_ii_vinyl_domain.py` (film formats pass; vinyl skipped; search excludes music domain).

## 6. Corrected film-II counts

| Metric | Value |
|--------|------:|
| Current projected records | **666** |
| Vinyl/soundtrack incorrectly included | **29** |
| Correct film `release_shopify_listings` | **637** |
| Correct film `tape_inventory_levels` | **637** |
| Film release_variants created solely from vinyl activation | **29** (all new on activation day; no supplier offers) |

## 7. Proposed production cleanup (approval required — not executed)

**Safety:** No supplier offers reference these release_variant_ids. Isolating film II is referentially safe if done in order.

**Proposed operation (after approval):**

1. Apply migration `20260809120000_shopify_ii_product_domain_fields.sql`.  
2. Deploy vinyl-exclusion code; run one store sync so future projection skips soundtracks (`vinyl_soundtrack`).  
3. Soft-tag (optional): `UPDATE release_variants SET product_domain = 'music_vinyl', format = COALESCE(format, 'Vinyl') WHERE id IN (…)`.  
4. Delete film II projections only (preserve Shopify):
   - `DELETE FROM tape_inventory_levels WHERE id IN (…29 level ids…)`  
   - `DELETE FROM release_shopify_listings WHERE id IN (…29 channel ids…)`  
   - `DELETE FROM variant_identifiers WHERE release_variant_id IN (…29…)`  
   - `DELETE FROM inventory_events WHERE release_variant_id IN (…29…)` (tape_stock_synced noise)  
   - `DELETE FROM release_variants WHERE id IN (…29…)` **only if** still unreferenced  
5. Re-count: expect listings/levels ≈ **637**.

Exact IDs: see `vinyl-soundtrack-ii-cleanup-candidates.json`.

## 8. Search / Ordering Agent impact

Film `search_inventory` excludes:
- `product_domain = music_vinyl` / format Vinyl|LP  
- releases whose Shopify listing snapshot has `soundtracks` collection or Vinyl media_format  

**Until cleanup + migration/sync populate listing domain fields**, contaminated vinyl titles can still appear in title search (they remain as published releases). Dual-write will not grow the set further once exclusion is deployed. Cleanup (or soft-tag `product_domain`) is required before Ordering Agent V1 film search is clean.

## 9. Future music / vinyl extensibility

Reuse `product_domain = music_vinyl` + collection `soundtracks` + `media_format` without undoing film II. A later Music II dual-write can project the same Shopify products into a music-scoped model. Do not force music into film `release_variants` publication paths.

---

READY FOR VINYL II CLEANUP APPROVAL
