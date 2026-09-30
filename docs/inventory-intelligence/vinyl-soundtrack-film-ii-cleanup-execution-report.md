# Vinyl Soundtrack Film-II Cleanup — Execution Report

**Date:** 2026-08-09  
**Status:** Complete

## 1. Pre-cleanup baseline

| Table | Count |
|-------|------:|
| `release_variants` | 14 849 |
| `release_shopify_listings` | 666 |
| `tape_inventory_levels` | 666 |
| `variant_identifiers` | 14 656 |
| `inventory_events` | 2 793 |
| `supplier_offers` | 22 686 |

## 2. Migration result

Applied via approved SQL (`scripts/inventory/vinyl_film_ii_cleanup.sql`):

- `shopify_listings.media_format` ✓
- `shopify_listings.collection_handles` ✓
- `release_variants.product_domain` ✓

## 3. Candidate verification

- File candidates: **29**
- Live active `soundtracks` collection variants: **29** (exact match)
- Live II rows for candidates: 29 listings / 29 levels / 29 releases
- `supplier_offers` linked: **0**
- Listing/level IDs matched audit file: **yes**

## 4. Records removed by table

| Action | Count |
|--------|------:|
| `tape_inventory_levels` deleted | 29 |
| `release_shopify_listings` deleted | 29 |
| `inventory_events` (`tape_stock_synced`) deleted | 29 |
| `release_variants` deleted | **0** (kept as `music_vinyl`) |
| `variant_identifiers` deleted | **0** (kept) |
| `supplier_offers` deleted | **0** |

Tagged: **29** `release_variants.product_domain = 'music_vinyl'` (format Vinyl).

## 5. Post-cleanup counts

| Metric | Expected | Actual |
|--------|---------:|-------:|
| Film `release_shopify_listings` | 637 | **637** |
| Film `tape_inventory_levels` | 637 | **637** |
| Vinyl Film-II listings | 0 | **0** |
| Vinyl Film-II tape levels | 0 | **0** |
| `music_vinyl` releases | 29 | **29** |
| `supplier_offers` | 22 686 | **22 686** |
| `release_variants` total | 14 849 (kept) | **14 849** |

## 6. Normal Shopify sync result

One `jobs.shopify_store_sync` on VM after cleanup — **SUCCESS**, errors **0**.

| Dual-write stat | Value |
|-----------------|------:|
| considered | 672 |
| `vinyl_soundtrack` skipped | **29** |
| gift_card / test skipped | 4 / 1 |
| channels upserted | 638 |
| tape levels upserted | 638 |
| created_new_release | 1 (legitimate film catalogue change) |
| level_fetch | 638 / 1000 (no truncation) |
| soundtracks collection products seen | 34 |

Post-sync Film II: **638** listings / **638** levels (+1 new film release vs post-cleanup 637). Vinyl Film channels/levels remain **0**.

## 7. Vinyl exclusion result

- Soundtrack products snapshotted with domain fields
- Classified / skipped as `vinyl_soundtrack` (**29**)
- Not re-projected into Film `release_shopify_listings` / `tape_inventory_levels`
- Remain as `music_vinyl` `release_variants` for future Music II

## 8. Search validation

Overlapping title queries (`Blade Runner`, `La La Land`, `Sinners`, `Twin Peaks`, `Baby Driver`) returned **film releases only**; **zero** `music_vinyl` candidates.

## 9. Shopify / supplier integrity

- Shopify product still ACTIVE (e.g. La La Land soundtrack) — unchanged
- `supplier_offers` count unchanged (22 686)
- No Film channels on music sample releases

## 10. Test results

**56 passed** (vinyl domain, dual-write exclusions, stock availability, commerce offer, inventory dual-write + behaviour).

## 11. Skipped / problem records

**None.** All 29 audited candidates cleaned and excluded.

---

FILM INVENTORY II CLEAN — READY FOR END-TO-END VALIDATION
