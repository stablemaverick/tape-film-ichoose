# TAPE Inventory Intelligence — Final End-to-End Production Validation Report

**Date:** 2026-08-10 (UTC)  
**Scope:** Read-only production validation (no Shopify mutations, no flag changes, no Ordering Agent build)  
**Evidence:** `docs/inventory-intelligence/tape-inventory-final-e2e-production-validation-results.json`  
**Runner:** `scripts/inventory/run_final_e2e_production_validation.py`  
**Plan:** `docs/inventory-intelligence/tape-inventory-final-e2e-production-validation.md`

---

## 1. Executive summary

Production Film Inventory Intelligence was validated end-to-end across six real scenarios (A–F). Stock Availability → Commerce Offer → customer-safe payloads behave as designed: Shopify price authority holds for listed titles, agent-only pricing uses `gbp_formula_v1`, supplier identity stays internal, vinyl remains excluded from Film search, and negative TAPE available is preserved.

All six scenarios **PASS**. Regression suite: **59 passed**.

---

## 2. Production health / baseline

| Metric | Count |
|---|---:|
| release_variants | 14,850 |
| release_shopify_listings (Film II channels) | 638 |
| tape_inventory_levels | 638 |
| supplier_offers (active) | 22,686 |
| supplier_offer_observations | 24,659 |
| inventory_events | 3,045 |
| shopify_listings (store sync mirror) | 682 |
| music_vinyl release_variants | 29 |
| music_vinyl still linked to Film Shopify listings | **0** |

Notes:
- `shopify_listings` (682) > Film II channels (638) is expected: gift cards, test product, and vinyl soundtracks are excluded from Film projection.
- Most release_variants have `product_domain` null (treated as film); only `music_vinyl` is explicitly tagged (29).
- Dual-write error count on latest sync path: **0** (from prior activation/cleanup); this validation run was read-only.

---

## 3. Case A — In Stock — **PASS**

| Field | Value |
|---|---|
| Title | True Romance - Blu-Ray |
| release_variant_id | `dc771d96-c524-4a64-9d42-8f3c4a51526c` |
| Shopify variant | `gid://shopify/ProductVariant/46318163493088` |
| Shopify retail | A$28.99 |
| on_hand / committed / available | 1 / 0 / 1 |
| Supplier positions | none |
| StockAvailability | tape `available`, supplier_available=false |
| Commerce internal | `in_stock`, pricing_source=`shopify` |
| Public | `availability=in_stock`, `price=28.99` |

Customer behaviour: **In Stock — Shopify price**  
Assertions: price source Shopify; public has no supplier fields — **PASS**

---

## 4. Case B — Available from Supplier — **PASS**

| Field | Value |
|---|---|
| Title | An American Werewolf in London 4K UHD |
| release_variant_id | `dfa0e286-ba8c-407f-a039-313cf8827b86` |
| Barcode | 5027035024776 |
| Shopify variant | `gid://shopify/ProductVariant/46286847377632` |
| TAPE | on_hand=0, committed=0, available=0 (`sold_out`) |
| Moovies | available, fresh, unit_cost=15.35 |
| Lasgo | available, fresh, unit_cost=16.69 |
| Preferred | moovies (lower cost among fresh available) |
| Shopify price | A$42.99 |
| Public | `available_from_supplier`, `price=42.99` |

Critical assertions: no Lasgo/Moovies/SKU/cost/qty in public; no price recalculation — **PASS**

---

## 5. Case C — Out of Stock — **PASS**

| Field | Value |
|---|---|
| Title | Breathless - 4K (UHD) |
| release_variant_id | `b0a65507-2ebb-4915-a0e6-20ecc9154d51` |
| TAPE | on_hand=1, committed=1, available=0 |
| Suppliers | none (`NO_SUPPLIER_OFFERS`) |
| Public | `out_of_stock`, price A$34.99 (Shopify) |

Missing supplier data is **not** treated as supplier availability — **PASS**

---

## 6. Case D — Oversold / preorder — **PASS**

| Field | Value |
|---|---|
| Title | The Addiction Limited Edition 4K Ultra HD |
| release_variant_id | `87df4755-43c4-4073-aaa8-5141554e3918` |
| on_hand / committed / available | 0 / 1 / **-1** |
| tape status | `oversold` |
| Suppliers | moovies + lasgo, both fresh available |
| Preferred | lasgo (lower cost 9.55 vs 16.81) |
| Commerce customer_status | `available_from_supplier` |
| Public price | A$46.79 (Shopify) |

Confirmed: available remains **-1** (not clamped); `available = on_hand - committed`.

**Deterministic precedence observed (unchanged):**
1. Shopify preorder flag → `preorder` (not present on this title)
2. Else `tape.available > 0` → `in_stock`
3. Else fresh eligible supplier (+ margin gate) → `available_from_supplier`
4. Else → `out_of_stock`

Oversold TAPE with fresh supplier therefore surfaces as **Available from Supplier** while canonical TAPE available stays negative — **PASS**

---

## 7. Case E — Multiple suppliers — **PASS**

Same production release as Case B (natural Lasgo+Moovies sample):

| Supplier | availability | qty | cost | freshness |
|---|---|---|---:|---|
| moovies | available | null (boolean_only) | 15.35 | fresh |
| lasgo | available | null (boolean_only) | 16.69 | fresh |

Preferred: **moovies** (fresh available → lowest unit_cost).  
Suppliers remain independent; no combined quantity field.  
Public: `Available from Supplier — A$42.99` with no supplier count/names/qty/cost — **PASS**

---

## 8. Case F — Agent-only — **PASS**

| Field | Value |
|---|---|
| Title | Creepozoids Blu-Ray |
| release_variant_id | `ef3f91f3-acf0-444c-864c-22845300382f` |
| Barcode | 5037899091425 |
| Shopify listing | none |
| TAPE inventory | absent (`unknown`, warning `TAPE_INVENTORY_ABSENT`) |
| Suppliers | moovies 13.26 + lasgo 13.35 (both fresh) |
| Preferred | moovies |
| Pricing | `supplier_pricing_policy` / `gbp_formula_v1` → A$43.99 |
| listing_type | `agent_only` |
| Public | `available_to_order`, `price=43.99` |

No Shopify product required/created; supplier identity/cost/qty hidden publicly — **PASS**

---

## 9. Customer-safe payload / privacy

Recursive inspection of public payloads for all six cases:

`PUBLIC SUPPLIER PRIVACY: PASS`

---

## 10. Price authority

`SHOPIFY PRICE AUTHORITY: PASS` — Cases A–E public retail price equals Shopify listing price.  
`AGENT-ONLY PRICING: PASS` — Case F uses deterministic TAPE pricing policy output (A$43.99).

---

## 11. Inventory arithmetic

Five Film Shopify releases spot-checked: all satisfy `available = on_hand - committed`, including zero-available and committed>0. Case D separately proves negative available.

Shopify `inventory_quantity` aligns with available for sampled in-stock/sold-out titles (Shopify mirror does not expose committed separately).

---

## 12. Freshness

Supplier-backed cases B/D/E/F: `feed_freshness=fresh`, `is_stale=false`, recent `observed_at`.  
No natural stale production sample in this run → stale behaviour covered by automated tests (`test_stock_availability_service` / freshness mapping).

---

## 13. Film search + vinyl exclusion — **PASS**

| Query | Result |
|---|---|
| `True` (Shopify film neighbourhood) | film candidates returned |
| `Creepozoids Blu-Ray` (Case F) | Case F found (`contains_case_f=true`) |
| `blade runner` (overlap neighbourhood) | film candidates only; `music_vinyl_excluded=true` |

---

## 14. Failure behaviour — **PASS**

| Input | Result |
|---|---|
| unknown barcode | structured `RELEASE_NOT_FOUND` |
| unknown release_variant_id | structured `RELEASE_NOT_FOUND` |
| release with no TAPE inventory | returns stock with tape `present=false` / status `unknown` (no hallucinated qty) |
| missing supplier inventory | Case C → `out_of_stock`, not synthetic supplier availability |

No fuzzy fallback on exact lookup.

---

## 15. History

History capability is **implemented** (not deferred): Case A showed `tape_stock_synced` event. Observation history for that release was empty in the sample window; supplier observation stream exists globally (24,659 rows).

---

## 16. Performance (approx., production reads)

| Path | n | min | median | max (ms) |
|---|---:|---:|---:|---:|
| Stock by release_variant_id | 6 | 177 | 203 | 227 |
| Stock by barcode | 4 | 203 | 240 | 286 |
| Commerce Offer (Shopify) | 5 | 243 | 275 | 316 |
| Commerce Offer (agent-only) | 1 | 208 | 208 | 208 |
| search → commerce | 1 | 446 | 446 | 446 |

No stress test performed.

---

## 17. Tests

```text
59 passed
```

Covered: Stock Availability, Commerce Offer, Shopify II exclusions, vinyl/product domain, supplier dual-write / projection, privacy/price-authority/no-combined-quantity behaviours in unit suite.

---

## 18. Final customer-state matrix

| Scenario | Release | TAPE available | Supplier state | Shopify? | Price source | Customer state | Result |
|---|---|---:|---|---|---|---|---|
| In Stock | True Romance - Blu-Ray | 1 | none | Yes | Shopify | in_stock | PASS |
| Supplier | An American Werewolf in London 4K UHD | 0 | available (×2) | Yes | Shopify | available_from_supplier | PASS |
| OOS | Breathless - 4K (UHD) | 0 | none | Yes | Shopify | out_of_stock | PASS |
| Oversold | The Addiction LE 4K | -1 | available (×2) | Yes | Shopify | available_from_supplier | PASS |
| Multi-supplier | An American Werewolf in London 4K UHD | 0 | available (×2) | Yes | Shopify | available_from_supplier | PASS |
| Agent-only | Creepozoids Blu-Ray | n/a | available | No | TAPE pricing | available_to_order | PASS |

---

## 19. Remaining risks (non-blocking)

- Many release_variants still have `product_domain` null rather than explicit `film` (behaviourally fine today; music exclusion relies on `music_vinyl` tag + soundtrack listing signals).
- Supplier quantities are often null (boolean availability); Ordering Agent must not invent exact supplier qty.
- Case D shows oversold TAPE can still be sellable via supplier path — intentional under current precedence; agent UX should explain fulfilment source carefully.
- PO Inventory II remains OFF; incoming PO quantities are not part of this foundation claim.
- No natural stale supplier sample in this production slice (covered by tests).

---

## 20. Recommendation

Stop Film Inventory Intelligence infrastructure expansion. Ordering Agent V1 should consume:

```text
search_inventory → StockAvailabilityService → CommerceOfferService → customer-safe response
```

LLM: language/intent/clarification only. Deterministic services: identity, inventory, supplier selection, freshness, pricing, privacy.

---

INVENTORY FOUNDATION COMPLETE — READY FOR ORDERING AGENT V1
