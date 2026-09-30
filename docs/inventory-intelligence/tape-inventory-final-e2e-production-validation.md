# TAPE Inventory Intelligence — Final End-to-End Production Validation

The Film Inventory Intelligence foundation is now clean and ready for final validation.

Current production state:

- Supplier Inventory II: ON
- Shopify Inventory II: ON
- PO Inventory II: OFF
- supplier_offers: 22,686
- Shopify Film channels/levels after latest sync: 638
- music_vinyl releases: 29
- vinyl Film-II listings/levels: 0
- Shopify inventory fetch: 638 / max 1000
- latest Shopify II errors: 0
- latest relevant tests: 56 passed

Vinyl soundtracks are explicitly represented as `product_domain = music_vinyl` and excluded from Film Inventory Intelligence/search.

We now need to perform the final read-only end-to-end production validation before declaring the Film Inventory Intelligence foundation complete and beginning Ordering Agent V1.

Do not introduce new architecture unless validation discovers a genuine defect.

---

# 1. Objective

Prove this complete production path:

```text
customer/release query
→ canonical film release
→ TAPE inventory
→ supplier inventory
→ StockAvailabilityService
→ CommerceOfferService
→ customer-safe commerce result
```

Validate six real scenarios:

A. In Stock  
B. Available from Supplier  
C. Out of Stock  
D. Oversold / preorder  
E. Multiple Suppliers  
F. Agent-only / supplier-only release

Use real production data wherever possible. Do not modify production data to manufacture scenarios.

---

# 2. Pre-validation health check

Capture current production counts for:

- release_variants
- release_shopify_listings
- tape_inventory_levels
- supplier_offers
- supplier_offer_observations
- inventory_events
- shopify_listings

Also report counts by `product_domain`.

Confirm:
- Film Shopify II contains no `music_vinyl`
- Supplier II is healthy
- Shopify II is healthy
- latest dual-write errors = 0

Normal catalogue movement since the last report is acceptable; explain any differences.

---

# 3. Case A — TAPE In Stock

Find a real Film Shopify listing where `tape.available > 0`.

Report:
- title
- release_variant_id
- barcode
- Shopify variant ID
- Shopify retail price
- on_hand
- committed
- available
- supplier positions
- StockAvailability result
- internal CommerceOffer result
- public CommerceOffer result

Expected customer behaviour:

`In Stock — Shopify price`

Assertions:
- price source = Shopify
- supplier cost cannot alter retail price
- supplier identity hidden publicly

PASS / FAIL.

---

# 4. Case B — Available from Supplier

Find a real Shopify-listed Film release where `tape.available <= 0` and at least one fresh eligible supplier offer exists.

Report internally:
- TAPE availability
- supplier(s)
- supplier quantities
- supplier costs
- preferred supplier
- Shopify price

Expected public behaviour:

`Available from Supplier — Shopify price`

Critical assertions:
- NO Lasgo/Moovies name
- NO supplier SKU
- NO supplier cost
- NO supplier quantity
- NO price recalculation

PASS / FAIL.

---

# 5. Case C — Out of Stock

Find a real Shopify-listed Film release where `tape.available <= 0` and no fresh eligible supplier offer exists.

Expected public state: `Out of Stock` or exact existing equivalent.

Confirm missing supplier data is not interpreted as supplier availability.

Report Stock Availability + Commerce Offer results.

PASS / FAIL.

---

# 6. Case D — Oversold / preorder

Find a real Film release where `available < 0`. Prefer a preorder if available.

Report:
- on_hand
- committed
- available
- preorder/backorder state
- supplier positions
- StockAvailability status
- CommerceOffer status
- public customer state

Confirm negative available remains negative and is not clamped to zero in canonical Inventory Intelligence.

Document the actual deterministic precedence between preorder, supplier availability and oversold.

Do not redesign it unless the result is demonstrably wrong.

PASS / FAIL.

---

# 7. Case E — Multiple suppliers

Find a real Film release with multiple current supplier offers. Prefer one with both Lasgo and Moovies if available.

Report internally for each supplier:
- availability
- quantity
- cost
- freshness

Also report:
- preferred supplier
- reason preferred

Confirm suppliers remain independent. Do not calculate combined supplier quantity.

Expected public result where applicable:

`Available from Supplier — $XX.XX`

Public result must not reveal supplier count, names, quantities, costs or preferred supplier.

PASS / FAIL.

---

# 8. Case F — Agent-only release

Find a real Film `release_variant` with NO `release_shopify_listing` and at least one fresh available supplier offer.

Report internally:
- title
- release_variant_id
- barcode if available
- Shopify listing = none
- supplier positions
- preferred supplier
- preferred supplier cost
- calculated TAPE retail price
- pricing policy/path

Run:
- StockAvailabilityService
- CommerceOfferService

Expected:
- listing_type = agent_only
- customer_status = available_to_order
- pricing_source = supplier pricing policy

Expected public presentation:

`Available to Order — A$XX.XX`

Critical assertions:
- NO Shopify product required
- NO Shopify product created
- NO supplier identity exposed
- NO supplier cost exposed
- NO supplier quantity exposed

PASS / FAIL.

---

# 9. Customer-safe payload audit

For all six cases, recursively inspect the actual public/customer-safe payload.

It must not expose fields or values corresponding to:
- Lasgo
- Moovies
- supplier_id
- supplier_sku
- supplier_offer_id
- supplier_cost
- unit_cost
- supplier_quantity
- preferred_supplier

Use actual field names from the implementation.

Do not rely only on unit tests. Validate production-shaped responses.

Report:

`PUBLIC SUPPLIER PRIVACY: PASS / FAIL`

---

# 10. Price authority validation

For every Shopify-listed sample prove:

`customer retail price == Shopify retail price`

regardless of TAPE stock, supplier availability, supplier cost, preferred supplier or number of suppliers.

For agent-only Case F prove:

`customer retail price = deterministic TAPE pricing service output`

Explicitly report:

`SHOPIFY PRICE AUTHORITY: PASS / FAIL`

`AGENT-ONLY PRICING: PASS / FAIL`

---

# 11. Inventory arithmetic

Spot-check at least 5 real Shopify Film releases.

Compare Shopify source → tape_inventory_levels → StockAvailabilityService for:
- on_hand
- committed
- available

Confirm:

`available = on_hand - committed`

including a negative example if naturally available.

Report mismatches individually.

---

# 12. Freshness

For supplier-backed cases validate:
- observed_at
- availability_status
- is_stale

Confirm fresh offers are treated as fresh.

If a naturally stale example exists, validate it. Do not alter timestamps to manufacture one.

If no production stale example exists, use the existing automated test to prove stale behaviour.

---

# 13. Film search

Validate Film `search_inventory`.

Test:
1. a known Shopify-listed film
2. the Case F supplier-only film
3. at least one title known to have both film and soundtrack presence

Confirm:
- film candidates returned
- `music_vinyl` excluded
- Shopify films and supplier-only canonical films are discoverable

Report PASS / FAIL.

---

# 14. No combined inventory regression

Inspect all public and internal summary outputs used by the future agent.

Confirm there is no canonical field equivalent to:

`TAPE available + supplier quantity`

TAPE inventory and supplier inventory must remain independent.

Report:

`NO COMBINED INVENTORY: PASS / FAIL`

---

# 15. Failure behaviour

Validate:
- unknown barcode
- unknown release_variant_id
- release with no TAPE inventory
- release with no supplier inventory

Use existing test fixtures for ambiguity if no production ambiguity exists.

Confirm:
- structured errors
- no hallucinated release
- no fuzzy fallback inside exact lookup
- missing supplier inventory does not become zero stock
- valid TAPE data survives partial supplier absence

---

# 16. Inventory history

If inventory history is implemented, validate one real Film release with historical events.

Show deterministic examples where available:
- TAPE stock changed
- supplier stock changed
- supplier cost changed
- supplier availability changed

If this capability remains deferred, report:

`HISTORY DEFERRED`

Do not build it during this validation.

---

# 17. Performance

Measure a small number of production reads for:
- Stock Availability by release_variant_id
- Stock Availability by barcode
- Commerce Offer — Shopify path
- Commerce Offer — agent-only path
- search → release → commerce offer

Report approximate min / median / max.

Do not stress-test production.

---

# 18. Regression suite

Run all relevant tests, including:
- Stock Availability
- Commerce Offer
- Shopify II
- Supplier II where appropriate
- product domain
- vinyl exclusion
- search
- privacy
- price authority
- no-combined-quantity
- non-release exclusion

Report passed / failed / skipped.

Any failure must be explained.

---

# 19. Final customer-state matrix

Produce a matrix using the actual production releases selected:

| Scenario | Release | TAPE available | Supplier state | Shopify? | Price source | Customer state | Result |
|---|---|---:|---|---|---|---|---|
| In Stock | ... | ... | ... | Yes | Shopify | In Stock | PASS/FAIL |
| Supplier | ... | ... | ... | Yes | Shopify | Available from Supplier | PASS/FAIL |
| OOS | ... | ... | ... | Yes | Shopify | Out of Stock | PASS/FAIL |
| Oversold | ... | ... | ... | Yes | Shopify | ... | PASS/FAIL |
| Multi-supplier | ... | ... | ... | Yes/No | ... | ... | PASS/FAIL |
| Agent-only | ... | n/a | available | No | TAPE pricing | Available to Order | PASS/FAIL |

---

# 20. Architecture completion decision

If all critical validation passes, explicitly declare the Inventory Intelligence foundation complete.

We should not continue adding inventory infrastructure simply because additional improvements are possible.

Ordering Agent V1 should then consume the existing deterministic services.

Intended next architecture:

```text
Customer
   ↓
Ordering Agent
   ↓
Film search_inventory
   ↓
canonical release candidate(s)
   ↓
StockAvailabilityService
   ↓
CommerceOfferService
   ↓
customer-safe response
```

The LLM handles:
- natural language
- intent
- clarification
- candidate presentation
- conversation

Deterministic services handle:
- identity
- inventory
- supplier selection
- freshness
- pricing
- commerce eligibility
- customer-safe data

The LLM must never invent stock or calculate price.

---

# 21. Final report

Return:

1. Executive summary
2. Production health/baseline
3. Case A — In Stock
4. Case B — Available from Supplier
5. Case C — Out of Stock
6. Case D — Oversold / Preorder
7. Case E — Multiple Suppliers
8. Case F — Agent-only
9. Customer-safe payload/privacy
10. Price authority
11. Inventory arithmetic
12. Freshness
13. Film search + vinyl exclusion
14. Failure behaviour
15. History
16. Performance
17. Tests
18. Final customer-state matrix
19. Remaining risks
20. Recommendation

Finish with exactly one:

`INVENTORY FOUNDATION COMPLETE — READY FOR ORDERING AGENT V1`

or:

`INVENTORY FOUNDATION NOT READY`

If not ready, list only concrete blockers.

---

# Guardrails

This is a read-only production validation.

Do not:
- modify Shopify products
- modify Shopify prices
- modify Shopify inventory
- create Shopify products
- create customer orders
- create POs
- change feature flags
- alter cron
- delete production records
- change inventory to manufacture test cases
- build Ordering Agent V1 yet

Validate the platform we have built.

If it passes, stop infrastructure development and recommend moving to Ordering Agent V1.
