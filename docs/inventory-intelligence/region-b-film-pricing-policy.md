# Tape! Film — Region B existing-film pricing policy

Approved baseline for UK/GBP-sourced Region B film catalogue pricing and controlled existing-product repricing.

## Core principle

**Replacement cost determines the economic floor.**  
**Commercial judgement determines whether a material existing-price increase should actually be applied.**

Tape! Film is a curated specialist retailer — not an automatic lowest-price competitor and not a purely formula-driven retailer.

The calculated 28% price tells us what retail is required to achieve Tape’s desired minimum replacement-cost gross margin. That calculation is never weakened or removed. The calculated floor and the automatic Shopify mutation decision are **separate concepts**.

## Canonical calculator

Single implementation: `app/rules/pricing_rules.py`.

Do not duplicate the pricing formula elsewhere.

Pipeline:

1. GBP supplier cost × `GBP_AUD_RATE` × `LANDED_COST_MARKUP` → landed AUD
2. GST-aware retail calculation
3. Minimum **28%** replacement-cost gross margin (ex-GST)
4. `.99` retail convention
5. Exact Decimal validation after rounding

Production assumptions (defaults):

| Assumption | Value |
|---|---|
| `GBP_AUD_RATE` | 2.0 |
| `LANDED_COST_MARKUP` | 1.12 |
| `DEFAULT_MARGIN_FLOOR_RATIO` | 0.28 |
| `GST_RATE` | 1.10 |

## 28% is a floor, not a target

For existing film products:

- **Below floor** → the calculator reports the minimum valid `.99` price that achieves at least 28% replacement-cost GP.
- **Already at/above floor** → **KEEP_CURRENT_PRICE**. Never auto-reduce toward 28%.

Realised catalogue margin can and should exceed 28% where existing retail already supports it.

Tape may consciously approve an exception where achieving 28% would create an unattractive or commercially unreasonable retail price. For a reviewed product we may:

- accept the calculated 28% price;
- choose an intermediate retail price and knowingly accept a lower GP;
- retain the existing price temporarily;
- or investigate supplier/market economics further.

Any exception below the calculated floor must be **explicit and auditable**. Do not silently override the pricing calculator.

Persisted approvals live in `app/config/region_b_pricing_exceptions.json` (variant-ID keyed). While live retail still matches the approved price and GBP cost has not risen materially (≥£0.50 or ≥5%), scans emit `APPROVED_PRICING_EXCEPTION` instead of re-queuing an automatic increase. If GBP replacement cost later rises materially, the exception re-opens as `REVIEW_PRICE_INCREASE` (`exception_reopen_gbp_cost_increased`).

## Tiered price-increase review (existing catalogue)

| Required increase to floor | Action | Auto-apply? |
|---|---|---|
| $1–$5 inclusive | `PRICE_INCREASE_AUTO_ELIGIBLE` | Yes, only via reviewed allowlist + all safety checks |
| $6–$10 inclusive | `REVIEW_PRICE_INCREASE` | **No** — commercial/merchandising review first |
| >$10 | `REVIEW_LARGE_INCREASE` | **No** — mandatory manual review; never partial-increase to fit a threshold |

Defaults in code: `DEFAULT_MAX_AUTO_INCREASE = 5.0`, `DEFAULT_COMMERCIAL_REVIEW_MAX = 10.0`.

## Optional approved-price override

Reviewed candidate artifacts may carry:

| Field | Meaning |
|---|---|
| `calculated_floor_price` | Canonical 28% `.99` floor (always recorded) |
| `approved_retail_price` | Price authorised to apply |
| `approved_gp_percent` | GP at the approved retail |
| `pricing_exception_reason` | Required when approved retail is below the floor |

For normal automatic candidates: `approved_retail_price = calculated_floor_price`.

For manually reviewed products, approved retail may be below the calculated floor **only** when set in an explicitly reviewed artifact with a reason (e.g. `MARKET_POSITIONING_EXCEPTION`).

The apply service uses an override only when it exists on the reviewed row. **Never** generate a below-floor override automatically.

## Commercial positioning

Tape! Film is a specialist curated physical-media retailer (closer in philosophy to a BFI Shop–style proposition than a lowest-price commodity disc retailer): curated selection, boutique labels, important/new releases, presentation, editorial, discovery, specialist knowledge, Australian access, and sustainable retail economics.

Pricing must **not** automatically chase the cheapest Australian competitor.

## Competitor / market pricing is informational

Do **not** introduce competitor-price matching into the automatic pricing calculator.

A competitor selling below Tape’s calculated price does **not** justify an automatic reduction.

Primary authority:

`replacement cost → sustainable GP floor → (commercial judgement) → Tape retail price`

Not:

`competitor price → match/undercut`

For `$6–$10` and `>$10` review products, market/RRP evidence may inform the human decision.

Reserved reason: `MARKET_POSITIONING_EXCEPTION` — Tape consciously accepts a lower replacement-cost GP because the calculated floor would make retail commercially unattractive. This is a legitimate merchandising decision, not a pricing-calculation failure.

Reserved signal: `REVIEW_MARKET_PRICE_OUTLIER` — human review only; never automatically overrides the 28% floor.

## Permanent product scope

Automated film repricing excludes:

- Vinyl, CDs, Books, Games
- Gift cards / test products
- Ambiguous / non-film products

Film classification (`app/services/film_product_class.py`) **beats** Region B / GBP / allowlist eligibility.

## Region A remains separate

Do **not** apply the Region B GBP model to Region A / US products.

- Criterion Collection Region B → UK / GBP / this policy
- Criterion Collection Region A → US / USD / separate approved model (follow-up)

## Existing-product repricing controls

Long-term: extend across eligible existing Region B film catalogue **with** reviewed cohorts — not unrestricted whole-catalogue apply.

Required safeguards:

- Dry-run default
- Explicit reviewed allowlist required for `--apply`
- Fresh supplier evidence; unambiguous release mapping
- Region B + film-only verification (including on apply revalidation)
- No automatic price decreases
- Tiered increase review (auto ≤$5; commercial $6–$10; large >$10)
- Cost anomalies / stale / ambiguous → skip or review
- Hard-review barcodes where known (Easy Rider/Moonrise; The Mask LE; Gladiator II / Poltergeist Film Vault LE cost drift)
- Variant-ID mutation; price-only Shopify write
- Full audit artifacts under `tmp/region_b_film_repricing_*`

Barcode safety: variant/release identity > barcode. Ambiguous barcode→multi-release resolution → `REVIEW_COST_ANOMALY` / skip. Florida Project `5028836042709` is not hard-reviewed; inactive siblings are excluded by `status:active` + variant-ID allowlists.

CLI: `scripts/maintenance/sync_region_b_film_repricing.py`

## Production observability (READ-ONLY)

Scheduled evaluation entry point:

`scripts/maintenance/run_region_b_pricing_health.py`

Wired as stock-sync **step 04e** (after supplier intelligence projection / inventory-policy processing).

Hard guarantees:

- No `--apply` flag on the health CLI
- Sets `REGION_B_PRICING_HEALTH_READONLY=1` so `apply_price_updates` refuses mutations
- Does not update Shopify price, inventoryPolicy, inventory quantity, cost, or metafields
- Manual allowlisted repricing remains a separate guarded command (`sync_region_b_film_repricing.py --apply` + allowlist)

### Durable output (dashboard ingestion)

Deterministic latest current-state (not timestamped `tmp/`):

| Artifact | Path |
|---|---|
| Latest JSON | `var/pricing_health/region_b_pricing_health_latest.json` |
| Latest CSV | `var/pricing_health/region_b_pricing_health_latest.csv` |
| Meta / last success | `var/pricing_health/region_b_pricing_health_meta.json` |
| History copies | `var/pricing_health/history/region_b_pricing_health_YYYYMMDDThhmmssZ.json` |

Future Shopify Operations Dashboard should consume the **latest** files (and/or meta), not ad-hoc `tmp/` run names.

### Exception registry

`app/config/region_b_pricing_exceptions.json` is version-controlled production config (variant-ID keyed). Stock sync must not overwrite it.

While live retail matches the approved exception and GBP has not risen materially (≥£0.50 or ≥5%), health emits `APPROVED_PRICING_EXCEPTION`.

Material GBP increase → `REVIEW_PRICE_INCREASE` / `exception_reopen_gbp_cost_increased` (observe only; no Shopify mutation).

Live retail no longer equals approved exception → `REVIEW_PRICE_INCREASE` / `exception_live_price_mismatch` (registry is not silently authoritative).

Supplier cost decreases → exception remains approved; never auto-reduce retail.

### Region A / non-film

Region A remains parked and is reported as `REGION_A_BLOCKED` (separate model; out of GBP scope).

Non-film products are `OUT_OF_SCOPE_NON_FILM` / skipped ambiguous — never enter film pricing.
