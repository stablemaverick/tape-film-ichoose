# Ordering Agent V1 — Discovery & Implementation Plan

**Spec:** `docs/inventory-intelligence/tape-ordering-agent-v1-build-specification.md`  
**Date:** 2026-08-10

---

## Existing agent code reconciliation

| Component | Disposition | Notes |
|---|---|---|
| `app/routes/api.agent-query.ts` | **ADAPT → DEPRECATE for commerce** | Live admin assistant; stock/price from catalogue — must not remain authoritative |
| `app/lib/query-parser.server.ts` | **REUSE** | OpenAI `gpt-4o-mini` structured parse |
| `app/lib/tape-agent-query-parser.server.ts` | **REUSE / ADAPT** | Deterministic intent + facets; extend for II search intent |
| `app/lib/search-query-facets.server.ts` | **REUSE** | Format/label/edition facets |
| `app/routes/api.intelligence-search.ts` | **DEPRECATE for ordering** | Film discovery over `catalog_items`; not II truth |
| `app/lib/film-offer-ranking.server.ts` | **DEPRECATE for commerce** | Catalogue availability buckets |
| `app/routes/app._index.tsx` | **ADAPT** | Admin harness → internal eval UI; strip cost/supplier qty |
| `app/routes/api.create-draft-order.ts` / wishlist | **UNCHANGED** (later phase) | Out of V1 customer scope |
| `app/routes/api.stock-availability.ts` | **REUSE** (internal pattern) | Key-gated II access |
| `StockAvailabilityService` / `CommerceOfferService` | **REUSE** | Authoritative V1 path |
| `tape-film-agent/**` | **REMOVE LATER** | Scaffold; do not grow as second agent |
| Agent catalogue tests | **ADAPT** | Retarget to II contracts |

**Conflict:** today’s agent returns `costGbp`, `supplierStock`, and catalogue `calculated_sale_price`. V1 must use only public Commerce Offer fields.

**After V1:** one customer ordering path:

```text
search_inventory → StockAvailabilityService → CommerceOfferService
```

---

## Existing Shopify app architecture

- **One app** at repo root: `shopify.app.toml`, `embedded = true`, React Router + `app/shopify.server.ts`
- **Admin-only today** — agent UI under `/app` with `authenticate.admin`
- **No App Proxy** configured
- **No Theme App Extension** (`extensions/` empty)
- Storefront does **not** call TAPE today
- Auth available: admin session, webhooks, `PIPELINE_HEALTH_KEY`; no public/proxy auth yet

---

## Storefront integration recommendation (evidence-based)

```text
Theme App Extension + App Embed
  → App Proxy (/apps/<subpath>/ordering-agent)
  → POST Ordering Agent API on existing Shopify app
  → II services
```

Do **not** create a second Shopify app. Do **not** expose unauthenticated `/api/agent-query` from the theme.

V1 deliverable: backend + customer-safe API + documented path. Minimal gated admin/CLI harness only — no public widget launch.

---

## Implementation plan (proceed)

### Phase A — Backend core
1. Python orchestrator: intent → `search_inventory` → rank/clarify → `CommerceOfferService` public payload
2. Customer-safe serializers (no supplier/cost/qty; optional `shopify_listed` + product path)
3. Bounded conversation store (in-memory/DB short-lived by `conversation_id`)
4. `POST /api/ordering-agent` (Remix) calling orchestrator; deprecate catalogue stock answers on this path
5. Wire LLM parse to reuse existing OpenAI structured output; force tool/service calls before stock/price claims

### Phase B — Harness & eval
6. CLI harness for NL queries
7. Fixtures from e2e cases A–F + NL/adversarial/pricing eval sets
8. Privacy server-side assertions on API responses

### Phase C — Shopify readiness (no public launch)
9. Document App Proxy + Theme Embed contract
10. Optional: stub extension folder / feature-gated admin “Ask TAPE (II)” panel
11. Final report per §37 + §49

### Explicit non-goals (V1)
No checkout, draft orders for customers, Shopify product create, PO, Music/Vinyl agent, public storefront widget launch, parallel `tape-film-agent` runtime.

### Flags
Do not change production `INVENTORY_DUAL_WRITE_*` flags.
