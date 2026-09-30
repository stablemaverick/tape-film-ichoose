# Ordering Agent V1 — Shopify storefront integration contract

## Existing app (reuse)

- Single embedded Shopify app at repo root (`shopify.app.toml`, `embedded = true`)
- Auth: `app/shopify.server.ts` (Prisma session storage, `authenticate.admin`)
- No App Proxy configured today
- No Theme App Extension (`extensions/` empty)
- Do **not** create a second Shopify app

## Target request path (next phase)

```text
TAPE Shopify storefront
  → Theme App Extension + App Embed (“Ask TAPE”)
  → App Proxy: /apps/<subpath>/ordering-agent
  → POST /api/ordering-agent (customer-safe)
  → OrderingAgentService
  → search_inventory → StockAvailabilityService → CommerceOfferService
  → public serializer
```

## V1 backend contract (ready now)

`POST /api/ordering-agent` (auth: `PIPELINE_HEALTH_KEY` until App Proxy)

Request:
```json
{ "message": "Can you get Creepozoids on Blu-ray?", "conversation_id": "optional" }
```

Response types: `answer` | `clarify` | `not_found` | `error`

Answer release card (customer-safe):
```json
{
  "release_variant_id": "...",
  "title": "...",
  "availability": "available_to_order",
  "price": 43.99,
  "currency": "AUD",
  "shopify_listed": false,
  "product_url": null,
  "availability_label": "Available to Order — A$43.99"
}
```

- `shopify_listed=true` when a `release_shopify_listings` row exists
- `product_url` is null until store sync persists product handles (storefront prerequisite)
- Never returns supplier names, SKUs, costs, or raw II objects

## Session / security

- Bounded conversation candidates stored server-side (temp file, 1h TTL) keyed by `conversation_id`
- Anonymous storefront customers supported via opaque conversation IDs
- Browser must not call Stock Availability / Commerce Offer / Supabase directly
- Before public launch: add App Proxy signature verification + rate limiting
- Do not expose current unauthenticated `/api/agent-query` to the theme

## Shopify-listed vs agent-only

| | Shopify-listed | Agent-only |
|---|---|---|
| Customer state | In Stock / Available from Supplier / OOS / Preorder | Available to Order |
| Price | Shopify retail | Deterministic TAPE pricing |
| Widget CTA (future) | View product | Informational in V1; cart/checkout later |

## Legacy path

`/api/agent-query` + `/api/intelligence-search` remain admin catalogue assistants. They must **not** be the customer ordering authority. Ordering Agent V1 is the single authoritative customer path.

## Storefront launch prerequisites

1. Persist Shopify product `handle` into listings (for `product_url`)
2. Configure `[app_proxy]` in `shopify.app.toml`
3. Theme App Extension + App Embed widget
4. Proxy signature auth on `/api/ordering-agent` (replace or complement key auth)
5. Rate limiting / abuse protection
6. Human acceptance testing of NL eval set
7. Feature gate off until launch
