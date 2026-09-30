# Ordering Agent V1 — Shopify App Proxy design

**Status:** Designed, not launched.  
**Recommendation:** Theme App Embed → App Proxy → Ordering Agent backend on the existing embedded app.

## Concrete configuration

Add to `shopify.app.toml` (not applied in this phase):

```toml
[app_proxy]
url = "https://<app-host>/api/ordering-agent"
subpath = "ordering-agent"
prefix = "apps"
```

Storefront call path:

```text
POST https://{shop}.myshopify.com/apps/ordering-agent
  → Shopify App Proxy (signed)
  → https://<app-host>/api/ordering-agent
```

## Backend route

- Existing: `app/routes/api.ordering-agent.ts`
- Keep internal `PIPELINE_HEALTH_KEY` auth for CLI/ops
- Before public launch, add App Proxy HMAC verification (Shopify `authenticate.public.appProxy` or equivalent signature check on query params `signature`, `timestamp`, `shop`, …)

## Request / response contract

Request JSON (body after proxy):

```json
{
  "message": "Can you get Creepozoids on Blu-ray?",
  "conversation_id": "optional-opaque-id"
}
```

Response: Ordering Agent V1 customer-safe payload (`answer` | `clarify` | `not_found` | `error`).

Never return `observability` to browsers in production (strip unless `debug=1` + staff gate).

## Conversation ID / anonymous session

- Client generates opaque UUID in `sessionStorage` / embed bootstrap
- Sent as `conversation_id` on each turn
- Server stores only bounded candidate list (1h TTL) — no customer PII
- Closing/reopening widget may reuse the same ID within TTL

## Rate limiting

Before launch:
- Per-IP and per-shop soft limits on `/api/ordering-agent`
- Reject oversized messages
- Optional bot challenge later

## CORS / CSRF

- App Proxy is **same-origin** to the shop domain → no CORS needed for theme JS calling `/apps/ordering-agent`
- CSRF: Shopify proxy signature replaces cookie CSRF for proxy path; do not enable wide-open CORS to Admin APIs

## Security checklist

- [ ] Verify proxy signature on every request
- [ ] Strip observability from public responses
- [ ] No direct browser access to Stock Availability / Commerce Offer / Supabase
- [ ] Rate limits
- [ ] Feature gate until human acceptance passes
