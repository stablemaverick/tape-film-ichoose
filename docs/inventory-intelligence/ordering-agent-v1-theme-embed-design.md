# Ordering Agent V1 — Theme App Embed design

**Status:** Designed; scaffold optional. Do not enable for customers in this phase.

## Extension location

```text
extensions/ask-tape-embed/
  shopify.extension.toml
  blocks/ask-tape.liquid
  assets/ask-tape.js
  assets/ask-tape.css
```

`shopify.extension.toml` (sketch):

```toml
api_version = "2025-01"
[[extensions]]
name = "Ask TAPE"
handle = "ask-tape-embed"
type = "theme"
```

App Embed block: floating chat launcher in theme editor → merchant enables on TAPE theme.

## Bootstrap

1. Liquid injects `assets/ask-tape.js` + CSS when embed enabled
2. JS creates launcher button + panel (closed by default)
3. Generates/stores `conversation_id` in `sessionStorage`
4. API base: `/apps/ordering-agent` (App Proxy)

## Behaviour

| Action | Behaviour |
|---|---|
| Open/close | Toggle panel; retain transcript in-memory for session |
| Send message | POST JSON `{message, conversation_id}` |
| Loading | Disable input; show subtle pending state |
| Answer | Show message + availability label + price |
| Shopify-listed | If `product_url`, show “View product” → `product_url` |
| Agent-only | Show Available to Order + price; no fake product link |
| Clarify | Render 3–5 choices; click sends choice label / index as follow-up |
| Errors | Customer-safe text only |
| Mobile | Full-width bottom sheet; large tap targets |

## Non-goals for this phase

- No polished production styling pass
- No checkout / add-to-cart for agent-only
- Not enabled on production theme

## Low-risk scaffold

A non-public scaffold under `extensions/ask-tape-embed/` may be added later; V1 closeout documents the contract without requiring merchant enablement.
