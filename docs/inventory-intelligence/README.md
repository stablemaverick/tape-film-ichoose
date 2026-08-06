# Inventory intelligence — overview

Phase 3a lands the additive schema and pure rules.  
Phase 3b adds dual-write hooks behind feature flags (default **OFF**).  
Consumers (agent, admin, storefront) are **unchanged**.

## Canonical entity: `release_variants`

A **release_variant** is TAPE’s permanent record of a specific physical media release/edition.

- Exists **independently of Shopify**
- Primary key: `release_variants.id` (`release_variant_id`)
- `publication_status`: `supplier_only` | `candidate` | `approved_for_listing` | `published` | `archived` | `discontinued`

Shopify is only a sales channel (`release_shopify_listings`), never the canonical identity.

## Three identities

| Identity | Key | Role |
|----------|-----|------|
| Canonical release | `release_variant_id` | Stable TAPE id |
| Supplier offer | `(supplier_id, supplier_sku)` | Supplier commercial offer; barcode is an attribute |
| Shopify listing | `(shop, shopify_variant_id)` | Published channel (0..N per release) |

## Documents

| Doc | Purpose |
|-----|---------|
| [live-schema-introspection.md](./live-schema-introspection.md) | Live DB discovery (Phase 3a) |
| [current-state-field-mapping.md](./current-state-field-mapping.md) | Current → new mapping |
| [architecture-and-data-flow.md](./architecture-and-data-flow.md) | Diagrams + data flow |
| [phase-3a-rollback.md](./phase-3a-rollback.md) | Drop foundation tables |
| [phase-3b-dual-write-plan.md](./phase-3b-dual-write-plan.md) | Dual-write plan |
| [phase-3b-verification-report.md](./phase-3b-verification-report.md) | Verification + reconciliation + assumptions |

## Feature flags (default off)

| Variable | Default | Purpose |
|----------|---------|---------|
| `INVENTORY_DUAL_WRITE_ENABLED` | `0` | Master switch |
| `INVENTORY_DUAL_WRITE_SHOPIFY` | `0` | Releases + channels + tape levels |
| `INVENTORY_DUAL_WRITE_SUPPLIER` | `0` | Offers + observations + resolution |
| `INVENTORY_DUAL_WRITE_PO` | `0` | Purchase orders + `po_incoming_confirmed` |
| `INVENTORY_CREATE_SUPPLIER_ONLY_RELEASES` | `1` | Create `supplier_only` releases when unmatched |
| `AVAILABILITY_FEED_FRESH_MAX_HOURS` | `36` | Freshness window |
| `AVAILABILITY_FEED_AGING_MAX_HOURS` | `72` | Aging window |
| `SUPPLIER_RESOLUTION_AUTO_ACCEPT_MIN_CONFIDENCE` | `0.95` | Auto-accept floor |
| `INVENTORY_DUAL_WRITE_LEVEL_FETCH_MAX` | `500` | Max Shopify level fetches per store sync |

## Explicitly not done yet

- Phase 4 backfills  
- Switching agent / admin / storefront to the unified contract  
- Materialised availability view as required path  
- Sales / demand event writers  
- Editions model (only nullable `edition_id` placeholder)
