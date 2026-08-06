# Architecture, ERD, and data flow (Phase 3b)

## Architecture (inventory intelligence layer)

```
┌─────────────────────────────────────────────────────────────────┐
│  Consumers (UNCHANGED in Phase 3b)                              │
│  Agent ranking · Admin lookup · Storefront                      │
│  still read catalog_items / Shopify live APIs                   │
└──────────────────────────────┬──────────────────────────────────┘
                               │ future: unified availability only
┌──────────────────────────────▼──────────────────────────────────┐
│  Inventory intelligence facts (additive)                        │
│                                                                 │
│  release_variants  ←── supplier_offers ←── observations/events  │
│        │                    ▲                                   │
│        ├── release_shopify_listings                             │
│        ├── tape_inventory_levels (stocked only)                 │
│        ├── variant_identifiers                                  │
│        └── purchase_order_lines                                 │
└──────────────────────────────▲──────────────────────────────────┘
                               │ dual-write (feature-flagged)
┌──────────────────────────────┴──────────────────────────────────┐
│  Existing operational pipelines                                 │
│  Moovies/Lasgo normalize · Shopify store sync · PO inbound CSV  │
│  catalog_items remains operational SoT for agent                │
└─────────────────────────────────────────────────────────────────┘
```

## Entity relationship diagram

```
films
  └── release_variants (id = release_variant_id)
        │  publication_status, primary_barcode, film_id, edition_id?
        │
        ├── variant_identifiers (barcode/ean/sku attrs; conflict_flag)
        │
        ├── release_shopify_listings
        │     UNIQUE (shop, shopify_variant_id)
        │     0..N channels; Shopify IDs are NOT canonical
        │
        ├── tape_inventory_levels          ★ only if TAPE physically stocks
        │     on_hand, committed, available
        │     po_incoming_confirmed        (PO-owned)
        │     shopify_incoming_reported    (Shopify-owned)
        │
        ├── supplier_offers
        │     UNIQUE (supplier_id, supplier_sku)
        │     raw_barcode attribute
        │     release_variant_id nullable until resolved
        │       └── supplier_offer_observations (append-only, dedupe_key)
        │
        ├── supplier_sku_resolutions
        │     UNIQUE active (supplier_id, supplier_sku)
        │     review_status: auto_accepted | needs_review | …
        │
        ├── purchase_order_lines
        │     └── purchase_orders
        │
        └── inventory_events
              optional observation_id / po / tape level refs
              dedupe_key prevents unchanged duplicates

suppliers (moovies, lasgo, tape_film)
```

## Data flow: supplier feed → release → offer → tape

```
Supplier FTP file
      │
      ▼
staging_*_raw  →  normalize  →  staging_supplier_offers
      │                              │
      │                              │ existing path (always)
      │                              ▼
      │                         catalog_items  (agent SoT unchanged)
      │
      │         INVENTORY_DUAL_WRITE_ENABLED=1
      │         INVENTORY_DUAL_WRITE_SUPPLIER=1
      ▼
resolve (supplier_id + supplier_sku)
  ├─ high-confidence barcode match → attach release_variant_id
  ├─ ambiguous / low confidence    → needs_review (offer unresolved)
  └─ no release                    → create release_variant
                                       publication_status=supplier_only
      │
      ▼
supplier_offers upsert (idempotent)
  └─ material change? → observation + inventory_event

Shopify store sync (separate)
  └─ INVENTORY_DUAL_WRITE_SHOPIFY=1
       → release_variants (published)
       → release_shopify_listings
       → tape_inventory_levels (stocked/tracked only)
            Shopify owns on_hand/committed/available/shopify_incoming_reported
            NEVER written by supplier dual-write

PO inbound CSV
  └─ INVENTORY_DUAL_WRITE_PO=1
       → purchase_orders / lines
       → updates po_incoming_confirmed on existing tape levels only
```

### Ownership rules (enforced)

| Writer | May write | Must not write |
|--------|-----------|----------------|
| Supplier dual-write | `supplier_offers`, observations, resolutions, `supplier_only` releases | `tape_inventory_levels` qty fields, Shopify inventory API |
| Shopify dual-write | channels, published releases, tape levels (Shopify facts) | `po_incoming_confirmed`, supplier offer qty as tape on_hand |
| PO dual-write | PO tables, `po_incoming_confirmed` | Shopify `inventorySetQuantities`, inventing tape on_hand from supplier |

Preorder / supplier availability never creates positive TAPE on_hand via supplier paths.
