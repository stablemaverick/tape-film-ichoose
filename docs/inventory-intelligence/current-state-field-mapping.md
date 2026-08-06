# Current-state → inventory intelligence field mapping

Companion to [live-schema-introspection.md](./live-schema-introspection.md).

## Identity

| Current | Target | Notes |
|---------|--------|-------|
| — | `release_variants.id` | Canonical `release_variant_id` |
| `films.id` | `release_variants.film_id` | Optional |
| — | `release_variants.edition_id` | Placeholder null |
| — | `release_variants.publication_status` | Default `supplier_only` until published |
| `shopify_listings (shop, shopify_variant_id)` | `release_shopify_listings` | Channel only — never canonical id |
| `catalog_items.id` | `release_variants.catalog_item_id` | Bridge |
| `barcode` | `variant_identifiers` + `primary_barcode` | Attribute |

## TAPE inventory (stocked releases only)

| Current | Target |
|---------|--------|
| Shopify `on_hand` / `committed` / `available` / `incoming` | `tape_inventory_levels.*` + `shopify_incoming_reported` |
| PO open confirmed qty | `po_incoming_confirmed` (separate; do not auto-sum) |
| `shopify_listings.inventory_quantity` | Fallback for `available` when detailed levels not fetched |

Supplier-only releases **do not require** tape inventory rows.

## Supplier availability

| Current | Target |
|---------|--------|
| `(supplier, barcode)` grain | `(supplier_id, supplier_sku)` identity; barcode attribute |
| `availability_status` legacy strings | Controlled enum via `availability_rules` |
| `supplier_stock_status` | `reported_quantity` + `quantity_is_exact` |
| `supplier_last_seen_at` | `last_seen_at` + `source_feed_at` + `pipeline_completed_at` |

## Purchase orders / history

| Current | Target |
|---------|--------|
| Inbound PO CSV | `purchase_orders` / `purchase_order_lines` |
| `supplier_orders` (customer webhook) | Unchanged — not inbound POs |
| Overwrites on catalog | `supplier_offer_observations` + `inventory_events` |
