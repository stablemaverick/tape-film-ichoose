# Shopify Product Publisher

Publishes products from `catalog_items` to your Shopify store as draft products
using barcodes as the lookup key.

---

## Setup

1. Ensure `.env.prod` in this folder has the correct credentials:

```
SHOPIFY_SHOP=your-store.myshopify.com
SHOPIFY_CLIENT_ID=your_client_id
SHOPIFY_CLIENT_SECRET=your_client_secret
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_KEY=your_service_key
GBP_AUD_RATE=1.95
LANDED_COST_MARKUP=1.12
```

2. The `venv` must be set up in the parent project with dependencies installed.

---

## How to use

### 1. Add barcodes to `barcodes.csv`

One barcode per line, with a `barcode` header:

```csv
barcode
5028836042884
5028836042860
5028836042839
```

### 2. Dry-run (preview what will be created)

```bash
cd /Users/simonpittaway/Dropbox/tape-film-ichoose
venv/bin/python shopify_publish/publish_selected_barcodes_to_shopify.py \
  --env shopify_publish/.env.prod \
  --barcodes-file shopify_publish/barcodes.csv \
  --dry-run
```

### 3. Create products for real

```bash
venv/bin/python shopify_publish/publish_selected_barcodes_to_shopify.py \
  --env shopify_publish/.env.prod \
  --barcodes-file shopify_publish/barcodes.csv
```

### 4. Inline barcodes (without CSV)

```bash
venv/bin/python shopify_publish/publish_selected_barcodes_to_shopify.py \
  --env shopify_publish/.env.prod \
  --barcodes "5028836042884,5028836042860"
```

---

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `--barcodes` | — | Comma-separated list of barcodes |
| `--barcodes-file` | — | Path to CSV/text file with one barcode per line |
| `--supplier` | `best_offer` | Choose supplier: `best_offer`, `moovies`, `lasgo`, `Tape Film` |
| `--status` | `draft` | Shopify product status: `draft`, `active`, `archived` |
| `--env` | `.env` | Path to environment file |
| `--dry-run` | off | Preview only, no Shopify writes |
| `--api-version` | `2026-04` | Shopify API version |

---

## What gets created

Each product is created as a **simple product** (no variants) with:

| Field | Source |
|-------|--------|
| Title | `catalog_items.title` |
| Handle | Auto-generated slug from title |
| Vendor | `TAPE! FILM` |
| Category | Media > Videos > Blu-ray |
| Status | Draft (default) |
| Tags | genres + format + director + barcode |
| SEO Title | Product title |
| SKU | `catalog_items.supplier_sku` |
| Barcode | As provided |
| Price | `catalog_items.calculated_sale_price` (already in AUD) |
| Cost | `catalog_items.cost_price` × GBP→AUD rate × 1.12 |
| Weight | 0.25 kg |
| Inventory | Tracked, deny when out of stock |

### Metafields set

| Metafield | Source |
|-----------|--------|
| `custom.director` | `catalog_items.director` |
| `custom.studio` | `catalog_items.studio` |
| `custom.format` | `catalog_items.format` |
| `custom.starring` | `catalog_items.top_cast` |
| `custom.country_of_origin` | `catalog_items.country_of_origin` |
| `custom.region` | Derived (UK suppliers = Region B) |
| `custom.film_released` | `catalog_items.film_released` |
| `custom.media_release_date` | `catalog_items.media_release_date` |
| `custom.pre_order` | `true` if media_release_date > today |
| `custom.po_flag` | `Pre-Order` if pre-order, otherwise skipped |

---

## Pricing

- **Sale price**: Taken directly from `calculated_sale_price` in catalog (already AUD).
- **Cost price**: Converted from GBP using `cost_price × GBP_AUD_RATE × LANDED_COST_MARKUP`.
- Both rates are configurable in `.env.prod`. Defaults: `GBP_AUD_RATE=1.95`, `LANDED_COST_MARKUP=1.12`.

---

## Behaviour

- **Duplicate check**: Before creating, checks if a variant with that barcode already exists in Shopify. If so, skips it.
- **Best offer**: When multiple suppliers have the same barcode, picks the best row using the scoring system (Tape Film priority > availability > stock > supplier priority > price).
- **Supplier override**: Use `--supplier moovies` to force a specific supplier.
- **Idempotent**: Safe to re-run. Existing products are skipped, not duplicated.
