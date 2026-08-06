-- Inventory intelligence foundation (Phase 3a schema + Phase 3b-ready identity model).
-- Additive only. No dual-writes are performed by this SQL.
--
-- Canonical entity: release_variants (NOT sellable_variants).
-- Three identities:
--   1. release_variants.id              — TAPE permanent release identity
--   2. supplier_offers (supplier_id, supplier_sku) — supplier commercial offer
--   3. release_shopify_listings (shop, shopify_variant_id) — sales channel
--
-- Shopify existence is NOT required to create a release_variant.
-- tape_inventory_levels only for releases physically stocked by TAPE.
--
-- Rollback: docs/inventory-intelligence/phase-3a-rollback.md

begin;

-- ---------------------------------------------------------------------------
-- Suppliers
-- ---------------------------------------------------------------------------
create table if not exists public.suppliers (
  id text primary key,
  display_name text not null,
  priority integer not null default 100,
  active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint suppliers_priority_nonneg check (priority >= 0)
);

comment on table public.suppliers is
  'Canonical supplier registry. Seeded ids: moovies, lasgo, tape_film.';

insert into public.suppliers (id, display_name, priority, active)
values
  ('moovies', 'Moovies', 1, true),
  ('lasgo', 'Lasgo', 2, true),
  ('tape_film', 'Tape Film', 0, true)
on conflict (id) do nothing;

-- ---------------------------------------------------------------------------
-- Release variants (canonical TAPE physical media release)
-- ---------------------------------------------------------------------------
create table if not exists public.release_variants (
  id uuid primary key default gen_random_uuid(),
  film_id uuid references public.films (id) on delete set null,
  -- Placeholder for a future editions model; no FK in this migration.
  edition_id uuid null,
  catalog_item_id uuid references public.catalog_items (id) on delete set null,
  primary_barcode text null,
  title text null,
  format text null,
  publication_status text not null default 'supplier_only',
  active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint release_variants_publication_status_check check (
    publication_status in (
      'supplier_only',
      'candidate',
      'approved_for_listing',
      'published',
      'archived',
      'discontinued'
    )
  )
);

comment on table public.release_variants is
  'Canonical TAPE record for a specific physical media release/edition. Exists independently of Shopify.';
comment on column public.release_variants.id is
  'release_variant_id — permanent internal identifier; never replaced by Shopify IDs.';
comment on column public.release_variants.publication_status is
  'supplier_only | candidate | approved_for_listing | published | archived | discontinued';
comment on column public.release_variants.edition_id is
  'Nullable placeholder only — editions table not created yet.';

create index if not exists release_variants_primary_barcode_idx
  on public.release_variants (primary_barcode)
  where primary_barcode is not null;

create index if not exists release_variants_film_id_idx
  on public.release_variants (film_id)
  where film_id is not null;

create index if not exists release_variants_catalog_item_id_idx
  on public.release_variants (catalog_item_id)
  where catalog_item_id is not null;

create index if not exists release_variants_publication_status_idx
  on public.release_variants (publication_status);

-- ---------------------------------------------------------------------------
-- Shopify sales channels attached to a release (0..N)
-- ---------------------------------------------------------------------------
create table if not exists public.release_shopify_listings (
  id uuid primary key default gen_random_uuid(),
  release_variant_id uuid not null references public.release_variants (id) on delete cascade,
  shop text not null,
  shopify_product_id text null,
  shopify_variant_id text not null,
  shopify_inventory_item_id text null,
  is_primary boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

comment on table public.release_shopify_listings is
  'Shopify listing channel(s) for a release_variant. Identity: (shop, shopify_variant_id). Not the canonical release id.';

create unique index if not exists release_shopify_listings_shop_variant_uidx
  on public.release_shopify_listings (shop, shopify_variant_id);

create index if not exists release_shopify_listings_release_idx
  on public.release_shopify_listings (release_variant_id);

create index if not exists release_shopify_listings_inventory_item_idx
  on public.release_shopify_listings (shopify_inventory_item_id)
  where shopify_inventory_item_id is not null;

-- ---------------------------------------------------------------------------
-- Release identifiers (barcode and other attrs — not sole identity)
-- ---------------------------------------------------------------------------
create table if not exists public.variant_identifiers (
  id uuid primary key default gen_random_uuid(),
  release_variant_id uuid not null references public.release_variants (id) on delete cascade,
  id_type text not null,
  id_value text not null,
  source text null,
  is_primary boolean not null default false,
  is_valid boolean not null default true,
  conflict_flag boolean not null default false,
  created_at timestamptz not null default now(),
  constraint variant_identifiers_type_check check (
    id_type in ('barcode', 'ean', 'upc', 'sku', 'shopify_variant_id', 'supplier_sku', 'other')
  ),
  constraint variant_identifiers_value_nonempty check (length(trim(id_value)) > 0)
);

comment on table public.variant_identifiers is
  'Searchable identifiers for a release_variant. Barcode is an attribute; conflicts across releases are flagged.';

create unique index if not exists variant_identifiers_release_type_value_uidx
  on public.variant_identifiers (release_variant_id, id_type, id_value);

create index if not exists variant_identifiers_type_value_idx
  on public.variant_identifiers (id_type, id_value);

create index if not exists variant_identifiers_conflict_idx
  on public.variant_identifiers (conflict_flag)
  where conflict_flag = true;

-- ---------------------------------------------------------------------------
-- TAPE inventory levels (only for physically stocked releases)
-- ---------------------------------------------------------------------------
create table if not exists public.tape_inventory_levels (
  id uuid primary key default gen_random_uuid(),
  release_variant_id uuid not null references public.release_variants (id) on delete cascade,
  shopify_location_id text not null,
  on_hand integer not null default 0,
  committed integer not null default 0,
  available integer not null default 0,
  po_incoming_confirmed integer not null default 0,
  shopify_incoming_reported integer not null default 0,
  damaged_or_unavailable integer not null default 0,
  last_synced_at timestamptz null,
  pipeline_run_id uuid references public.pipeline_runs (id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
  -- No CHECK (available <= on_hand): Shopify allows negative available when committed > on_hand.
);

comment on table public.tape_inventory_levels is
  'TAPE-owned inventory for physically stocked releases only. Supplier-only releases must not require rows here. Supplier qty must never write these fields.';
comment on column public.tape_inventory_levels.po_incoming_confirmed is
  'PO-owned confirmed incoming units not yet received. Do not auto-sum with shopify_incoming_reported.';
comment on column public.tape_inventory_levels.shopify_incoming_reported is
  'Shopify-reported incoming. May overlap PO quantities.';

create unique index if not exists tape_inventory_levels_release_location_uidx
  on public.tape_inventory_levels (release_variant_id, shopify_location_id);

create index if not exists tape_inventory_levels_location_idx
  on public.tape_inventory_levels (shopify_location_id);

create index if not exists tape_inventory_levels_last_synced_idx
  on public.tape_inventory_levels (last_synced_at desc nulls last);

-- ---------------------------------------------------------------------------
-- Supplier offers (identity: supplier_id + supplier_sku)
-- ---------------------------------------------------------------------------
create table if not exists public.supplier_offers (
  id uuid primary key default gen_random_uuid(),
  supplier_id text not null references public.suppliers (id),
  supplier_sku text not null,
  raw_barcode text null,
  release_variant_id uuid references public.release_variants (id) on delete set null,
  catalog_item_id uuid references public.catalog_items (id) on delete set null,
  availability_status text not null default 'unknown',
  reported_quantity integer null,
  quantity_is_exact boolean not null default false,
  supplier_can_supply boolean null,
  unit_cost numeric(18, 4) null,
  currency text null,
  expected_dispatch_days integer null,
  release_date date null,
  last_seen_at timestamptz null,
  source_feed_at timestamptz null,
  pipeline_completed_at timestamptz null,
  latest_successful_pipeline_run_id uuid references public.pipeline_runs (id) on delete set null,
  availability_confidence numeric(5, 4) null,
  availability_confidence_version text null,
  raw_status_text text null,
  raw_payload jsonb not null default '{}'::jsonb,
  last_changed_at timestamptz null,
  active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint supplier_offers_availability_status_check check (
    availability_status in (
      'in_stock',
      'low_stock',
      'preorder',
      'backorder',
      'unavailable',
      'discontinued',
      'unknown'
    )
  ),
  constraint supplier_offers_reported_quantity_nonneg check (
    reported_quantity is null or reported_quantity >= 0
  ),
  constraint supplier_offers_confidence_range check (
    availability_confidence is null
    or (availability_confidence >= 0 and availability_confidence <= 1)
  ),
  constraint supplier_offers_confidence_version_pair check (
    (availability_confidence is null and availability_confidence_version is null)
    or (availability_confidence is not null and availability_confidence_version is not null)
  ),
  constraint supplier_offers_sku_nonempty check (length(trim(supplier_sku)) > 0)
);

comment on table public.supplier_offers is
  'Supplier commercial offer. Primary identity: (supplier_id, supplier_sku). Barcode is an attribute. Resolves to release_variant_id via supplier_sku_resolutions.';
comment on column public.supplier_offers.supplier_can_supply is
  'Supplier supply capability — distinct from customer_can_purchase.';

create unique index if not exists supplier_offers_supplier_sku_uidx
  on public.supplier_offers (supplier_id, supplier_sku);

create index if not exists supplier_offers_release_variant_id_idx
  on public.supplier_offers (release_variant_id)
  where release_variant_id is not null;

create index if not exists supplier_offers_supplier_id_idx
  on public.supplier_offers (supplier_id);

create index if not exists supplier_offers_raw_barcode_idx
  on public.supplier_offers (raw_barcode)
  where raw_barcode is not null;

create index if not exists supplier_offers_last_seen_idx
  on public.supplier_offers (last_seen_at desc nulls last);

create index if not exists supplier_offers_status_idx
  on public.supplier_offers (availability_status);

-- ---------------------------------------------------------------------------
-- Supplier offer observations (append-only source facts)
-- ---------------------------------------------------------------------------
create table if not exists public.supplier_offer_observations (
  id uuid primary key default gen_random_uuid(),
  supplier_offer_id uuid not null references public.supplier_offers (id) on delete cascade,
  pipeline_run_id uuid references public.pipeline_runs (id) on delete set null,
  observed_at timestamptz not null default now(),
  source_feed_at timestamptz null,
  availability_status text not null,
  reported_quantity integer null,
  quantity_is_exact boolean not null default false,
  supplier_can_supply boolean null,
  unit_cost numeric(18, 4) null,
  currency text null,
  raw_status_text text null,
  raw_payload jsonb not null default '{}'::jsonb,
  dedupe_key text not null,
  created_at timestamptz not null default now(),
  constraint supplier_offer_observations_status_check check (
    availability_status in (
      'in_stock',
      'low_stock',
      'preorder',
      'backorder',
      'unavailable',
      'discontinued',
      'unknown'
    )
  )
);

comment on table public.supplier_offer_observations is
  'Append-only supplier source facts. Unchanged input must not insert a duplicate dedupe_key.';

create unique index if not exists supplier_offer_observations_dedupe_uidx
  on public.supplier_offer_observations (dedupe_key);

create index if not exists supplier_offer_observations_offer_observed_idx
  on public.supplier_offer_observations (supplier_offer_id, observed_at desc);

create index if not exists supplier_offer_observations_pipeline_run_idx
  on public.supplier_offer_observations (pipeline_run_id)
  where pipeline_run_id is not null;

-- ---------------------------------------------------------------------------
-- Supplier SKU → release resolution
-- ---------------------------------------------------------------------------
create table if not exists public.supplier_sku_resolutions (
  id uuid primary key default gen_random_uuid(),
  supplier_id text not null references public.suppliers (id),
  supplier_sku text not null,
  raw_barcode text null,
  resolved_release_variant_id uuid references public.release_variants (id) on delete set null,
  match_method text not null,
  match_confidence numeric(5, 4) null,
  review_status text not null default 'needs_review',
  reviewed_at timestamptz null,
  reviewed_by text null,
  notes text null,
  active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint supplier_sku_resolutions_review_status_check check (
    review_status in ('auto_accepted', 'needs_review', 'rejected', 'manual')
  ),
  constraint supplier_sku_resolutions_confidence_range check (
    match_confidence is null
    or (match_confidence >= 0 and match_confidence <= 1)
  ),
  constraint supplier_sku_resolutions_sku_nonempty check (length(trim(supplier_sku)) > 0)
);

comment on table public.supplier_sku_resolutions is
  'Maps (supplier_id, supplier_sku) to release_variant_id with confidence and review status.';

create unique index if not exists supplier_sku_resolutions_active_sku_uidx
  on public.supplier_sku_resolutions (supplier_id, supplier_sku)
  where active = true;

create index if not exists supplier_sku_resolutions_review_status_idx
  on public.supplier_sku_resolutions (review_status);

create index if not exists supplier_sku_resolutions_release_idx
  on public.supplier_sku_resolutions (resolved_release_variant_id)
  where resolved_release_variant_id is not null;

create index if not exists supplier_sku_resolutions_barcode_idx
  on public.supplier_sku_resolutions (raw_barcode)
  where raw_barcode is not null;

-- ---------------------------------------------------------------------------
-- Purchase orders
-- ---------------------------------------------------------------------------
create table if not exists public.purchase_orders (
  id uuid primary key default gen_random_uuid(),
  supplier_id text not null references public.suppliers (id),
  purchase_order_number text not null,
  status text not null default 'ordered',
  ordered_at date null,
  expected_at date null,
  received_at date null,
  source_filename text null,
  notes text null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint purchase_orders_status_check check (
    status in (
      'draft',
      'ordered',
      'confirmed',
      'partially_received',
      'received',
      'cancelled',
      'delayed'
    )
  )
);

comment on table public.purchase_orders is
  'Purchase orders bridging supplier inventory and TAPE inventory. Stock becomes TAPE-owned only when received.';

create unique index if not exists purchase_orders_supplier_number_uidx
  on public.purchase_orders (supplier_id, purchase_order_number);

create index if not exists purchase_orders_status_idx
  on public.purchase_orders (status);

create table if not exists public.purchase_order_lines (
  id uuid primary key default gen_random_uuid(),
  purchase_order_id uuid not null references public.purchase_orders (id) on delete cascade,
  release_variant_id uuid references public.release_variants (id) on delete set null,
  raw_barcode text null,
  supplier_sku text null,
  title text null,
  quantity_ordered integer not null default 0,
  quantity_confirmed integer not null default 0,
  quantity_received integer not null default 0,
  quantity_cancelled integer not null default 0,
  unit_cost numeric(18, 4) null,
  currency text null,
  expected_at date null,
  received_at date null,
  over_receipt_adjustment boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint purchase_order_lines_qty_nonneg check (
    quantity_ordered >= 0
    and quantity_confirmed >= 0
    and quantity_received >= 0
    and quantity_cancelled >= 0
  ),
  constraint purchase_order_lines_received_vs_ordered check (
    over_receipt_adjustment = true
    or quantity_received <= quantity_ordered
  )
);

comment on table public.purchase_order_lines is
  'PO line quantities. Received must not exceed ordered unless over_receipt_adjustment is set.';

create index if not exists purchase_order_lines_po_id_idx
  on public.purchase_order_lines (purchase_order_id);

create index if not exists purchase_order_lines_release_variant_id_idx
  on public.purchase_order_lines (release_variant_id)
  where release_variant_id is not null;

create index if not exists purchase_order_lines_barcode_idx
  on public.purchase_order_lines (raw_barcode)
  where raw_barcode is not null;

create index if not exists purchase_order_lines_supplier_sku_idx
  on public.purchase_order_lines (supplier_sku)
  where supplier_sku is not null;

-- ---------------------------------------------------------------------------
-- Inventory events
-- ---------------------------------------------------------------------------
create table if not exists public.inventory_events (
  id uuid primary key default gen_random_uuid(),
  event_type text not null,
  release_variant_id uuid references public.release_variants (id) on delete set null,
  supplier_offer_id uuid references public.supplier_offers (id) on delete set null,
  observation_id uuid references public.supplier_offer_observations (id) on delete set null,
  purchase_order_id uuid references public.purchase_orders (id) on delete set null,
  purchase_order_line_id uuid references public.purchase_order_lines (id) on delete set null,
  tape_inventory_level_id uuid references public.tape_inventory_levels (id) on delete set null,
  before_state jsonb not null default '{}'::jsonb,
  after_state jsonb not null default '{}'::jsonb,
  observed_at timestamptz not null default now(),
  pipeline_run_id uuid references public.pipeline_runs (id) on delete set null,
  dedupe_key text not null,
  created_at timestamptz not null default now(),
  constraint inventory_events_type_check check (
    event_type in (
      'tape_stock_synced',
      'tape_stock_received',
      'tape_stock_sold',
      'tape_stock_adjusted',
      'supplier_stock_increased',
      'supplier_stock_decreased',
      'supplier_became_available',
      'supplier_became_unavailable',
      'supplier_price_changed',
      'purchase_order_created',
      'purchase_order_confirmed',
      'purchase_order_delayed',
      'purchase_order_partially_received',
      'purchase_order_received',
      'purchase_order_cancelled'
    )
  )
);

comment on table public.inventory_events is
  'Interpreted material inventory changes. Link observation_id when caused by a supplier observation.';

create unique index if not exists inventory_events_dedupe_uidx
  on public.inventory_events (dedupe_key);

create index if not exists inventory_events_release_observed_idx
  on public.inventory_events (release_variant_id, observed_at desc)
  where release_variant_id is not null;

create index if not exists inventory_events_type_observed_idx
  on public.inventory_events (event_type, observed_at desc);

create index if not exists inventory_events_observation_idx
  on public.inventory_events (observation_id)
  where observation_id is not null;

create index if not exists inventory_events_supplier_offer_idx
  on public.inventory_events (supplier_offer_id)
  where supplier_offer_id is not null;

create index if not exists inventory_events_pipeline_run_idx
  on public.inventory_events (pipeline_run_id)
  where pipeline_run_id is not null;

commit;
