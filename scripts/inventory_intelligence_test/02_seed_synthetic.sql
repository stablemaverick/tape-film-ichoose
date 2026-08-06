-- Deterministic synthetic seed for inventory intelligence Phase 3a/3b validation.
-- SYNTHETIC DATA ONLY — no production titles, barcodes, customers, or supplier exports.
--
-- Prerequisites:
--   1) 00_bootstrap_prereq_tables.sql
--   2) supabase/migrations/20260806120000_inventory_intelligence_foundation.sql
--
-- Idempotent: deletes prior seed rows tagged with seed_marker, then inserts fixed UUIDs.

begin;

-- ---------------------------------------------------------------------------
-- Stable synthetic identifiers
-- ---------------------------------------------------------------------------
-- Films / catalog stubs
--   film:     11111111-1111-4111-a111-111111111101
-- Releases:
--   A on-hand+:        aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1
--   B committed:       aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2
--   C neg available:   aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3
--   D dual supplier:   aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa4
--   E supplier-only:   aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa5
--   F1 ambiguous:      aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa6
--   F2 ambiguous:      aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa7
-- Synthetic barcodes: 9900000000001 .. 9900000000009, shared 9900000000099

-- Clean previous seed (child tables first)
delete from public.inventory_events
 where dedupe_key like 'seed:%' or release_variant_id::text like 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa%';
delete from public.supplier_offer_observations
 where dedupe_key like 'seed:%'
    or supplier_offer_id in (
      select id from public.supplier_offers where supplier_sku like 'SYN-%'
    );
delete from public.purchase_order_lines
 where purchase_order_id in (
   select id from public.purchase_orders where purchase_order_number like 'SYN-PO-%'
 );
delete from public.purchase_orders where purchase_order_number like 'SYN-PO-%';
delete from public.supplier_sku_resolutions where supplier_sku like 'SYN-%';
delete from public.supplier_offers where supplier_sku like 'SYN-%';
delete from public.tape_inventory_levels
 where release_variant_id::text like 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa%';
delete from public.release_shopify_listings
 where shop = 'synthetic-test.myshopify.com';
delete from public.variant_identifiers
 where id_value like '99000000000%' or id_value = '9900000000099';
delete from public.release_variants
 where id::text like 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa%';
delete from public.catalog_items where id = '22222222-2222-4222-8222-222222222201';
delete from public.films where id = '11111111-1111-4111-a111-111111111101';
delete from public.pipeline_runs where id = '33333333-3333-4333-8333-333333333301';

insert into public.films (id, title)
values ('11111111-1111-4111-a111-111111111101', 'Synthetic Film Fixture')
on conflict (id) do update set title = excluded.title;

insert into public.catalog_items (id, title, barcode, supplier, active)
values (
  '22222222-2222-4222-8222-222222222201',
  'Synthetic Catalog Fixture',
  '9900000000001',
  'Tape Film',
  true
)
on conflict (id) do update set title = excluded.title;

insert into public.pipeline_runs (id, pipeline_type, completed)
values ('33333333-3333-4333-8333-333333333301', 'inventory_test_seed', true)
on conflict (id) do nothing;

-- Ensure suppliers exist (migration also seeds these)
insert into public.suppliers (id, display_name, priority, active)
values
  ('moovies', 'Moovies', 1, true),
  ('lasgo', 'Lasgo', 2, true),
  ('tape_film', 'Tape Film', 0, true)
on conflict (id) do nothing;

-- ---------------------------------------------------------------------------
-- Release variants
-- ---------------------------------------------------------------------------
insert into public.release_variants (
  id, film_id, catalog_item_id, primary_barcode, title, format, publication_status, active
) values
  ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1', '11111111-1111-4111-a111-111111111101',
   '22222222-2222-4222-8222-222222222201', '9900000000001',
   'Synthetic Release Alpha', '4K', 'published', true),
  ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2', '11111111-1111-4111-a111-111111111101',
   null, '9900000000002',
   'Synthetic Release Beta', 'Blu-ray', 'published', true),
  ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3', '11111111-1111-4111-a111-111111111101',
   null, '9900000000003',
   'Synthetic Release Gamma Preorder', '4K', 'published', true),
  ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa4', '11111111-1111-4111-a111-111111111101',
   null, '9900000000004',
   'Synthetic Release Delta Dual Supplier', '4K', 'published', true),
  ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa5', '11111111-1111-4111-a111-111111111101',
   null, '9900000000005',
   'Synthetic Release Epsilon Supplier Only', 'Blu-ray', 'supplier_only', true),
  ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa6', null, null, '9900000000099',
   'Synthetic Ambiguous One', '4K', 'candidate', true),
  ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa7', null, null, '9900000000099',
   'Synthetic Ambiguous Two', '4K', 'candidate', true);

-- Identifiers (shared barcode on F1/F2 flagged as conflict)
insert into public.variant_identifiers (
  release_variant_id, id_type, id_value, source, is_primary, is_valid, conflict_flag
) values
  ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1', 'barcode', '9900000000001', 'seed', true, true, false),
  ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2', 'barcode', '9900000000002', 'seed', true, true, false),
  ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3', 'barcode', '9900000000003', 'seed', true, true, false),
  ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa4', 'barcode', '9900000000004', 'seed', true, true, false),
  ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa5', 'barcode', '9900000000005', 'seed', true, true, false),
  ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa6', 'barcode', '9900000000099', 'seed', true, true, true),
  ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa7', 'barcode', '9900000000099', 'seed', true, true, true);

-- Shopify channels for A–D only (not E supplier-only; not ambiguous unless needed)
insert into public.release_shopify_listings (
  release_variant_id, shop, shopify_product_id, shopify_variant_id,
  shopify_inventory_item_id, is_primary
) values
  ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1', 'synthetic-test.myshopify.com',
   'gid://shopify/Product/9100000001', 'gid://shopify/ProductVariant/9200000001',
   'gid://shopify/InventoryItem/9300000001', true),
  ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2', 'synthetic-test.myshopify.com',
   'gid://shopify/Product/9100000002', 'gid://shopify/ProductVariant/9200000002',
   'gid://shopify/InventoryItem/9300000002', true),
  ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3', 'synthetic-test.myshopify.com',
   'gid://shopify/Product/9100000003', 'gid://shopify/ProductVariant/9200000003',
   'gid://shopify/InventoryItem/9300000003', true),
  ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa4', 'synthetic-test.myshopify.com',
   'gid://shopify/Product/9100000004', 'gid://shopify/ProductVariant/9200000004',
   'gid://shopify/InventoryItem/9300000004', true);

-- ---------------------------------------------------------------------------
-- TAPE inventory (stocked / published only — NOT supplier-only E)
-- ---------------------------------------------------------------------------
insert into public.tape_inventory_levels (
  release_variant_id, shopify_location_id,
  on_hand, committed, available,
  po_incoming_confirmed, shopify_incoming_reported, damaged_or_unavailable,
  last_synced_at, pipeline_run_id
) values
  -- Scenario: positive on-hand
  ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1', 'gid://shopify/Location/9400000001',
   5, 0, 5, 0, 0, 0, now(), '33333333-3333-4333-8333-333333333301'),
  -- Scenario: committed inventory
  ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2', 'gid://shopify/Location/9400000001',
   3, 1, 2, 0, 0, 0, now(), '33333333-3333-4333-8333-333333333301'),
  -- Scenario: oversold / preorder negative available
  ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3', 'gid://shopify/Location/9400000001',
   0, 2, -2, 0, 0, 0, now(), '33333333-3333-4333-8333-333333333301'),
  -- Dual-supplier release also stocked at TAPE
  ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa4', 'gid://shopify/Location/9400000001',
   1, 0, 1, 4, 0, 0, now(), '33333333-3333-4333-8333-333333333301');

-- ---------------------------------------------------------------------------
-- Supplier offers
-- ---------------------------------------------------------------------------
insert into public.supplier_offers (
  id, supplier_id, supplier_sku, raw_barcode, release_variant_id,
  availability_status, reported_quantity, quantity_is_exact, supplier_can_supply,
  unit_cost, currency, last_seen_at, source_feed_at, pipeline_completed_at,
  latest_successful_pipeline_run_id, availability_confidence, availability_confidence_version,
  raw_status_text, raw_payload, active
) values
  -- Dual supplier on release D — exact numeric qty (Moovies)
  ('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb1', 'moovies', 'SYN-MOOV-DELTA', '9900000000004',
   'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa4',
   'in_stock', 14, true, true,
   12.50, 'GBP', now(), now(), now(),
   '33333333-3333-4333-8333-333333333301', 0.95, 'v1',
   null, '{"seed": true, "scenario": "dual_supplier_exact_qty"}'::jsonb, true),
  -- Dual supplier on release D — Lasgo
  ('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb2', 'lasgo', 'SYN-LASGO-DELTA', '9900000000004',
   'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa4',
   'low_stock', 2, true, true,
   11.00, 'GBP', now(), now(), now(),
   '33333333-3333-4333-8333-333333333301', 0.90, 'v1',
   null, '{"seed": true, "scenario": "dual_supplier_lasgo"}'::jsonb, true),
  -- Supplier-only release E
  ('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb3', 'moovies', 'SYN-MOOV-EPS', '9900000000005',
   'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa5',
   'in_stock', 8, true, true,
   9.99, 'GBP', now(), now(), now(),
   '33333333-3333-4333-8333-333333333301', 0.92, 'v1',
   null, '{"seed": true, "scenario": "supplier_only"}'::jsonb, true),
  -- Missing barcode (SKU identity only)
  ('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb4', 'moovies', 'SYN-MOOV-NOBARCODE', null,
   null,
   'in_stock', 3, true, true,
   7.00, 'GBP', now(), now(), now(),
   '33333333-3333-4333-8333-333333333301', 0.70, 'v1',
   null, '{"seed": true, "scenario": "missing_barcode"}'::jsonb, true),
  -- Two offers sharing barcode 9900000000006 (different SKUs) — attached loosely
  ('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb5', 'moovies', 'SYN-MOOV-SHARE-A', '9900000000006',
   null,
   'in_stock', 1, true, true,
   8.00, 'GBP', now(), now(), now(),
   '33333333-3333-4333-8333-333333333301', 0.60, 'v1',
   null, '{"seed": true, "scenario": "shared_barcode_a"}'::jsonb, true),
  ('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb6', 'lasgo', 'SYN-LASGO-SHARE-B', '9900000000006',
   null,
   'in_stock', 5, true, true,
   8.50, 'GBP', now(), now(), now(),
   '33333333-3333-4333-8333-333333333301', 0.60, 'v1',
   null, '{"seed": true, "scenario": "shared_barcode_b"}'::jsonb, true),
  -- Non-numeric availability text, null quantity
  ('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb7', 'lasgo', 'SYN-LASGO-TEXTSTATUS', '9900000000007',
   null,
   'in_stock', null, false, true,
   10.00, 'GBP', now(), now(), now(),
   '33333333-3333-4333-8333-333333333301', 0.55, 'v1',
   'In stock', '{"seed": true, "scenario": "non_numeric_status"}'::jsonb, true),
  -- Unresolved supplier SKU (needs review)
  ('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb8', 'moovies', 'SYN-MOOV-UNRESOLVED', '9900000000008',
   null,
   'unknown', null, false, null,
   null, 'GBP', now(), now(), now(),
   null, 0.20, 'v1',
   'Unknown', '{"seed": true, "scenario": "unresolved_sku"}'::jsonb, true);

-- Resolutions
insert into public.supplier_sku_resolutions (
  supplier_id, supplier_sku, raw_barcode, resolved_release_variant_id,
  match_method, match_confidence, review_status, notes, active
) values
  ('moovies', 'SYN-MOOV-DELTA', '9900000000004', 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa4',
   'barcode_exact', 0.98, 'auto_accepted', 'seed dual supplier', true),
  ('lasgo', 'SYN-LASGO-DELTA', '9900000000004', 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa4',
   'barcode_exact', 0.98, 'auto_accepted', 'seed dual supplier', true),
  ('moovies', 'SYN-MOOV-EPS', '9900000000005', 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa5',
   'created_supplier_only', 1.0, 'auto_accepted', 'seed supplier only', true),
  -- Ambiguous barcode maps to two releases
  ('moovies', 'SYN-MOOV-AMBIG', '9900000000099', null,
   'barcode_ambiguous', 0.40, 'needs_review', 'seed ambiguous identity', true),
  -- Unresolved
  ('moovies', 'SYN-MOOV-UNRESOLVED', '9900000000008', null,
   'unmatched', 0.0, 'needs_review', 'seed unresolved sku', true);

-- Observations (material facts) + duplicate-ready fingerprint for dedupe test
insert into public.supplier_offer_observations (
  id, supplier_offer_id, pipeline_run_id, observed_at, source_feed_at,
  availability_status, reported_quantity, quantity_is_exact, supplier_can_supply,
  unit_cost, currency, raw_status_text, raw_payload, dedupe_key
) values
  ('cccccccc-cccc-4ccc-8ccc-ccccccccccc1',
   'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb1',
   '33333333-3333-4333-8333-333333333301', now(), now(),
   'in_stock', 14, true, true,
   12.50, 'GBP', null,
   '{"seed": true}'::jsonb,
   'seed:obs:bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb1:in_stock|14|1|1|12.5000|GBP');

-- ---------------------------------------------------------------------------
-- Purchase orders
-- ---------------------------------------------------------------------------
insert into public.purchase_orders (
  id, supplier_id, purchase_order_number, status, ordered_at, expected_at, source_filename
) values
  ('dddddddd-dddd-4ddd-8ddd-ddddddddddd1', 'moovies', 'SYN-PO-OPEN-001', 'confirmed',
   current_date - 7, current_date + 14, 'synthetic_po_seed.csv'),
  ('dddddddd-dddd-4ddd-8ddd-ddddddddddd2', 'moovies', 'SYN-PO-PARTIAL-002', 'partially_received',
   current_date - 21, current_date - 3, 'synthetic_po_seed.csv');

insert into public.purchase_order_lines (
  purchase_order_id, release_variant_id, raw_barcode, supplier_sku, title,
  quantity_ordered, quantity_confirmed, quantity_received, quantity_cancelled,
  unit_cost, currency, over_receipt_adjustment
) values
  -- Open PO: ordered + confirmed, not received
  ('dddddddd-dddd-4ddd-8ddd-ddddddddddd1', 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa4',
   '9900000000004', 'SYN-MOOV-DELTA', 'Synthetic Release Delta Dual Supplier',
   10, 10, 0, 0, 12.50, 'GBP', false),
  -- Partially received
  ('dddddddd-dddd-4ddd-8ddd-ddddddddddd2', 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1',
   '9900000000001', 'SYN-MOOV-ALPHA', 'Synthetic Release Alpha',
   6, 6, 2, 0, 12.00, 'GBP', false);

-- Sample inventory event linked to observation
insert into public.inventory_events (
  event_type, release_variant_id, supplier_offer_id, observation_id,
  before_state, after_state, observed_at, pipeline_run_id, dedupe_key
) values (
  'supplier_became_available',
  'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa4',
  'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb1',
  'cccccccc-cccc-4ccc-8ccc-ccccccccccc1',
  '{"availability_status": "unavailable"}'::jsonb,
  '{"availability_status": "in_stock", "reported_quantity": 14}'::jsonb,
  now(),
  '33333333-3333-4333-8333-333333333301',
  'seed:evt:supplier_became_available:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa4'
);

commit;
