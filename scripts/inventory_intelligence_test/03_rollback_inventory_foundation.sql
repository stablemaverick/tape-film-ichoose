-- Rollback inventory intelligence foundation tables (temporary test projects).
-- Does NOT drop bootstrap stubs (films / catalog_items / pipeline_runs) by default.
-- Use 04_rollback_bootstrap_stubs.sql only on disposable temp projects.

begin;

drop table if exists public.inventory_events cascade;
drop table if exists public.purchase_order_lines cascade;
drop table if exists public.purchase_orders cascade;
drop table if exists public.supplier_sku_resolutions cascade;
drop table if exists public.supplier_offer_observations cascade;
drop table if exists public.supplier_offers cascade;
drop table if exists public.tape_inventory_levels cascade;
drop table if exists public.variant_identifiers cascade;
drop table if exists public.release_shopify_listings cascade;
drop table if exists public.release_variants cascade;
drop table if exists public.suppliers cascade;

commit;
