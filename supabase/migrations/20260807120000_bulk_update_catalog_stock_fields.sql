-- Narrow stock-sync bulk updater for catalog_items commercial fields.
-- Join key: catalog_items.id (same identity as apply_stock_sync_row_updates).
-- Does NOT create missing rows. Does NOT accept arbitrary columns.
-- Payload is a JSON array of objects with fixed keys only.

create or replace function public.bulk_update_catalog_stock_fields(payload jsonb)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  attempted integer := 0;
  updated_count integer := 0;
begin
  if payload is null or jsonb_typeof(payload) <> 'array' then
    return jsonb_build_object('attempted', 0, 'updated', 0);
  end if;

  attempted := jsonb_array_length(payload);
  if attempted = 0 then
    return jsonb_build_object('attempted', 0, 'updated', 0);
  end if;

  with src as (
    select
      (r.elem->>'id')::uuid as id,
      -- catalog_items.supplier_stock_status is text (numeric strings in practice)
      case
        when r.elem ? 'supplier_stock_status'
          then coalesce(nullif(r.elem->>'supplier_stock_status', ''), '0')
        else '0'
      end as supplier_stock_status,
      nullif(r.elem->>'availability_status', '') as availability_status,
      case
        when r.elem ? 'cost_price' and nullif(r.elem->>'cost_price', '') is not null
          then (r.elem->>'cost_price')::numeric
        else null
      end as cost_price,
      case
        when r.elem ? 'calculated_sale_price'
             and nullif(r.elem->>'calculated_sale_price', '') is not null
          then (r.elem->>'calculated_sale_price')::numeric
        else null
      end as calculated_sale_price,
      nullif(r.elem->>'supplier_sku', '') as supplier_sku,
      case
        when r.elem ? 'supplier_last_seen_at'
             and nullif(r.elem->>'supplier_last_seen_at', '') is not null
          then (r.elem->>'supplier_last_seen_at')::timestamptz
        else null
      end as supplier_last_seen_at
    from jsonb_array_elements(payload) as r(elem)
    where (r.elem->>'id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ),
  upd as (
    update public.catalog_items c
    set
      supplier_stock_status = src.supplier_stock_status,
      availability_status = src.availability_status,
      cost_price = src.cost_price,
      calculated_sale_price = src.calculated_sale_price,
      supplier_sku = src.supplier_sku,
      supplier_last_seen_at = src.supplier_last_seen_at
    from src
    where c.id = src.id
    returning c.id
  )
  select count(*)::integer into updated_count from upd;

  return jsonb_build_object('attempted', attempted, 'updated', updated_count);
end;
$$;

comment on function public.bulk_update_catalog_stock_fields(jsonb) is
  'Stock-sync only: bulk-update catalog_items commercial fields by id. service_role only.';

revoke all on function public.bulk_update_catalog_stock_fields(jsonb) from public;
revoke all on function public.bulk_update_catalog_stock_fields(jsonb) from anon;
revoke all on function public.bulk_update_catalog_stock_fields(jsonb) from authenticated;
grant execute on function public.bulk_update_catalog_stock_fields(jsonb) to service_role;
