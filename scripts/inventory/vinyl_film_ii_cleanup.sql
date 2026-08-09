-- Vinyl soundtrack Film-II cleanup (APPROVED 2026-08-09)
-- Project: zdvjokkslhpoftimvdis
-- Does NOT mutate Shopify Admin products/inventory/prices or supplier_offers.
-- Keeps music_vinyl release_variants for future Music II; removes Film-II channels/levels/events.

begin;

-- A) Additive domain fields
alter table public.shopify_listings add column if not exists media_format text;
alter table public.shopify_listings add column if not exists collection_handles text;
comment on column public.shopify_listings.media_format is
  'Shopify metafield custom.format snapshot (e.g. Vinyl, Blu-ray)';
comment on column public.shopify_listings.collection_handles is
  'Comma-separated Shopify collection handles (e.g. soundtracks)';

alter table public.release_variants add column if not exists product_domain text;
comment on column public.release_variants.product_domain is
  'Inventory domain: film (null/default) | music_vinyl | future domains';

-- B) Guard: refuse if supplier offers linked
do $$
declare n int;
begin
  select count(*) into n from public.supplier_offers
  where release_variant_id in (
  'ccd40271-3c6f-4643-b0e0-eee4c9d657b0'::uuid,
  '11684a1a-df0b-4b24-9f7e-31c23bba4db5'::uuid,
  '8063198a-57fc-42a1-ac3d-842e95b85d82'::uuid,
  'dc29eddb-581a-4c3f-9b5d-dc5b363a1bbb'::uuid,
  '7f09f511-a08a-4dbb-bc08-114b806ffb5b'::uuid,
  '46f859d2-5912-4bab-bfb1-9e5d17068775'::uuid,
  '4e1ed910-b696-4c04-be55-e29908701805'::uuid,
  '0462db25-6721-4d20-82a7-d5e74715fa2c'::uuid,
  '89cd90a0-2b24-4e7b-a760-c9500298cff9'::uuid,
  '65180181-0cad-422a-bc14-e2ce19e53118'::uuid,
  'aba91518-7082-43cb-91f9-1c1501209d8f'::uuid,
  'fd302079-5170-414b-949a-1930822b9332'::uuid,
  '34c917fe-35cd-40e3-80b1-0dc036431687'::uuid,
  '55da2e66-8a86-4bbe-b92f-2dda92486e9a'::uuid,
  'e56dc090-8008-436a-8b29-75629ffebc09'::uuid,
  '5d26c7b6-073b-4d6b-b216-5cf29b1d78ef'::uuid,
  '90062cf5-2068-4b40-8eae-90a5dc356b0d'::uuid,
  '11148963-b8b3-4856-b0cf-9d59eed04f97'::uuid,
  '68e9955a-82a0-4c0a-aebe-ac8835969e84'::uuid,
  'e215bb9f-ff36-4559-8d0e-d6ee13014a9b'::uuid,
  'f67732c5-4373-42a8-ae47-e4ef94c4e396'::uuid,
  'cf44b7b8-0485-4e7b-8584-156f071a87a8'::uuid,
  '005df178-e725-43ea-a155-bbe7b8bac906'::uuid,
  'a2a251ac-c90e-434d-988f-243f24e8b385'::uuid,
  'e0bbd5b0-82c4-4644-bc61-1786dd7fea06'::uuid,
  '8379f0af-c6ed-47ef-9e52-75bad0d25dce'::uuid,
  '3494aa67-5c34-421e-a502-57433ffc5f7b'::uuid,
  'a0420214-79c5-4c10-9c9c-8a50747ad7ef'::uuid,
  '706116c9-f917-43e8-87d8-326f112c60e1'::uuid
  );
  if n <> 0 then
    raise exception 'ABORT: % supplier_offers linked to cleanup candidates', n;
  end if;
end $$;

-- C) Explicit music domain (keep releases for future Music II)
update public.release_variants
set
  product_domain = 'music_vinyl',
  format = coalesce(nullif(btrim(format), ''), 'Vinyl'),
  updated_at = now()
where id in (
  'ccd40271-3c6f-4643-b0e0-eee4c9d657b0'::uuid,
  '11684a1a-df0b-4b24-9f7e-31c23bba4db5'::uuid,
  '8063198a-57fc-42a1-ac3d-842e95b85d82'::uuid,
  'dc29eddb-581a-4c3f-9b5d-dc5b363a1bbb'::uuid,
  '7f09f511-a08a-4dbb-bc08-114b806ffb5b'::uuid,
  '46f859d2-5912-4bab-bfb1-9e5d17068775'::uuid,
  '4e1ed910-b696-4c04-be55-e29908701805'::uuid,
  '0462db25-6721-4d20-82a7-d5e74715fa2c'::uuid,
  '89cd90a0-2b24-4e7b-a760-c9500298cff9'::uuid,
  '65180181-0cad-422a-bc14-e2ce19e53118'::uuid,
  'aba91518-7082-43cb-91f9-1c1501209d8f'::uuid,
  'fd302079-5170-414b-949a-1930822b9332'::uuid,
  '34c917fe-35cd-40e3-80b1-0dc036431687'::uuid,
  '55da2e66-8a86-4bbe-b92f-2dda92486e9a'::uuid,
  'e56dc090-8008-436a-8b29-75629ffebc09'::uuid,
  '5d26c7b6-073b-4d6b-b216-5cf29b1d78ef'::uuid,
  '90062cf5-2068-4b40-8eae-90a5dc356b0d'::uuid,
  '11148963-b8b3-4856-b0cf-9d59eed04f97'::uuid,
  '68e9955a-82a0-4c0a-aebe-ac8835969e84'::uuid,
  'e215bb9f-ff36-4559-8d0e-d6ee13014a9b'::uuid,
  'f67732c5-4373-42a8-ae47-e4ef94c4e396'::uuid,
  'cf44b7b8-0485-4e7b-8584-156f071a87a8'::uuid,
  '005df178-e725-43ea-a155-bbe7b8bac906'::uuid,
  'a2a251ac-c90e-434d-988f-243f24e8b385'::uuid,
  'e0bbd5b0-82c4-4644-bc61-1786dd7fea06'::uuid,
  '8379f0af-c6ed-47ef-9e52-75bad0d25dce'::uuid,
  '3494aa67-5c34-421e-a502-57433ffc5f7b'::uuid,
  'a0420214-79c5-4c10-9c9c-8a50747ad7ef'::uuid,
  '706116c9-f917-43e8-87d8-326f112c60e1'::uuid
);

-- D) Remove Film-II projections only (audited IDs)
delete from public.tape_inventory_levels
where id in (
  '2b7a0bdf-e7e4-4eac-8e76-643c70e1295c'::uuid,
  '7585da4f-df1f-4f60-bf8a-709998228383'::uuid,
  '80e2efdd-7589-4752-9406-f6113e590a76'::uuid,
  '5a563061-a818-4635-b6a0-c077eff29521'::uuid,
  'fd8fbd02-5e2a-4f1e-955c-543622a847dd'::uuid,
  '75b44aaa-6191-44f3-9538-d89e1814d8c6'::uuid,
  '8a015f51-5a9e-4e88-afa2-5a4dd0ee0686'::uuid,
  '559f7a77-8632-4fce-80b1-edccd038e782'::uuid,
  '0b298604-ed40-46eb-90ea-22ce9b34f6cd'::uuid,
  '65e73412-5312-4a27-ae48-4ee46f6b4461'::uuid,
  '845aa237-d5cd-4e69-9a7e-432846e645b9'::uuid,
  'ac296787-3ba3-4c6a-9ef9-80d4d601539f'::uuid,
  'ae76b656-94de-430a-842e-26639e3338df'::uuid,
  '881a80ad-6f40-468e-b282-c38726f237c3'::uuid,
  'bb4d177a-095c-4907-bb61-19aeadca4a1d'::uuid,
  'c6eccbf7-f1ec-456f-81db-8be053c55f77'::uuid,
  '658f8278-1fbc-4316-b7e5-e16f0b4158f6'::uuid,
  '044c61eb-beaa-4717-842a-057d31995b5d'::uuid,
  'e3f643e1-f790-432c-a1ed-d96371104428'::uuid,
  '1db5e1ec-04ef-4d56-86b7-90cf3410826f'::uuid,
  'c1807100-97ad-4817-828b-826a02a0814a'::uuid,
  '1cd44b39-ce1f-4c53-8c01-c6c02ef60de2'::uuid,
  '274f4b4a-b2c4-4c4b-a490-47574182382a'::uuid,
  'b7e06120-3437-4f94-8052-f0020f598e38'::uuid,
  'bb1dc572-985e-4198-baef-df76f734ff5d'::uuid,
  '3d954522-6a99-4384-aa17-881d05334afd'::uuid,
  '3ec8834a-d040-43e0-81a2-d237c32ad0bc'::uuid,
  '11fecc80-8d07-40d2-92ef-57f713b7e6ed'::uuid,
  '5ddce233-a438-42ef-83ab-8bbdfc904e89'::uuid
);

delete from public.release_shopify_listings
where id in (
  '925342ee-df12-48f3-9a93-e0a0cfafd01c'::uuid,
  '1d375527-d73e-42bd-b6d0-d9d523dcc871'::uuid,
  'b69f644c-7548-43f1-a315-a35450b795da'::uuid,
  '4084c4f5-c32f-40ee-ad80-570d05ad6441'::uuid,
  'd8aef894-350a-4bcf-869e-99987db1677b'::uuid,
  '707e233e-dc88-41c5-824a-ffb55c15338c'::uuid,
  'ed667bae-f169-4d1e-a85f-3af5b37c5d9e'::uuid,
  'bb6303df-fe64-4b33-ae4f-7bf80423517e'::uuid,
  '3bce3069-3a10-41bf-9503-b52c6ce41ad7'::uuid,
  'cf90665b-f343-46e4-b3e4-51f54bf88285'::uuid,
  '6b2551e5-801c-48de-8764-1412ae120a7c'::uuid,
  '3c4d6774-7c01-4b18-803c-171d7cac6817'::uuid,
  '0e6ddef3-af93-4ce8-80d6-2231a0d300c4'::uuid,
  'e8ffc149-e4fe-4421-baef-47f5594394d2'::uuid,
  '3c53b044-fc2e-4fe6-905c-e947fe0deec3'::uuid,
  '98953709-1177-4d43-8058-f8ef680aca16'::uuid,
  '6e39a101-1226-4fbc-84f3-6af028d2c324'::uuid,
  '052552eb-a09b-439c-99e9-374cd89ff3be'::uuid,
  'e3f41bed-591b-43c5-bcba-52fd1abc7cbd'::uuid,
  '67dbee67-444d-41c6-bff6-dc22d8a48594'::uuid,
  'edb31cca-7929-42d3-a215-490217bf7b2e'::uuid,
  '5eb1f4b9-4796-42e0-b9d5-a9a53ee334aa'::uuid,
  'f77ada54-a7a4-4313-9a55-5103105d68db'::uuid,
  '37b634d9-9e9e-4517-8390-17884b59e79d'::uuid,
  '71b07c58-5887-4362-b7bd-1a900da01f2b'::uuid,
  '4abc3b99-2006-41fa-8563-5fcf6ef86bdb'::uuid,
  'cef67cc9-1cde-4c5a-a1af-89bd5228e9c1'::uuid,
  'fcbef371-c638-470a-883a-259b09619076'::uuid,
  'ef3e25d3-bc61-40d7-98db-3d49fa0f192c'::uuid
);

-- Film-II events created for these releases (tape_stock_synced from Shopify II)
delete from public.inventory_events
where release_variant_id in (
  'ccd40271-3c6f-4643-b0e0-eee4c9d657b0'::uuid,
  '11684a1a-df0b-4b24-9f7e-31c23bba4db5'::uuid,
  '8063198a-57fc-42a1-ac3d-842e95b85d82'::uuid,
  'dc29eddb-581a-4c3f-9b5d-dc5b363a1bbb'::uuid,
  '7f09f511-a08a-4dbb-bc08-114b806ffb5b'::uuid,
  '46f859d2-5912-4bab-bfb1-9e5d17068775'::uuid,
  '4e1ed910-b696-4c04-be55-e29908701805'::uuid,
  '0462db25-6721-4d20-82a7-d5e74715fa2c'::uuid,
  '89cd90a0-2b24-4e7b-a760-c9500298cff9'::uuid,
  '65180181-0cad-422a-bc14-e2ce19e53118'::uuid,
  'aba91518-7082-43cb-91f9-1c1501209d8f'::uuid,
  'fd302079-5170-414b-949a-1930822b9332'::uuid,
  '34c917fe-35cd-40e3-80b1-0dc036431687'::uuid,
  '55da2e66-8a86-4bbe-b92f-2dda92486e9a'::uuid,
  'e56dc090-8008-436a-8b29-75629ffebc09'::uuid,
  '5d26c7b6-073b-4d6b-b216-5cf29b1d78ef'::uuid,
  '90062cf5-2068-4b40-8eae-90a5dc356b0d'::uuid,
  '11148963-b8b3-4856-b0cf-9d59eed04f97'::uuid,
  '68e9955a-82a0-4c0a-aebe-ac8835969e84'::uuid,
  'e215bb9f-ff36-4559-8d0e-d6ee13014a9b'::uuid,
  'f67732c5-4373-42a8-ae47-e4ef94c4e396'::uuid,
  'cf44b7b8-0485-4e7b-8584-156f071a87a8'::uuid,
  '005df178-e725-43ea-a155-bbe7b8bac906'::uuid,
  'a2a251ac-c90e-434d-988f-243f24e8b385'::uuid,
  'e0bbd5b0-82c4-4644-bc61-1786dd7fea06'::uuid,
  '8379f0af-c6ed-47ef-9e52-75bad0d25dce'::uuid,
  '3494aa67-5c34-421e-a502-57433ffc5f7b'::uuid,
  'a0420214-79c5-4c10-9c9c-8a50747ad7ef'::uuid,
  '706116c9-f917-43e8-87d8-326f112c60e1'::uuid
)
and event_type in ('tape_stock_synced');

-- E) Keep release_variants + variant_identifiers (music_vinyl domain).
-- Do NOT delete release_variants: they are now explicit music_vinyl records.

commit;

-- F) Verification queries (run after commit)
-- select count(*) from release_shopify_listings;  -- expect 637
-- select count(*) from tape_inventory_levels;     -- expect 637
-- select count(*) from release_variants where product_domain = ''music_vinyl''; -- expect 29
