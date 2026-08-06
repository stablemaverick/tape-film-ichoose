# Production deployment checklist — inventory intelligence

Use **only after** temporary-environment validation has passed  
(`docs/inventory-intelligence/temporary-test-environment.md`).

**Do not use this checklist in the current step.** Migration must not be applied to production until this list is explicitly followed in a planned change window.

## Preconditions

- [ ] Temporary Supabase / local validation green (`verify_invariants.py` exit 0)
- [ ] Unit tests green (`test_availability_rules`, `test_inventory_invariant_rules`, `test_inventory_dual_write*`)
- [ ] Agent consumer tests green (`npm run test:agent`) — no consumer switch intended
- [ ] Dual-write flags confirmed **OFF** in production env (`INVENTORY_DUAL_WRITE_*=0` or unset)
- [ ] Rollback SQL reviewed (`phase-3a-rollback.md` / `03_rollback_inventory_foundation.sql`)
- [ ] Change window + owner agreed; Supabase backup / point-in-time recovery known

## Deploy foundation (additive only)

- [ ] Apply **only** `supabase/migrations/20260806120000_inventory_intelligence_foundation.sql` to production
- [ ] Confirm tables exist via `scripts/observability/inventory_dual_write_verify.py --env-file .env.prod` with flags OFF
- [ ] Confirm counts on `catalog_items` / `shopify_listings` / staging tables unchanged
- [ ] Confirm no dual-write rows appear while flags OFF (`release_variants` count = 0 or seed-free)

## Dual-write enablement (separate step — not automatic)

- [ ] Enable on a **non-production** DB first (when one exists), not production
- [ ] When enabling production dual-writes later: one flag stream at a time  
  (`SHOPIFY` → `SUPPLIER` → `PO`) under `INVENTORY_DUAL_WRITE_ENABLED=1`
- [ ] Monitor logs for `WARN: inventory dual-write` without pipeline failures
- [ ] Run verify + reconciliation; review `needs_review` resolutions
- [ ] Keep agent / admin / storefront on `catalog_items` until Phase 3c explicitly approved

## Explicit non-goals until separately approved

- [ ] No Phase 4 backfill
- [ ] No unified availability consumer switch
- [ ] No supplier quantities written to Shopify inventory APIs
- [ ] No enabling dual-write by default in cron wrappers

## Rollback (production)

1. Set all `INVENTORY_DUAL_WRITE_*` to `0`
2. Confirm operational pipelines healthy on existing tables
3. If foundation tables must be removed and are unused: apply drop order in `phase-3a-rollback.md`
4. Do **not** drop `catalog_items` / `films` / `shopify_listings`
