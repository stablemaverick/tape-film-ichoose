# Inventory intelligence — temporary DB test kit

Synthetic-only validation for Phase 3a/3b without a production clone.

See full guide: [`docs/inventory-intelligence/temporary-test-environment.md`](../../docs/inventory-intelligence/temporary-test-environment.md)

## Quick path (temporary Supabase SQL editor)

1. Create a **new** Supabase project (not production).
2. Run `00_bootstrap_prereq_tables.sql`
3. Run `../../supabase/migrations/20260806120000_inventory_intelligence_foundation.sql`
4. Run `02_seed_synthetic.sql`
5. `venv/bin/python verify_invariants.py --env-file ../../.env.inventory-test`

## Rollback

1. `03_rollback_inventory_foundation.sql`
2. Optional disposable stubs: `04_rollback_bootstrap_stubs.sql`

## Safety

- Dual-write flags must stay OFF.
- No production credentials in seed files.
- Do not apply these SQL files to production as part of this test kit (production uses the checklist doc later).
