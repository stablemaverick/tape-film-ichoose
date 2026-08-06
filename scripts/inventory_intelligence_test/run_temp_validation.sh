#!/usr/bin/env bash
# Helper for temporary / local inventory-intelligence validation.
# Requires DATABASE_URL for SQL apply. Does not enable dual-write flags.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "Set DATABASE_URL to the temporary or local Postgres URI." >&2
  exit 2
fi

export INVENTORY_DUAL_WRITE_ENABLED=0
export INVENTORY_DUAL_WRITE_SHOPIFY=0
export INVENTORY_DUAL_WRITE_SUPPLIER=0
export INVENTORY_DUAL_WRITE_PO=0

cmd="${1:-help}"

case "$cmd" in
  apply)
    psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f scripts/inventory_intelligence_test/00_bootstrap_prereq_tables.sql
    psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f supabase/migrations/20260806120000_inventory_intelligence_foundation.sql
    psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f scripts/inventory_intelligence_test/02_seed_synthetic.sql
    echo "Apply + seed complete."
    ;;
  verify)
    ./venv/bin/python scripts/inventory_intelligence_test/verify_invariants.py \
      --env-file "${ENV_FILE:-.env.inventory-test}" \
      --format text
    ;;
  rollback)
    psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f scripts/inventory_intelligence_test/03_rollback_inventory_foundation.sql
    echo "Foundation rolled back (bootstrap stubs retained)."
    ;;
  reapply)
    "$0" rollback
    "$0" apply
    "$0" verify
    ;;
  test-suite)
    ./venv/bin/python -m pytest \
      tests/rules/test_availability_rules.py \
      tests/rules/test_inventory_invariant_rules.py \
      tests/services/test_inventory_dual_write.py \
      tests/services/test_inventory_dual_write_behaviour.py -q
    ;;
  *)
    cat <<EOF
Usage: $0 {apply|verify|rollback|reapply|test-suite}

Environment:
  DATABASE_URL   Temporary/local Postgres URI (required for apply/rollback/reapply)
  ENV_FILE       Defaults to .env.inventory-test for verify
EOF
    exit 1
    ;;
esac
