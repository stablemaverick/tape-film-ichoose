#!/usr/bin/env bash
# Manual / optional drift report only. Not installed in cron.
# Always --dry-run: never push inventory quantities to Shopify from this wrapper.
set -euo pipefail

cd /opt/tape-film-ichoose
./venv/bin/python -m jobs.shopify_inventory_sync \
  --env-file /opt/tape-film-ichoose/.env.prod \
  --dry-run
