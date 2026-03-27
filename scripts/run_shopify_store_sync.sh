#!/usr/bin/env bash
set -euo pipefail

cd /opt/tape-film-ichoose
./venv/bin/python -m jobs.shopify_store_sync
