#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/Users/simonpittaway/Dropbox/tape-film-ichoose"
INTERVAL_SECONDS="${1:-300}"

cd "$PROJECT_DIR"

while true; do
  echo "----- $(date) -----"
  venv/bin/python - <<'PY'
import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv('.env')
sb = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_SERVICE_KEY'))

total = sb.table('catalog_items').select('id', count='exact').execute().count or 0
refreshed = sb.table('catalog_items').select('id', count='exact').not_.is_('tmdb_last_refreshed_at', 'null').execute().count or 0
pending = sb.table('catalog_items').select('id', count='exact').is_('tmdb_last_refreshed_at', 'null').execute().count or 0
matched = sb.table('catalog_items').select('id', count='exact').eq('tmdb_match_status', 'matched').execute().count or 0
not_found = sb.table('catalog_items').select('id', count='exact').eq('tmdb_match_status', 'not_found').execute().count or 0
no_clean = sb.table('catalog_items').select('id', count='exact').eq('tmdb_match_status', 'no_clean_title').execute().count or 0

print(f"total={total} refreshed={refreshed} pending={pending} matched={matched} not_found={not_found} no_clean_title={no_clean}")
PY
  sleep "$INTERVAL_SECONDS"
done

