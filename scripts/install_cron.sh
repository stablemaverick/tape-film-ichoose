#!/usr/bin/env bash
# =============================================================================
# Install tape-film-ichoose cron jobs from scripts/cron_jobs.example (idempotent).
# Merges into the current user's crontab: removes any prior managed block, appends the example.
# Safe to run multiple times. Does not print or store secrets.
# =============================================================================
set -euo pipefail

ROOT="/opt/tape-film-ichoose"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLE="${SCRIPT_DIR}/cron_jobs.example"

if [[ ! -d "$ROOT" ]]; then
  echo "ERROR: repository root not found: ${ROOT}" >&2
  exit 1
fi
if [[ ! -f "$EXAMPLE" ]]; then
  echo "ERROR: missing ${EXAMPLE}" >&2
  exit 1
fi

mkdir -p "${ROOT}/logs" "${ROOT}/.locks"

TMP="$(mktemp)"
trap 'rm -f "${TMP}"' EXIT

# Keep all existing lines except our managed section (delete from BEGIN through END inclusive).
(crontab -l 2>/dev/null || true) | sed '/^# tape-film-ichoose-cron BEGIN$/,/^# tape-film-ichoose-cron END$/d' >"${TMP}"

# Append the fresh managed block (must include the same BEGIN/END markers as above).
cat "${EXAMPLE}" >>"${TMP}"

crontab "${TMP}"

echo "Installed tape-film-ichoose cron block from: ${EXAMPLE}"
echo "Ensured directories: ${ROOT}/logs ${ROOT}/.locks"
echo "Crontab user: $(whoami)"
echo "-------------------------------------------------------------------"
crontab -l
echo "-------------------------------------------------------------------"
