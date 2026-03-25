#!/usr/bin/env bash
# =============================================================================
# Stock pipeline with Slack start/success/failure notifications. Preserves exit code.
# Loads /opt/tape-film-ichoose/.env when present so SLACK_WEBHOOK_URL is available under cron.
# =============================================================================
set -euo pipefail

ROOT="/opt/tape-film-ichoose"
NOTIFY="/opt/tape-film-ichoose/scripts/notify_slack.sh"
PIPELINE="/opt/tape-film-ichoose/pipeline/run_stock_sync.sh"
VENV_ACTIVATE="/opt/tape-film-ichoose/venv/bin/activate"
JOB_NAME="stock sync"

HOST="$(hostname -f 2>/dev/null || hostname)"

if [[ -r "${ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT}/.env" || true
  set +a
fi

cd "${ROOT}"
# shellcheck disable=SC1091
source "${VENV_ACTIVATE}"

"${NOTIFY}" "[${JOB_NAME}] START host=${HOST}"

set +e
/bin/bash "${PIPELINE}"
exit_code=$?
set -e

if [[ "${exit_code}" -eq 0 ]]; then
  "${NOTIFY}" "[${JOB_NAME}] SUCCESS host=${HOST}"
else
  "${NOTIFY}" "[${JOB_NAME}] FAILURE host=${HOST} exit_code=${exit_code}"
fi

exit "${exit_code}"
