#!/usr/bin/env bash
# =============================================================================
# Daily supplier orders needed report + Slack summary. Read-only (no Shopify writes).
# Loads /opt/tape-film-ichoose/.env for SLACK_WEBHOOK_URL; job uses .env.prod for Shopify.
#
# CSVs are written to the tapester FTP home (override with SUPPLIER_ORDERS_REPORT_DIR):
#   /srv/ftps/data/tapester/reports/supplier_orders/
# =============================================================================
set -euo pipefail

EXPECTED_ROOT="/opt/tape-film-ichoose"
ROOT="${EXPECTED_ROOT}"
NOTIFY="/opt/tape-film-ichoose/scripts/notify_slack.sh"
VENV_PYTHON="/opt/tape-film-ichoose/venv/bin/python"
JOB_LABEL="supplier orders report"
HOST="$(hostname -f 2>/dev/null || hostname)"
# FTP-visible folder for user ``tapester`` (vsftpd local_root=/srv/ftps/data/$USER).
DEFAULT_REPORT_DIR="/srv/ftps/data/tapester/reports/supplier_orders"

if [[ "${ROOT}" != "${EXPECTED_ROOT}" ]]; then
  echo "[${JOB_LABEL}] ERROR: ROOT path mismatch" >&2
  exit 1
fi

mkdir -p "${ROOT}/logs" "${ROOT}/tmp"

safe_load_env_file() {
  local env_file="$1"
  local line key val
  [[ -r "${env_file}" ]] || return 0
  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="${line#"${line%%[![:space:]]*}"}"
    line="${line%"${line##*[![:space:]]}"}"
    [[ -z "${line}" ]] && continue
    [[ "${line:0:1}" == "#" ]] && continue
    [[ "${line}" =~ ^([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]] || continue
    key="${BASH_REMATCH[1]}"
    val="${BASH_REMATCH[2]}"
    val="${val#"${val%%[![:space:]]*}"}"
    val="${val%"${val##*[![:space:]]}"}"
    if [[ "${val}" =~ ^\"(.*)\"$ ]]; then
      val="${BASH_REMATCH[1]}"
    elif [[ "${val}" =~ ^\'(.*)\'$ ]]; then
      val="${BASH_REMATCH[1]}"
    fi
    printf -v "${key}" '%s' "${val}" || continue
    export "${key}"
  done <"${env_file}"
}

safe_load_env_file "${ROOT}/.env"
# Optional override from .env.prod as well (LOCATION etc. already used by the job).
safe_load_env_file "${ROOT}/.env.prod"

REPORT_DIR="${SUPPLIER_ORDERS_REPORT_DIR:-${DEFAULT_REPORT_DIR}}"

if ! mkdir -p "${REPORT_DIR}" 2>/dev/null; then
  echo "[${JOB_LABEL}] ERROR: cannot create REPORT_DIR=${REPORT_DIR}" >&2
  echo "[${JOB_LABEL}] Create it once on the VM, e.g.:" >&2
  echo "  sudo mkdir -p ${DEFAULT_REPORT_DIR}" >&2
  echo "  sudo chown $(whoami):$(whoami) ${DEFAULT_REPORT_DIR}" >&2
  echo "  sudo chmod 755 ${DEFAULT_REPORT_DIR}" >&2
  "${NOTIFY}" "❌ ${JOB_LABEL} FAILURE host=${HOST} cannot create ${REPORT_DIR}"
  exit 1
fi
# Ensure tapester (and FTP clients) can read files written by the cron user.
chmod 755 "${REPORT_DIR}" 2>/dev/null || true

cd "${ROOT}"
echo "[${JOB_LABEL}] START out_dir=${REPORT_DIR}" >&2
"${NOTIFY}" "📦 ${JOB_LABEL} START host=${HOST} out_dir=${REPORT_DIR}"

set +e
# Capture Slack body from stdout; job logs go to stderr / log file.
slack_body="$("${VENV_PYTHON}" -m jobs.supplier_orders_report \
  --env-file /opt/tape-film-ichoose/.env.prod \
  --out-dir "${REPORT_DIR}" \
  --print-slack 2>>"${ROOT}/logs/cron_supplier_orders_report.log")"
exit_code=$?
set -e

if [[ "${exit_code}" -eq 0 ]]; then
  # World-readable CSVs so FTP user tapester can download.
  chmod a+r "${REPORT_DIR}"/*.csv 2>/dev/null || true
  # Drop the SUCCESS JSON line; keep the multi-line summary after it.
  summary="$(printf '%s\n' "${slack_body}" | sed -n '/^📦 supplier orders needed/,$p')"
  if [[ -z "${summary}" ]]; then
    summary="✅ ${JOB_LABEL} SUCCESS host=${HOST} ftp=${REPORT_DIR}"
  fi
  "${NOTIFY}" "${summary}"
else
  "${NOTIFY}" "❌ ${JOB_LABEL} FAILURE host=${HOST} exit_code=${exit_code}"
fi

exit "${exit_code}"
