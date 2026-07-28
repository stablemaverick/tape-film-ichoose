#!/usr/bin/env bash
# =============================================================================
# Daily supplier orders needed report + Slack summary. Read-only (no Shopify writes).
# Loads /opt/tape-film-ichoose/.env for SLACK_WEBHOOK_URL; job uses .env.prod for Shopify.
#
# CSVs are written to the tapester FTP home (override with SUPPLIER_ORDERS_REPORT_DIR):
#   /srv/ftps/data/tapester/reports/supplier_orders/
# Upload open-PO snapshots (latest *.csv wins) to:
#   /srv/ftps/data/tapester/reports/supplier_orders/inbound/
# Override inbound with SUPPLIER_ORDERS_INBOUND_DIR.
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
INBOUND_DIR="${SUPPLIER_ORDERS_INBOUND_DIR:-${REPORT_DIR}/inbound}"

if ! mkdir -p "${REPORT_DIR}" "${INBOUND_DIR}" 2>/dev/null; then
  echo "[${JOB_LABEL}] ERROR: cannot create REPORT_DIR=${REPORT_DIR} or INBOUND_DIR=${INBOUND_DIR}" >&2
  echo "[${JOB_LABEL}] Create it once on the VM, e.g.:" >&2
  echo "  sudo mkdir -p ${DEFAULT_REPORT_DIR}/inbound" >&2
  echo "  sudo chown $(whoami):$(whoami) ${DEFAULT_REPORT_DIR} ${DEFAULT_REPORT_DIR}/inbound" >&2
  echo "  sudo chmod 755 ${DEFAULT_REPORT_DIR}" >&2
  echo "  sudo chmod 775 ${DEFAULT_REPORT_DIR}/inbound" >&2
  "${NOTIFY}" "❌ ${JOB_LABEL} FAILURE host=${HOST} cannot create ${REPORT_DIR}"
  exit 1
fi
# Ensure tapester (and FTP clients) can read report CSVs and upload into inbound/.
chmod 755 "${REPORT_DIR}" 2>/dev/null || true
chmod 775 "${INBOUND_DIR}" 2>/dev/null || true

# FTP uploads often land as mode 600 owned by the vsftpd mapped uid. Cron runs as
# simonpittaway (in group staff). Prefer group/world-readable inbound CSVs so PO
# netting works. Default ACL on inbound/ (set once on the VM) is the durable fix;
# this is a best-effort repair each run.
fix_inbound_csv_permissions() {
  local f mode owner
  shopt -s nullglob
  for f in "${INBOUND_DIR}"/*.csv; do
    [[ -e "${f}" ]] || continue
    if [[ -r "${f}" ]]; then
      chmod ug+rw,o+r "${f}" 2>/dev/null || true
      continue
    fi
    echo "[${JOB_LABEL}] WARN: inbound CSV not readable: ${f}" >&2
    # Try without sudo first (works if we own it).
    if chmod ug+rw,o+r "${f}" 2>/dev/null && [[ -r "${f}" ]]; then
      echo "[${JOB_LABEL}] fixed mode on ${f}" >&2
      continue
    fi
    # Optional passwordless sudo for the FTP-mapped owner files.
    if command -v sudo >/dev/null 2>&1; then
      if sudo -n chmod ug+rw,o+r "${f}" 2>/dev/null && [[ -r "${f}" ]]; then
        echo "[${JOB_LABEL}] fixed mode via sudo on ${f}" >&2
        continue
      fi
      if sudo -n chown "$(id -u):staff" "${f}" 2>/dev/null \
        && sudo -n chmod ug+rw,o+r "${f}" 2>/dev/null \
        && [[ -r "${f}" ]]; then
        echo "[${JOB_LABEL}] fixed owner+mode via sudo on ${f}" >&2
        continue
      fi
    fi
    mode="$(stat -c '%a %U:%G' "${f}" 2>/dev/null || echo '?')"
    echo "[${JOB_LABEL}] ERROR: cannot read inbound CSV ${f} (${mode})" >&2
    "${NOTIFY}" "⚠️ ${JOB_LABEL} inbound CSV unreadable host=${HOST} file=$(basename "${f}") perms=${mode} — PO netting may be skipped. Fix: chmod ug+rw,o+r or set default ACL on ${INBOUND_DIR}"
  done
  shopt -u nullglob
}
fix_inbound_csv_permissions

cd "${ROOT}"
echo "[${JOB_LABEL}] START out_dir=${REPORT_DIR} inbound_dir=${INBOUND_DIR}" >&2
"${NOTIFY}" "📦 ${JOB_LABEL} START host=${HOST} out_dir=${REPORT_DIR} inbound=${INBOUND_DIR}"

set +e
# Capture Slack body from stdout; job logs go to stderr / log file.
slack_body="$("${VENV_PYTHON}" -m jobs.supplier_orders_report \
  --env-file /opt/tape-film-ichoose/.env.prod \
  --out-dir "${REPORT_DIR}" \
  --inbound-dir "${INBOUND_DIR}" \
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
