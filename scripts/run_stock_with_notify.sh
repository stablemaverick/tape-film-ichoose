#!/usr/bin/env bash
# =============================================================================
# Stock pipeline with Slack start/success/failure notifications. Preserves exit code.
# Loads /opt/tape-film-ichoose/.env via line-by-line KEY=value parsing only (no shell execution).
# After the pipeline, runs verify_pipeline_observability.py and enriches the final Slack line.
# =============================================================================
set -euo pipefail

EXPECTED_ROOT="/opt/tape-film-ichoose"
ROOT="${EXPECTED_ROOT}"
NOTIFY="/opt/tape-film-ichoose/scripts/notify_slack.sh"
PIPELINE="/opt/tape-film-ichoose/pipeline/run_stock_sync.sh"
VENV_PYTHON="/opt/tape-film-ichoose/venv/bin/python"
VERIFY_OBS="/opt/tape-film-ichoose/scripts/observability/verify_pipeline_observability.py"
VENV_ACTIVATE="/opt/tape-film-ichoose/venv/bin/activate"
JOB_LABEL="stock sync"

HOST="$(hostname -f 2>/dev/null || hostname)"

if [[ "${ROOT}" != "${EXPECTED_ROOT}" ]]; then
  echo "[${JOB_LABEL}] ERROR: ROOT path mismatch (refusing mkdir)" >&2
  exit 1
fi

mkdir -p "${ROOT}/logs" "${ROOT}/.locks"

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

echo "[${JOB_LABEL}] START" >&2

cd "${ROOT}"
# shellcheck disable=SC1091
source "${VENV_ACTIVATE}"

"${NOTIFY}" "🚀 ${JOB_LABEL} START host=${HOST}"

set +e
/bin/bash "${PIPELINE}"
exit_code=$?
set -e

slack_final_line_for_pipeline() {
  local wcode="$1"
  local succ=0
  [[ "${wcode}" -eq 0 ]] && succ=1
  local json_out obs_ec fmt_py line_out
  set +e
  json_out="$("${VENV_PYTHON}" "${VERIFY_OBS}" --require-snapshot-link --env /opt/tape-film-ichoose/.env 2>/dev/null)"
  obs_ec=$?
  set -e
  fmt_py="$(mktemp "${TMPDIR:-/tmp}/slack_fmt_stock.XXXXXX.py")"
  cat >"${fmt_py}" <<'PY'
import json
import sys

job, host, wcode_s, succ_s, obs_ec_s = sys.argv[1:6]
wcode = int(wcode_s)
success = succ_s == "1"
obs_ec = int(obs_ec_s)
raw = sys.stdin.read().strip()


def fallback() -> None:
    emoji = "✅" if success else "❌"
    status = "SUCCESS" if success else "FAILURE"
    if success:
        print(f"{emoji} {job} {status} host={host}", end="")
    else:
        print(f"{emoji} {job} {status} host={host} exit_code={wcode}", end="")


if obs_ec != 0 or not raw:
    fallback()
    sys.exit(0)

try:
    d = json.loads(raw)
    pr = d.get("pipeline_runs_latest")
    if not isinstance(pr, dict):
        pr = {}

    def g(x):
        return "n/a" if x is None else str(x)

    def fbool(v):
        if v is None:
            return "n/a"
        return "true" if v else "false"

    emoji = "✅" if success else "❌"
    status = "SUCCESS" if success else "FAILURE"
    msg = f"{emoji} {job} {status} host={host}"
    if not success:
        msg += f" exit_code={wcode}"
    msg += (
        f" pipeline_type={g(pr.get('pipeline_type'))} completed={g(pr.get('completed'))}"
        f" inserts={g(pr.get('inserts'))} updates={g(pr.get('updates'))}"
        f" duration_seconds={g(pr.get('duration_seconds'))} health_exit_code={g(pr.get('health_exit_code'))}"
        f" log_file={g(pr.get('log_file'))} run_id={g(pr.get('id'))}"
        f" snapshot_match={fbool(d.get('snapshot_pipeline_run_id_matches_row'))}"
        f" pairs_latest={fbool(d.get('latest_snapshot_pairs_latest_run_by_id'))}"
    )
    print(msg, end="")
except Exception:
    fallback()
PY
  line_out="$(
    printf '%s' "${json_out}" | "${VENV_PYTHON}" "${fmt_py}" "${JOB_LABEL}" "${HOST}" "${wcode}" "${succ}" "${obs_ec}" || true
  )"
  rm -f "${fmt_py}"
  printf '%s' "${line_out}"
}

final_line="$(slack_final_line_for_pipeline "${exit_code}")"
if [[ -z "${final_line}" ]]; then
  if [[ "${exit_code}" -eq 0 ]]; then
    final_line="✅ ${JOB_LABEL} SUCCESS host=${HOST}"
  else
    final_line="❌ ${JOB_LABEL} FAILURE host=${HOST} exit_code=${exit_code}"
  fi
fi
"${NOTIFY}" "${final_line}"

exit "${exit_code}"
