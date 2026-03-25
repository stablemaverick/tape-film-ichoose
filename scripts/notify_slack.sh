#!/usr/bin/env bash
# =============================================================================
# Post a single message to Slack via incoming webhook (SLACK_WEBHOOK_URL).
# Best-effort only: missing URL, bad payload, or curl errors → warning on stderr, exit 0.
# =============================================================================
set -uo pipefail

if [[ -z "${SLACK_WEBHOOK_URL:-}" ]]; then
  echo "notify_slack: SLACK_WEBHOOK_URL not set; skipping Slack notification" >&2
  exit 0
fi

if [[ $# -lt 1 ]] || [[ -z "${1:-}" ]]; then
  echo "notify_slack: empty message; skipping" >&2
  exit 0
fi

if ! payload="$(/usr/bin/python3 -c 'import json,sys; print(json.dumps({"text": sys.argv[1]}))' "$1" 2>/dev/null)"; then
  echo "notify_slack: failed to encode message (need /usr/bin/python3)" >&2
  exit 0
fi

if ! curl -sS --max-time 30 -f -X POST -H 'Content-Type: application/json' -d "${payload}" "${SLACK_WEBHOOK_URL}" >/dev/null; then
  echo "notify_slack: curl POST to Slack webhook failed" >&2
fi
exit 0
