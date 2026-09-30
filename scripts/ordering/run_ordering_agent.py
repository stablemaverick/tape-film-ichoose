#!/usr/bin/env python3
"""CLI harness for Ordering Agent V1 (internal / eval)."""

from __future__ import annotations

import argparse
import json
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dotenv import load_dotenv
from supabase import create_client

from app.services.ordering_agent_service import OrderingAgentService, clear_ordering_sessions


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--env-file", default=".env")
    p.add_argument("--message", required=True)
    p.add_argument("--conversation-id")
    p.add_argument("--reset-sessions", action="store_true")
    p.add_argument("--intent-json", help="Optional JSON intent override")
    args = p.parse_args(argv)

    load_dotenv(args.env_file, override=True)
    if args.reset_sessions:
        clear_ordering_sessions()

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")
    if not url or not key:
        print(json.dumps({"error": "MISSING_ENV"}))
        return 2

    intent_override = None
    if args.intent_json:
        intent_override = json.loads(args.intent_json)

    sb = create_client(url, key)
    out = OrderingAgentService(sb).handle(
        args.message,
        conversation_id=args.conversation_id,
        intent_override=intent_override,
    )
    print(json.dumps(out, default=str))
    return 0 if out.get("type") != "error" else 1


if __name__ == "__main__":
    raise SystemExit(main())
