#!/usr/bin/env python3
"""Interactive / multi-turn human acceptance harness for Ordering Agent V1."""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dotenv import load_dotenv
from supabase import create_client

from app.services.ordering_agent_service import OrderingAgentService, clear_ordering_sessions


def print_turn(out: dict, *, debug: bool) -> None:
    print(f"\n[{out.get('type')}] {out.get('message')}\n")
    if out.get("release"):
        r = out["release"]
        print(
            "  release:",
            r.get("title"),
            "|",
            r.get("availability"),
            "|",
            r.get("price"),
            "| shopify_listed=",
            r.get("shopify_listed"),
            "| product_url=",
            r.get("product_url"),
        )
    if out.get("choices"):
        for i, c in enumerate(out["choices"], 1):
            print(f"  {i}. {c.get('label') or c.get('title')}")
    if debug:
        obs = out.get("observability") or {}
        print(
            "  debug:",
            json.dumps(
                {
                    "latency_ms": obs.get("latency_ms"),
                    "tool_calls": obs.get("tool_calls"),
                    "selected": obs.get("selected_release_variant_id"),
                    "intent": obs.get("intent"),
                    "search_result_count": obs.get("search_result_count"),
                    "error_code": obs.get("error_code"),
                },
                default=str,
            ),
        )


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--env-file", default=".env")
    p.add_argument("--conversation-id")
    p.add_argument("--debug", action="store_true")
    p.add_argument("--message", help="Single-shot message (non-interactive)")
    p.add_argument("--reset", action="store_true")
    args = p.parse_args()
    load_dotenv(args.env_file, override=True)
    if args.reset:
        clear_ordering_sessions()

    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])
    svc = OrderingAgentService(sb)
    cid = args.conversation_id or str(uuid.uuid4())
    print(f"conversation_id={cid}")

    if args.message:
        out = svc.handle(args.message, conversation_id=cid)
        print_turn(out, debug=args.debug)
        if args.debug:
            print(json.dumps({k: v for k, v in out.items() if k != "observability"}, default=str, indent=2))
        return 0

    print("Ordering Agent V1 harness. Empty line exits. Commands: /reset /id")
    while True:
        try:
            msg = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not msg:
            break
        if msg == "/reset":
            clear_ordering_sessions()
            cid = str(uuid.uuid4())
            print(f"reset conversation_id={cid}")
            continue
        if msg == "/id":
            print(cid)
            continue
        out = svc.handle(msg, conversation_id=cid)
        print_turn(out, debug=args.debug)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
