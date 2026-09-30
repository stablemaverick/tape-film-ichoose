"""
Ordering Agent V1 — orchestration over Inventory Intelligence.

Path:
  intent → search_inventory → rank/clarify → CommerceOfferService → customer-safe response

LLM never supplies stock or price. Conversation state is bounded candidate context only.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from app.services.commerce_offer_service import CommerceOfferService
from app.services.ordering_agent_intent import (
    ReleaseSearchIntent,
    dominant_candidate,
    extract_release_intent,
    rank_release_candidates,
)
from app.services.ordering_agent_public import (
    assert_ordering_public_safe,
    customer_status_phrase,
    public_choice,
    public_offer_from_commerce,
)
from app.services.stock_availability_service import (
    StockAvailabilityError,
    StockAvailabilityService,
)


_SESSION_TTL_SEC = 60 * 60
_SESSION_PATH = os.environ.get(
    "ORDERING_AGENT_SESSION_PATH",
    os.path.join(tempfile.gettempdir(), "tape_ordering_agent_sessions.json"),
)


def _load_all_sessions() -> dict[str, Any]:
    try:
        with open(_SESSION_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _save_all_sessions(data: dict[str, Any]) -> None:
    try:
        with open(_SESSION_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except OSError:
        pass


def _purge_sessions(store: dict[str, Any], now: Optional[float] = None) -> dict[str, Any]:
    t = now or time.time()
    return {
        k: v
        for k, v in store.items()
        if t - float((v or {}).get("updated_at") or 0) <= _SESSION_TTL_SEC
    }


def _get_session(conversation_id: str) -> dict[str, Any]:
    store = _purge_sessions(_load_all_sessions())
    sess = store.get(conversation_id)
    if not sess:
        sess = {"candidates": [], "updated_at": time.time()}
        store[conversation_id] = sess
        _save_all_sessions(store)
    return sess


def _save_session(conversation_id: str, candidates: list[dict[str, Any]]) -> None:
    store = _purge_sessions(_load_all_sessions())
    store[conversation_id] = {
        "candidates": candidates[:10],
        "updated_at": time.time(),
    }
    _save_all_sessions(store)


def clear_ordering_sessions() -> None:
    try:
        if os.path.exists(_SESSION_PATH):
            os.remove(_SESSION_PATH)
    except OSError:
        pass
    _save_all_sessions({})


@dataclass
class OrderingAgentObservability:
    request_id: str
    conversation_id: str
    query: str
    intent: dict[str, Any] = field(default_factory=dict)
    search_result_count: int = 0
    selected_release_variant_id: Optional[str] = None
    clarification: bool = False
    tool_calls: list[str] = field(default_factory=list)
    final_customer_status: Optional[str] = None
    latency_ms: float = 0.0
    error_code: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "conversation_id": self.conversation_id,
            "query": self.query,
            "intent": self.intent,
            "search_result_count": self.search_result_count,
            "selected_release_variant_id": self.selected_release_variant_id,
            "clarification": self.clarification,
            "tool_calls": self.tool_calls,
            "final_customer_status": self.final_customer_status,
            "latency_ms": self.latency_ms,
            "error_code": self.error_code,
        }


class OrderingAgentService:
    def __init__(self, supabase: Any, *, now: Optional[datetime] = None):
        self.sb = supabase
        self.now = now or datetime.now(timezone.utc)
        self.stock = StockAvailabilityService(supabase, now=self.now)
        self.commerce = CommerceOfferService(supabase, now=self.now)

    def handle(
        self,
        message: str,
        *,
        conversation_id: Optional[str] = None,
        intent_override: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        t0 = time.perf_counter()
        cid = (conversation_id or "").strip() or str(uuid.uuid4())
        rid = str(uuid.uuid4())
        obs = OrderingAgentObservability(request_id=rid, conversation_id=cid, query=message or "")

        try:
            intent = extract_release_intent(message or "")
            if intent_override:
                intent = self._merge_intent(intent, intent_override)
            obs.intent = intent.to_dict()

            sess = _get_session(cid)
            prior = list(sess.get("candidates") or [])

            # Numeric / ordinal follow-up selection ("2", "the first one")
            if prior:
                picked = self._pick_by_index(message or "", prior)
                if picked is not None:
                    return self._answer_release(picked, intent, obs, cid, t0)

            # Multi-turn refinement against prior candidates
            if prior and self._looks_like_refinement(message or "", intent):
                refined = self._refine_against_prior(prior, intent, message or "")
                if refined:
                    ranked = rank_release_candidates(refined, intent, limit=5)
                    dom = dominant_candidate(ranked)
                    if dom:
                        return self._answer_release(dom, intent, obs, cid, t0)
                    choices = [public_choice(c) for c in ranked[:5]]
                    _save_session(cid, ranked)
                    return self._clarify(choices, obs, cid, t0, intent)

            # Exact barcode path
            if intent.barcode:
                obs.tool_calls.append("get_commerce_offer:barcode")
                return self._offer_by_barcode(intent.barcode, intent, obs, cid, t0)

            # Search
            search_q = intent.title or intent.query_text
            if not search_q or len(search_q.strip()) < 2:
                obs.error_code = "RELEASE_NOT_FOUND"
                return self._no_results(obs, cid, t0, intent)

            obs.tool_calls.append("search_inventory")
            search = self.stock.search_inventory(search_q, limit=30)
            candidates = list(search.get("candidates") or [])
            # Fallback: drop short filler tokens if full phrase misses (e.g. missing "in")
            if not candidates:
                tokens = [t for t in re.findall(r"[A-Za-z0-9']+", search_q) if len(t) > 2]
                if len(tokens) >= 2:
                    loose = " ".join(tokens[:3])
                    if loose.lower() != search_q.strip().lower():
                        obs.tool_calls.append("search_inventory:loose")
                        search = self.stock.search_inventory(loose, limit=30)
                        candidates = list(search.get("candidates") or [])
            obs.search_result_count = len(candidates)

            if not candidates:
                obs.error_code = "RELEASE_NOT_FOUND"
                return self._no_results(obs, cid, t0, intent)

            ranked = rank_release_candidates(candidates, intent, limit=8)
            _save_session(cid, ranked)
            dom = dominant_candidate(ranked)

            # "what versions" → always clarify when multiple
            if self._wants_versions(message or "") and len(ranked) > 1:
                choices = [public_choice(c) for c in ranked[:5]]
                return self._clarify(choices, obs, cid, t0, intent)

            if dom is None:
                choices = [public_choice(c) for c in ranked[:5]]
                return self._clarify(choices, obs, cid, t0, intent)

            return self._answer_release(dom, intent, obs, cid, t0)

        except StockAvailabilityError as exc:
            obs.error_code = getattr(exc, "code", None) or "INVENTORY_UNAVAILABLE"
            obs.latency_ms = round((time.perf_counter() - t0) * 1000, 1)
            out = {
                "conversation_id": cid,
                "type": "error",
                "error": obs.error_code,
                "message": self._customer_error_message(obs.error_code),
                "observability": obs.to_dict(),
            }
            assert_ordering_public_safe({k: v for k, v in out.items() if k != "observability"})
            return out
        except Exception:
            obs.error_code = "AGENT_ERROR"
            obs.latency_ms = round((time.perf_counter() - t0) * 1000, 1)
            return {
                "conversation_id": cid,
                "type": "error",
                "error": "AGENT_ERROR",
                "message": "Sorry — I couldn’t complete that request right now. Please try again.",
                "observability": obs.to_dict(),
            }

    def get_film_offer(self, release_variant_id: str) -> dict[str, Any]:
        """Narrow tool: commerce offer only (customer-safe)."""
        offer = self.commerce.get_commerce_offer(
            release_variant_id=release_variant_id, include_internal=False
        )
        public = offer.get("public") or offer
        return public_offer_from_commerce(public)

    # ------------------------------------------------------------------ helpers

    def _merge_intent(self, intent: ReleaseSearchIntent, override: dict[str, Any]) -> ReleaseSearchIntent:
        data = intent.to_dict()
        for k, v in override.items():
            if k in data and v is not None:
                data[k] = v
        return ReleaseSearchIntent(**data)

    def _pick_by_index(self, message: str, prior: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
        m = (message or "").strip().lower()
        if not m or not prior:
            return None
        import re

        ordinals = {
            "1": 0,
            "first": 0,
            "one": 0,
            "2": 1,
            "second": 1,
            "two": 1,
            "3": 2,
            "third": 2,
            "three": 2,
            "4": 3,
            "fourth": 3,
            "5": 4,
            "fifth": 4,
        }
        if m in ordinals and ordinals[m] < len(prior):
            return prior[ordinals[m]]
        m_num = re.fullmatch(r"(?:option\s+|number\s+|#)?([1-5])\b", m)
        if m_num:
            idx = int(m_num.group(1)) - 1
            if 0 <= idx < len(prior):
                return prior[idx]
        for word, idx in (("first", 0), ("second", 1), ("third", 2), ("fourth", 3), ("fifth", 4)):
            if re.search(rf"\b{word}\b", m) and idx < len(prior) and len(m.split()) <= 5:
                return prior[idx]
        return None

    def _looks_like_refinement(self, message: str, intent: ReleaseSearchIntent) -> bool:
        m = (message or "").strip().lower()
        if not m:
            return False
        # Short follow-ups / qualifier-only
        if intent.steelbook or intent.limited_edition or intent.collectors_edition or intent.format:
            if not intent.title or len(intent.title.split()) <= 2:
                return True
        if len(m.split()) <= 4 and any(
            x in m for x in ("steelbook", "limited", "collector", "4k", "blu", "dvd", "standard", "that one", "the first")
        ):
            return True
        return False

    def _refine_against_prior(
        self, prior: list[dict[str, Any]], intent: ReleaseSearchIntent, message: str
    ) -> list[dict[str, Any]]:
        return rank_release_candidates(prior, intent, limit=8)

    def _wants_versions(self, message: str) -> bool:
        m = message.lower()
        return "what version" in m or "which version" in m or "what editions" in m or "which editions" in m

    def _answer_release(
        self,
        candidate: dict[str, Any],
        intent: ReleaseSearchIntent,
        obs: OrderingAgentObservability,
        cid: str,
        t0: float,
    ) -> dict[str, Any]:
        rid = candidate.get("release_variant_id") or candidate.get("id")
        obs.tool_calls.append("get_commerce_offer")
        offer_wrap = self.commerce.get_commerce_offer(release_variant_id=str(rid), include_internal=False)
        public = offer_wrap.get("public") or offer_wrap
        release = public_offer_from_commerce(public, format=candidate.get("format"))
        # Enrich shopify product path when listing exists
        release = self._enrich_shopify_nav(release)

        obs.selected_release_variant_id = release.get("release_variant_id")
        obs.final_customer_status = release.get("availability")
        obs.latency_ms = round((time.perf_counter() - t0) * 1000, 1)

        status = release.get("availability")
        price = release.get("price")
        title = release.get("title") or candidate.get("title") or "That title"
        phrase = customer_status_phrase(status, price)

        if status in {"in_stock", "available_from_supplier", "available_to_order", "preorder"}:
            msg = f"Yes — {title} is {phrase}."
        elif status == "out_of_stock":
            msg = f"{title} is currently Out of Stock."
        else:
            msg = f"{title}: {phrase}."

        out = {
            "conversation_id": cid,
            "type": "answer",
            "message": msg,
            "release": release,
            "observability": obs.to_dict(),
        }
        assert_ordering_public_safe({k: v for k, v in out.items() if k != "observability"})
        return out

    def _offer_by_barcode(
        self,
        barcode: str,
        intent: ReleaseSearchIntent,
        obs: OrderingAgentObservability,
        cid: str,
        t0: float,
    ) -> dict[str, Any]:
        try:
            offer_wrap = self.commerce.get_commerce_offer(barcode=barcode, include_internal=False)
        except StockAvailabilityError as exc:
            code = getattr(exc, "code", None) or "RELEASE_NOT_FOUND"
            obs.error_code = code
            obs.latency_ms = round((time.perf_counter() - t0) * 1000, 1)
            return {
                "conversation_id": cid,
                "type": "error",
                "error": code,
                "message": self._customer_error_message(code),
                "observability": obs.to_dict(),
            }
        public = offer_wrap.get("public") or offer_wrap
        release = self._enrich_shopify_nav(public_offer_from_commerce(public))
        obs.selected_release_variant_id = release.get("release_variant_id")
        obs.final_customer_status = release.get("availability")
        obs.latency_ms = round((time.perf_counter() - t0) * 1000, 1)
        title = release.get("title") or "That title"
        phrase = customer_status_phrase(release.get("availability"), release.get("price"))
        out = {
            "conversation_id": cid,
            "type": "answer",
            "message": f"Yes — {title} is {phrase}.",
            "release": release,
            "observability": obs.to_dict(),
        }
        assert_ordering_public_safe({k: v for k, v in out.items() if k != "observability"})
        return out

    def _enrich_shopify_nav(self, release: dict[str, Any]) -> dict[str, Any]:
        """Attach shopify_listed + canonical product_url from product_handle (mirror or live)."""
        rid = release.get("release_variant_id")
        if not rid:
            return release
        try:
            rows = (
                self.sb.table("release_shopify_listings")
                .select("shopify_variant_id,shopify_product_id")
                .eq("release_variant_id", rid)
                .limit(1)
                .execute()
                .data
                or []
            )
        except Exception:
            return release
        if not rows:
            release["shopify_listed"] = False
            release["product_url"] = None
            return release
        release["shopify_listed"] = True
        release["product_url"] = None
        vid = rows[0].get("shopify_variant_id")
        pid = rows[0].get("shopify_product_id")
        handle = None
        if vid:
            try:
                listing = (
                    self.sb.table("shopify_listings")
                    .select("product_handle")
                    .eq("shopify_variant_id", vid)
                    .limit(1)
                    .execute()
                    .data
                    or []
                )
                handle = ((listing[0] if listing else {}) or {}).get("product_handle")
            except Exception:
                handle = None
        if not handle and pid:
            handle = self._fetch_shopify_product_handle(str(pid))
        if handle and isinstance(handle, str) and handle.strip():
            release["product_url"] = f"/products/{handle.strip()}"
        return release

    def _fetch_shopify_product_handle(self, shopify_product_id: str) -> Optional[str]:
        """Live Admin read of canonical handle when mirror column is empty/unmigrated."""
        try:
            from app.clients.shopify_client import ShopifyClient

            client = ShopifyClient()
            data = client.graphql(
                """
                query ProductHandle($id: ID!) {
                  product(id: $id) { handle }
                }
                """,
                {"id": shopify_product_id},
            )
            handle = ((data.get("product") or {}) or {}).get("handle")
            return str(handle).strip() if handle else None
        except Exception:
            return None

    def _clarify(
        self,
        choices: list[dict[str, Any]],
        obs: OrderingAgentObservability,
        cid: str,
        t0: float,
        intent: ReleaseSearchIntent,
    ) -> dict[str, Any]:
        obs.clarification = True
        obs.error_code = "AMBIGUOUS_RELEASE"
        obs.latency_ms = round((time.perf_counter() - t0) * 1000, 1)
        lines = [f"{i+1}. {c.get('label') or c.get('title')}" for i, c in enumerate(choices)]
        msg = "I found a few matching editions — which one did you mean?\n" + "\n".join(lines)
        out = {
            "conversation_id": cid,
            "type": "clarify",
            "message": msg,
            "choices": choices,
            "observability": obs.to_dict(),
        }
        assert_ordering_public_safe({k: v for k, v in out.items() if k != "observability"})
        return out

    def _no_results(
        self,
        obs: OrderingAgentObservability,
        cid: str,
        t0: float,
        intent: ReleaseSearchIntent,
    ) -> dict[str, Any]:
        obs.latency_ms = round((time.perf_counter() - t0) * 1000, 1)
        title = intent.title or "that title"
        out = {
            "conversation_id": cid,
            "type": "not_found",
            "message": (
                f"I couldn’t find {title} in the TAPE ordering catalogue. "
                "If you have a format, label, or year, I can try again."
            ),
            "observability": obs.to_dict(),
        }
        assert_ordering_public_safe({k: v for k, v in out.items() if k != "observability"})
        return out

    def _customer_error_message(self, code: str) -> str:
        if code == "RELEASE_NOT_FOUND":
            return "I couldn’t find that release in the TAPE ordering catalogue."
        if code == "OFFER_UNAVAILABLE":
            return "I found the title, but couldn’t retrieve a current offer. Please try again."
        return "Sorry — I couldn’t complete that request right now. Please try again."
