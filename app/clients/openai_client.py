"""
Minimal OpenAI Responses API client (httpx) with retry on 429 / 5xx / network errors.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Optional

import httpx

DEFAULT_API_URL = "https://api.openai.com/v1"
DEFAULT_TIMEOUT = 180.0
DEFAULT_MAX_RETRIES = 5


class OpenAIClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        api_url: str = DEFAULT_API_URL,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise SystemExit("Missing OPENAI_API_KEY")
        self.api_url = api_url
        self.timeout = timeout
        self.max_retries = max_retries

    def responses(
        self,
        *,
        model: str,
        input: str | List[Dict[str, Any]],
        instructions: Optional[str] = None,
        tools: Optional[List[Dict[str, Any]]] = None,
        tool_choice: Optional[str] = None,
        json_schema: Optional[Dict[str, Any]] = None,
        schema_name: str = "result",
        reasoning_effort: Optional[str] = None,
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {"model": model, "input": input}
        if instructions:
            body["instructions"] = instructions
        if tools:
            body["tools"] = tools
        if tool_choice:
            body["tool_choice"] = tool_choice
        if json_schema:
            body["text"] = {
                "format": {"type": "json_schema", "name": schema_name, "schema": json_schema, "strict": True}
            }
        if reasoning_effort:
            body["reasoning"] = {"effort": reasoning_effort}

        delay = 2.0
        for attempt in range(self.max_retries):
            try:
                resp = httpx.post(
                    f"{self.api_url}/responses",
                    headers={"Authorization": f"Bearer {self.api_key}"},
                    json=body,
                    timeout=self.timeout,
                )
            except httpx.HTTPError as exc:
                if attempt == self.max_retries - 1:
                    raise RuntimeError(f"OpenAI request failed after retries: {exc}") from exc
                time.sleep(delay)
                delay = min(delay * 2, 60.0)
                continue
            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt == self.max_retries - 1:
                    raise RuntimeError(f"OpenAI {resp.status_code}: {resp.text[:300]}")
                time.sleep(delay)
                delay = min(delay * 2, 60.0)
                continue
            if resp.status_code != 200:
                raise RuntimeError(f"OpenAI {resp.status_code}: {resp.text[:500]}")
            return resp.json()
        raise RuntimeError("OpenAI request failed after retries")


def output_text(response: Dict[str, Any]) -> str:
    parts: List[str] = []
    for item in response.get("output") or []:
        if item.get("type") == "message":
            for content in item.get("content") or []:
                if content.get("type") == "output_text":
                    parts.append(content.get("text") or "")
    return "".join(parts)


def output_json(response: Dict[str, Any]) -> Dict[str, Any]:
    return json.loads(output_text(response))


def usage_tokens(response: Dict[str, Any]) -> tuple[int, int]:
    usage = response.get("usage") or {}
    return int(usage.get("input_tokens") or 0), int(usage.get("output_tokens") or 0)
