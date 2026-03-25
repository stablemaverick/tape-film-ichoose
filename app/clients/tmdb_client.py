"""
TMDB API client with rate limiting and retry logic.

Wraps all TMDB HTTP interactions with:
  - Configurable read timeout (60s default)
  - Exponential backoff on 429 / 5xx / network errors (up to 8 retries)
  - Clean separation from business logic
"""

import os
import time
from typing import Any, Dict, Optional, Tuple

import requests
from requests.exceptions import ReadTimeout, RequestException
from dotenv import load_dotenv


DEFAULT_API_URL = "https://api.themoviedb.org/3"
DEFAULT_TIMEOUT = 60
DEFAULT_MAX_RETRIES = 8


class TmdbClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        api_url: str = DEFAULT_API_URL,
        timeout: int = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ):
        self.api_key = api_key or os.getenv("TMDB_API_KEY")
        if not self.api_key:
            raise SystemExit("Missing TMDB_API_KEY")
        self.api_url = api_url
        self.timeout = timeout
        self.max_retries = max_retries

    def _request(self, url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """GET with retries on timeouts and 5xx."""
        delay = 1.0
        for attempt in range(self.max_retries):
            try:
                resp = requests.get(url, params=params, timeout=self.timeout)
            except (ReadTimeout, RequestException) as exc:
                if attempt == self.max_retries - 1:
                    raise RuntimeError(f"TMDB request failed after retries: {url} ({exc})") from exc
                time.sleep(delay)
                delay = min(delay * 2, 120.0)
                continue

            if resp.status_code == 404:
                return None
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                if attempt == self.max_retries - 1:
                    resp.raise_for_status()
                time.sleep(delay)
                delay = min(delay * 2, 120.0)
                continue

            resp.raise_for_status()
            return resp.json()

        raise RuntimeError(f"TMDB request failed after retries: {url}")

    def search(
        self,
        query: str,
        media_type: str = "movie",
        *,
        include_adult: bool = False,
    ) -> list[Dict[str, Any]]:
        """Search TMDB for movies or TV shows."""
        endpoint = "tv" if media_type == "tv" else "movie"
        data = self._request(
            f"{self.api_url}/search/{endpoint}",
            {
                "api_key": self.api_key,
                "query": query,
                "include_adult": include_adult,
            },
        )
        return (data or {}).get("results", [])

    def get_details(
        self, tmdb_id: int, media_type: str = "movie"
    ) -> Optional[Dict[str, Any]]:
        """Fetch full details for a movie or TV show."""
        endpoint = "tv" if media_type == "tv" else "movie"
        return self._request(
            f"{self.api_url}/{endpoint}/{tmdb_id}",
            {"api_key": self.api_key},
        )

    def get_credits(
        self, tmdb_id: int, media_type: str = "movie"
    ) -> Optional[Dict[str, Any]]:
        """Fetch credits (cast + crew) for a movie or TV show."""
        endpoint = "tv" if media_type == "tv" else "movie"
        return self._request(
            f"{self.api_url}/{endpoint}/{tmdb_id}/credits",
            {"api_key": self.api_key},
        )

    def get_details_and_credits(
        self, tmdb_id: int, media_type: str = "movie"
    ) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
        """Fetch both details and credits in sequence."""
        details = self.get_details(tmdb_id, media_type)
        if not details:
            return None
        credits = self.get_credits(tmdb_id, media_type)
        if not credits:
            return None
        return details, credits


def get_tmdb_client(env_file: str = ".env") -> TmdbClient:
    """Factory that loads env and returns a configured TmdbClient."""
    load_dotenv(env_file)
    return TmdbClient()
