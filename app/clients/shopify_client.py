"""
Shopify Admin API client with OAuth token generation and GraphQL helper.

Handles:
  - Dynamic access token generation via client_credentials grant
  - GraphQL query/mutation execution with error handling
  - Variant lookup by barcode (duplicate detection)
"""

import os
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv


class ShopifyClient:
    def __init__(
        self,
        shop: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        api_version: str = "2026-04",
    ):
        self.shop = shop or os.getenv("SHOPIFY_SHOP")
        self.client_id = client_id or os.getenv("SHOPIFY_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("SHOPIFY_CLIENT_SECRET")
        self.api_version = api_version

        if not self.shop:
            raise SystemExit("Missing SHOPIFY_SHOP")
        if not self.client_id:
            raise SystemExit("Missing SHOPIFY_CLIENT_ID")
        if not self.client_secret:
            raise SystemExit("Missing SHOPIFY_CLIENT_SECRET")

        self._access_token: Optional[str] = None

    @property
    def graphql_url(self) -> str:
        return f"https://{self.shop}/admin/api/{self.api_version}/graphql.json"

    @property
    def access_token(self) -> str:
        if not self._access_token:
            self._access_token = self._get_access_token()
        return self._access_token

    def _get_access_token(self) -> str:
        url = f"https://{self.shop}/admin/oauth/access_token"
        resp = requests.post(
            url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=30,
        )
        resp.raise_for_status()
        token = (resp.json() or {}).get("access_token")
        if not token:
            raise RuntimeError(f"No access token returned: {resp.text}")
        return token

    def graphql(self, query: str, variables: Optional[dict] = None) -> dict:
        """Execute a GraphQL query/mutation and return the data payload."""
        headers = {
            "X-Shopify-Access-Token": self.access_token,
            "Content-Type": "application/json",
        }
        resp = requests.post(
            self.graphql_url,
            headers=headers,
            json={"query": query, "variables": variables or {}},
            timeout=60,
        )
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("errors"):
            raise RuntimeError(payload["errors"])
        return payload["data"]

    def variant_exists_by_barcode(self, barcode: str) -> Optional[Dict[str, Any]]:
        """Check if a variant with this barcode already exists in the store."""
        query = """
        query ($q: String!) {
          productVariants(first: 1, query: $q) {
            nodes {
              id
              barcode
              sku
              product { id title }
            }
          }
        }
        """
        data = self.graphql(query, {"q": f"barcode:{barcode}"})
        nodes = data.get("productVariants", {}).get("nodes", []) or []
        return nodes[0] if nodes else None


def get_shopify_client(env_file: str = ".env", api_version: str = "2026-04") -> ShopifyClient:
    """Factory that loads env and returns a configured ShopifyClient."""
    load_dotenv(env_file)
    return ShopifyClient(api_version=api_version)
