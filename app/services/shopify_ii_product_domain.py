"""
Shopify → Inventory Intelligence product-domain helpers (film vs music/vinyl).

Film II excludes the Shopify merchandising collection ``soundtracks`` (vinyl/CD
soundtracks). This is a domain gate only — Shopify products stay untouched.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Optional, Sequence, Set

from app.helpers.text_helpers import clean_text

# Authoritative Shopify collection handle for soundtrack / vinyl music products.
SOUNDTRACKS_COLLECTION_HANDLE = "soundtracks"

# custom.format metafield values that are music-domain (not physical film media).
MUSIC_MEDIA_FORMATS = frozenset({"vinyl", "lp"})

# Future-safe domain labels (release_variants.product_domain when present).
PRODUCT_DOMAIN_FILM = "film"
PRODUCT_DOMAIN_MUSIC_VINYL = "music_vinyl"


def normalize_collection_handles(raw: Any) -> Set[str]:
    """Accept list/tuple/CSV/space-separated handles → lowercase set."""
    out: Set[str] = set()
    if raw is None:
        return out
    if isinstance(raw, (list, tuple, set)):
        items = list(raw)
    else:
        text = str(raw).strip()
        if not text:
            return out
        items = [p.strip() for p in text.replace(";", ",").split(",")]
    for item in items:
        h = (clean_text(item) or "").casefold()
        if h:
            out.add(h)
    return out


def listing_collection_handles(row: Mapping[str, Any]) -> Set[str]:
    if "collection_handles" in row:
        return normalize_collection_handles(row.get("collection_handles"))
    # GraphQL-shaped product node (in-memory only)
    nodes = ((row.get("collections") or {}).get("nodes") if isinstance(row.get("collections"), dict) else None)
    if nodes:
        return normalize_collection_handles([n.get("handle") for n in nodes if isinstance(n, dict)])
    return set()


def listing_media_format(row: Mapping[str, Any]) -> Optional[str]:
    for key in ("media_format", "shopify_media_format", "format"):
        val = clean_text(row.get(key))
        if val:
            return val
    meta = row.get("formatMeta")
    if isinstance(meta, dict):
        return clean_text(meta.get("value"))
    return None


def is_vinyl_soundtrack_listing(
    row: Mapping[str, Any],
    *,
    soundtrack_product_ids: Optional[Set[str]] = None,
) -> bool:
    """
    True when the Shopify listing belongs to the music/soundtrack domain.

    Precedence (deterministic, no broad title matching):
      1. collection handle ``soundtracks``
      2. custom.format / media_format ∈ {Vinyl, LP}
      3. shopify_product_id ∈ prefetched soundtracks-collection product IDs
    """
    handles = listing_collection_handles(row)
    if SOUNDTRACKS_COLLECTION_HANDLE in handles:
        return True
    fmt = (listing_media_format(row) or "").casefold()
    if fmt in MUSIC_MEDIA_FORMATS:
        return True
    pid = clean_text(row.get("shopify_product_id"))
    if soundtrack_product_ids and pid and pid in soundtrack_product_ids:
        return True
    return False


def is_film_inventory_release(row: Mapping[str, Any]) -> bool:
    """
    Whether a release_variants row belongs in the film II / Ordering Agent universe.

    Null/empty product_domain ⇒ film (legacy). Explicit music_vinyl ⇒ excluded.
    Format Vinyl/LP ⇒ excluded (domain mark from II).
    """
    domain = (clean_text(row.get("product_domain")) or "").casefold()
    if domain == PRODUCT_DOMAIN_MUSIC_VINYL:
        return False
    if domain and domain != PRODUCT_DOMAIN_FILM:
        # Unknown future domains stay out of film search until explicitly allowed.
        if domain not in {"", PRODUCT_DOMAIN_FILM}:
            return False
    fmt = (clean_text(row.get("format")) or "").casefold()
    if fmt in MUSIC_MEDIA_FORMATS:
        return False
    return True


def collection_handles_csv(handles: Iterable[str]) -> Optional[str]:
    cleaned = sorted({(clean_text(h) or "").casefold() for h in handles if clean_text(h)})
    return ",".join(cleaned) if cleaned else None


def fetch_soundtracks_collection_product_ids(shopify_client: Any) -> Set[str]:
    """Read-only: all product GIDs currently in the soundtracks collection."""
    query = """
    query ($cursor: String) {
      collectionByHandle(handle: "soundtracks") {
        products(first: 50, after: $cursor) {
          pageInfo { hasNextPage endCursor }
          nodes { id }
        }
      }
    }
    """
    out: Set[str] = set()
    cursor = None
    while True:
        data = shopify_client.graphql(query, {"cursor": cursor})
        conn = ((data or {}).get("collectionByHandle") or {}).get("products") or {}
        for node in conn.get("nodes") or []:
            pid = clean_text(node.get("id"))
            if pid:
                out.add(pid)
        page = conn.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            break
        cursor = page.get("endCursor")
    return out


def filter_film_search_candidates(
    candidates: Sequence[Mapping[str, Any]],
    *,
    music_release_ids: Optional[Set[str]] = None,
) -> list[dict[str, Any]]:
    """Drop music-domain releases from film search candidate lists."""
    blocked = music_release_ids or set()
    out: list[dict[str, Any]] = []
    for row in candidates:
        rid = clean_text(row.get("release_variant_id") or row.get("id"))
        if rid and rid in blocked:
            continue
        if not is_film_inventory_release(row):
            continue
        out.append(dict(row))
    return out
