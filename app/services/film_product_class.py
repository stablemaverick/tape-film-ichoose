"""
Film-only product classification for catalogue operations (repricing, etc.).

Product-class exclusion always takes precedence over region / supplier / currency
eligibility. Vinyl, CD, book, game, gift card, test, and ambiguous products must
never enter film pricing mutations — even if allowlisted.
"""

from __future__ import annotations

import re
from typing import Optional, Sequence, Set

from app.helpers.text_helpers import clean_text
from app.services.shopify_ii_product_domain import (
    MUSIC_MEDIA_FORMATS,
    SOUNDTRACKS_COLLECTION_HANDLE,
    is_vinyl_soundtrack_listing,
)
from app.services.shopify_inventory_settings_audit import is_gift_card_product_type
from app.services.shopify_release_dual_write_service import (
    SHOPIFY_II_EXACT_EXCLUDED_PRODUCT_TITLES,
)

PRODUCT_CLASS_FILM = "film"
PRODUCT_CLASS_VINYL = "vinyl"
PRODUCT_CLASS_CD = "cd"
PRODUCT_CLASS_BOOK = "book"
PRODUCT_CLASS_GAME = "game"
PRODUCT_CLASS_OTHER_NON_FILM = "other_non_film"
PRODUCT_CLASS_AMBIGUOUS = "ambiguous"

ACTION_OUT_OF_SCOPE_NON_FILM = "OUT_OF_SCOPE_NON_FILM"
ACTION_SKIP_AMBIGUOUS_PRODUCT = "SKIP"

FILM_FORMAT_TOKENS = frozenset(
    {
        "blu-ray",
        "bluray",
        "blu ray",
        "4k",
        "uhd",
        "dvd",
        "steelbook",
        "bd",
        "uhd blu-ray",
    }
)
CD_FORMAT_TOKENS = frozenset({"cd", "compact disc", "compact-disc", "audio cd"})
BOOK_FORMAT_TOKENS = frozenset({"book", "hardcover", "paperback", "softcover", "novel"})
GAME_FORMAT_TOKENS = frozenset(
    {"game", "video game", "videogame", "xbox", "playstation", "nintendo", "switch", "ps5", "ps4"}
)
NON_FILM_COLLECTION_HANDLES = {
    PRODUCT_CLASS_VINYL: frozenset({SOUNDTRACKS_COLLECTION_HANDLE, "vinyl", "vinyls"}),
    PRODUCT_CLASS_CD: frozenset({"cds", "cd", "music-cds"}),
    PRODUCT_CLASS_BOOK: frozenset({"books", "book", "reading"}),
    PRODUCT_CLASS_GAME: frozenset({"games", "video-games", "videogames"}),
}
FILM_COLLECTION_HANDLES = frozenset(
    {"all-film", "all-film-ex-pre-orders", "blu-ray", "4k-uhd", "pre-orders"}
)


def format_tokens(raw: Optional[str]) -> Set[str]:
    text = (clean_text(raw) or "").casefold()
    if not text:
        return set()
    parts = re.split(r"[,+/|]+", text)
    out: Set[str] = set()
    for part in parts:
        p = re.sub(r"\s+", " ", part).strip()
        if p:
            out.add(p)
            out.add(p.replace("-", " "))
            out.add(p.replace(" ", "-"))
            out.add(p.replace(" ", ""))
    out.add(text)
    return {t for t in out if t}


def _product_type_class(product_type: Optional[str]) -> Optional[str]:
    pt = (clean_text(product_type) or "").casefold()
    if not pt:
        return None
    if is_gift_card_product_type(product_type):
        return PRODUCT_CLASS_OTHER_NON_FILM
    if any(k in pt for k in ("vinyl", "lp", "soundtrack", "music")):
        return PRODUCT_CLASS_VINYL
    if re.search(r"\bcd\b", pt) or "compact disc" in pt:
        return PRODUCT_CLASS_CD
    if any(k in pt for k in ("book", "hardcover", "paperback")):
        return PRODUCT_CLASS_BOOK
    if any(k in pt for k in ("game", "xbox", "playstation", "nintendo", "switch")):
        return PRODUCT_CLASS_GAME
    if any(k in pt for k in ("film", "movie", "blu-ray", "bluray", "4k", "dvd", "uhd")):
        return PRODUCT_CLASS_FILM
    return None


def _collection_class(handles: Set[str]) -> Optional[str]:
    if not handles:
        return None
    for cls, wanted in NON_FILM_COLLECTION_HANDLES.items():
        if handles & wanted:
            return cls
    return None


def _has_film_format(tokens: Set[str]) -> bool:
    for tok in tokens:
        compact = tok.replace(" ", "").replace("-", "")
        if tok in FILM_FORMAT_TOKENS or compact in {"bluray", "4kuhd", "uhdbluray", "steelbook"}:
            return True
        if "blu" in tok and "ray" in tok:
            return True
        if "4k" in tok or "uhd" in tok or tok == "dvd" or "steelbook" in tok:
            return True
    return False


def _has_token(tokens: Set[str], wanted: frozenset[str]) -> bool:
    for tok in tokens:
        if tok in wanted:
            return True
        compact = tok.replace(" ", "").replace("-", "")
        for w in wanted:
            if compact == w.replace(" ", "").replace("-", ""):
                return True
    return False


def classify_product_class(
    *,
    title: str,
    product_id: str,
    product_type: str = "",
    format_value: str = "",
    media_format: str = "",
    tags: Optional[Sequence[str]] = None,
    collection_handles: Optional[Set[str]] = None,
    soundtrack_product_ids: Optional[Set[str]] = None,
) -> tuple[str, str]:
    """
    Return (product_class, reason).

    Non-film classes always win over Region B / GBP supplier eligibility.
    Ambiguous → not film (caller must SKIP).
    """
    tags = list(tags or [])
    handles = {(h or "").casefold() for h in (collection_handles or set()) if h}
    title_cf = (clean_text(title) or "").casefold()
    fmt = format_value or media_format
    tokens = format_tokens(fmt)

    listing_row = {
        "shopify_product_id": product_id,
        "product_title": title,
        "title": title,
        "product_type": product_type,
        "media_format": format_value or media_format,
        "format": format_value or media_format,
        "collection_handles": ",".join(sorted(handles)),
    }

    if is_vinyl_soundtrack_listing(listing_row, soundtrack_product_ids=soundtrack_product_ids):
        if _has_token(tokens, CD_FORMAT_TOKENS):
            return PRODUCT_CLASS_CD, "non_film:cd:soundtracks_or_format"
        return PRODUCT_CLASS_VINYL, "non_film:vinyl:soundtracks_collection_or_format"

    if SOUNDTRACKS_COLLECTION_HANDLE in handles:
        if _has_token(tokens, CD_FORMAT_TOKENS):
            return PRODUCT_CLASS_CD, "non_film:cd:soundtracks_collection"
        return PRODUCT_CLASS_VINYL, "non_film:vinyl:soundtracks_collection"

    if _has_token(tokens, MUSIC_MEDIA_FORMATS) or _has_token(tokens, frozenset({"vinyl", "lp"})):
        return PRODUCT_CLASS_VINYL, "non_film:vinyl:custom_format"
    if _has_token(tokens, CD_FORMAT_TOKENS):
        return PRODUCT_CLASS_CD, "non_film:cd:custom_format"
    if _has_token(tokens, BOOK_FORMAT_TOKENS):
        return PRODUCT_CLASS_BOOK, "non_film:book:custom_format"
    if _has_token(tokens, GAME_FORMAT_TOKENS):
        return PRODUCT_CLASS_GAME, "non_film:game:custom_format"

    pt_class = _product_type_class(product_type)
    if pt_class == PRODUCT_CLASS_VINYL:
        return PRODUCT_CLASS_VINYL, "non_film:vinyl:product_type"
    if pt_class == PRODUCT_CLASS_CD:
        return PRODUCT_CLASS_CD, "non_film:cd:product_type"
    if pt_class == PRODUCT_CLASS_BOOK:
        return PRODUCT_CLASS_BOOK, "non_film:book:product_type"
    if pt_class == PRODUCT_CLASS_GAME:
        return PRODUCT_CLASS_GAME, "non_film:game:product_type"
    if pt_class == PRODUCT_CLASS_OTHER_NON_FILM:
        return PRODUCT_CLASS_OTHER_NON_FILM, "non_film:other:gift_card_product_type"

    coll = _collection_class(handles)
    if coll == PRODUCT_CLASS_VINYL:
        return PRODUCT_CLASS_VINYL, "non_film:vinyl:collection"
    if coll == PRODUCT_CLASS_CD:
        return PRODUCT_CLASS_CD, "non_film:cd:collection"
    if coll == PRODUCT_CLASS_BOOK:
        return PRODUCT_CLASS_BOOK, "non_film:book:collection"
    if coll == PRODUCT_CLASS_GAME:
        return PRODUCT_CLASS_GAME, "non_film:game:collection"

    if title_cf in SHOPIFY_II_EXACT_EXCLUDED_PRODUCT_TITLES:
        return PRODUCT_CLASS_OTHER_NON_FILM, "non_film:other:exact_excluded_title"
    if "gift card" in title_cf:
        return PRODUCT_CLASS_OTHER_NON_FILM, "non_film:other:gift_card_title"

    if re.search(r"\b(card game|board game|video ?game|tabletop)\b", title_cf) or (
        "cinephile" in title_cf and re.search(r"\b(expansion|pack|base set)\b", title_cf)
    ):
        return PRODUCT_CLASS_GAME, "non_film:game:title"
    if re.search(r"\b(hardcover|paperback)\b", title_cf) or (
        re.search(r"\bbook\b", title_cf) and "steelbook" not in title_cf
    ):
        return PRODUCT_CLASS_BOOK, "non_film:book:title"
    if re.search(r"\b(vinyl|lp)\b", title_cf) and not re.search(
        r"\b(blu-?ray|4k|uhd|dvd|steelbook)\b", title_cf
    ):
        return PRODUCT_CLASS_VINYL, "non_film:vinyl:title"
    if re.search(r"\b(original soundtrack|ost)\b", title_cf) and not re.search(
        r"\b(blu-?ray|4k|uhd|dvd|steelbook)\b", title_cf
    ):
        if re.search(r"\bcd\b", title_cf):
            return PRODUCT_CLASS_CD, "non_film:cd:title_soundtrack"
        return PRODUCT_CLASS_VINYL, "non_film:vinyl:title_soundtrack"

    tag_blob = " ".join(tags).casefold()
    if re.search(r"\b(vinyl|lp)\b", tag_blob) and not _has_film_format(tokens):
        return PRODUCT_CLASS_VINYL, "non_film:vinyl:tag"
    if re.search(r"\bcd\b", tag_blob) and not _has_film_format(tokens):
        return PRODUCT_CLASS_CD, "non_film:cd:tag"
    if re.search(r"\b(hardcover|paperback)\b", tag_blob) or (
        re.search(r"\bbook\b", tag_blob)
        and "steelbook" not in title_cf
        and "steelbook" not in tag_blob
    ):
        return PRODUCT_CLASS_BOOK, "non_film:book:tag"
    if re.search(
        r"\b(xbox|playstation|nintendo|switch|ps5|ps4|video ?game|card game)\b", tag_blob
    ):
        return PRODUCT_CLASS_GAME, "non_film:game:tag"

    if _has_film_format(tokens) or pt_class == PRODUCT_CLASS_FILM:
        return PRODUCT_CLASS_FILM, "film:format_or_product_type"
    if handles & FILM_COLLECTION_HANDLES:
        return PRODUCT_CLASS_FILM, "film:film_collection"
    if re.search(r"\b(blu-?ray|4k|uhd|dvd|steelbook)\b", title_cf):
        return PRODUCT_CLASS_FILM, "film:title_media_token"

    return PRODUCT_CLASS_AMBIGUOUS, "ambiguous_product_type"


def product_class_to_action(product_class: str, reason: str) -> tuple[str, str]:
    """Map class → (action, reason). Empty action means proceed as film."""
    if product_class == PRODUCT_CLASS_FILM:
        return "", ""
    if product_class == PRODUCT_CLASS_AMBIGUOUS:
        return ACTION_SKIP_AMBIGUOUS_PRODUCT, reason
    return ACTION_OUT_OF_SCOPE_NON_FILM, reason


def is_film_eligible(
    *,
    title: str,
    product_id: str,
    product_type: str = "",
    format_value: str = "",
    media_format: str = "",
    tags: Optional[Sequence[str]] = None,
    collection_handles: Optional[Set[str]] = None,
    soundtrack_product_ids: Optional[Set[str]] = None,
) -> tuple[bool, str, str]:
    """
    Production gate helper.

    Returns (ok, product_class, reason). ok is True only for film.
    """
    product_class, reason = classify_product_class(
        title=title,
        product_id=product_id,
        product_type=product_type,
        format_value=format_value,
        media_format=media_format,
        tags=tags,
        collection_handles=collection_handles,
        soundtrack_product_ids=soundtrack_product_ids,
    )
    return product_class == PRODUCT_CLASS_FILM, product_class, reason
