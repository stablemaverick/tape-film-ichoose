"""
Official distributor sources for product description drafting.

Descriptions may only cite official distributor / studio pages. ``DISTRIBUTORS`` maps catalogue
``studio`` values to the domains we trust for that label; ``BLOCKED_DOMAINS`` is a second guard for
retailer, review and fan sites that must never be used as a source.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse


@dataclass(frozen=True)
class DistributorSource:
    key: str
    name: str
    aliases: tuple[str, ...]
    domains: tuple[str, ...]
    shopify_base: Optional[str] = None
    fallback_domains: tuple[str, ...] = field(default_factory=tuple)
    film_page_patterns: tuple[str, ...] = field(default_factory=tuple)

    @property
    def all_domains(self) -> tuple[str, ...]:
        return self.domains + self.fallback_domains


DISTRIBUTORS: tuple[DistributorSource, ...] = (
    DistributorSource("arrow", "Arrow Films", ("arrow films", "arrow video", "arrow academy", "arrow"),
                      ("arrowfilms.com", "arrowvideo.com")),
    DistributorSource("toy_robot", "Toy Robot Video (Arrow Films)", ("toy robot video", "toy robot"),
                      ("toyrobotvideo.co.uk",), fallback_domains=("arrowfilms.com",)),
    DistributorSource("eureka", "Eureka Entertainment",
                      ("eureka entertainment", "eureka classics", "masters of cinema", "eureka"),
                      ("eurekavideo.co.uk",)),
    DistributorSource("radiance", "Radiance Films", ("radiance films", "radiance", "transmission"),
                      ("radiancefilms.co.uk",), shopify_base="https://www.radiancefilms.co.uk"),
    DistributorSource("second_sight", "Second Sight Films", ("second sight films", "second sight"),
                      ("secondsightfilms.co.uk",), shopify_base="https://www.secondsightfilms.co.uk"),
    DistributorSource("criterion", "The Criterion Collection", ("criterion collection", "criterion"),
                      ("criterion.com",)),
    DistributorSource("studiocanal", "StudioCanal", ("studio canal", "studiocanal", "vintage classics"),
                      ("studiocanal.co.uk", "studiocanal.com"),
                      film_page_patterns=("https://www.studiocanal.co.uk/title/{slug}-{year}/",
                                          "https://www.studiocanal.com/title/{slug}-{year}/")),
    DistributorSource("sony", "Sony Pictures Home Entertainment", ("sony pictures", "sony"),
                      ("sonypictures.co.uk", "sonypictures.com"),
                      film_page_patterns=("https://www.sonypictures.co.uk/movies/{slug}",
                                          "https://www.sonypictures.com/movies/{slug}")),
    DistributorSource("disney", "Walt Disney Studios Home Entertainment",
                      ("walt disney", "disney", "marvel", "pixar", "20th century studios"),
                      ("disney.co.uk", "press.disney.co.uk", "disney.com"),
                      film_page_patterns=("https://www.disney.co.uk/movies/{slug}-{year}",
                                          "https://www.disney.co.uk/movies/{slug}")),
    DistributorSource("warner", "Warner Bros. Home Entertainment", ("warner bros", "warner brothers", "warner"),
                      ("warnerbros.co.uk", "warnerbros.com"),
                      film_page_patterns=("https://www.warnerbros.com/movies/{slug}",
                                          "https://www.warnerbros.co.uk/movies/{slug}")),
    DistributorSource("universal", "Universal Pictures Home Entertainment", ("universal pictures", "universal"),
                      ("universalpictures.co.uk", "universalpictures.com")),
    DistributorSource("paramount", "Paramount Home Entertainment", ("paramount pictures", "paramount"),
                      ("paramount.com", "paramountmovies.com", "paramount.co.uk")),
    DistributorSource("lionsgate", "Lionsgate UK", ("lionsgate", "lions gate"),
                      ("lionsgate.co.uk", "lionsgate.com")),
    DistributorSource("curzon", "Curzon Film World", ("curzon film world", "curzon artificial eye", "curzon"),
                      ("curzon.com",)),
    DistributorSource("88_films", "88 Films", ("88 films",), ("88-films.co.uk", "88films.co.uk")),
    DistributorSource("indicator", "Indicator / Powerhouse Films", ("indicator", "powerhouse films", "powerhouse"),
                      ("powerhousefilms.co.uk",)),
    DistributorSource("bfi", "BFI", ("bfi", "british film institute"), ("shop.bfi.org.uk", "bfi.org.uk")),
    DistributorSource("101_films", "101 Films", ("101 films",), ("101films.co.uk",)),
    DistributorSource("vinegar_syndrome", "Vinegar Syndrome", ("vinegar syndrome",),
                      ("vinegarsyndrome.com",), shopify_base="https://vinegarsyndrome.com"),
    DistributorSource("severin", "Severin Films", ("severin films", "severin"),
                      ("severin-films.com",), shopify_base="https://severin-films.com"),
    DistributorSource("imprint", "Imprint Films (Via Vision)", ("imprint films", "imprint", "via vision"),
                      ("viavision.com.au",)),
    DistributorSource("umbrella", "Umbrella Entertainment", ("umbrella entertainment", "umbrella"),
                      ("umbrellaent.com.au",)),
    DistributorSource("kino_lorber", "Kino Lorber", ("kino lorber", "kino"), ("kinolorber.com",)),
    DistributorSource("a24", "A24", ("a24",), ("a24films.com", "shop.a24films.com")),
)

BLOCKED_DOMAINS: frozenset[str] = frozenset({
    "zavvi.com", "hmv.com", "amazon.co.uk", "amazon.com", "ebay.co.uk", "ebay.com", "rarewaves.com",
    "wowhd.co.uk", "base.com", "sainsburys.co.uk", "tesco.com", "cex.co.uk", "rakuten.co.uk",
    "blu-ray.com", "dvdcompare.net", "hidefninja.com", "thedigitalfix.com", "whysoblu.com",
    "highdefdigest.com", "justlovemovies.com", "moviezyne.com", "cineorama.co.uk", "thepeoplesmovies.com",
    "imdb.com", "themoviedb.org", "wikipedia.org", "letterboxd.com", "rottentomatoes.com", "reddit.com",
    "facebook.com", "instagram.com", "x.com", "twitter.com", "youtube.com", "herokuapp.com",
})

INSECURE_TLS_DOMAINS: frozenset[str] = frozenset({"studiocanal.co.uk"})

# Distributor-operated hosts serving the same pages when the public domain blocks automated fetches.
# Only used as a fetch route; the public URL is always what gets cited.
FETCH_MIRRORS: dict[str, str] = {"criterion.com": "https://criterion-v2.herokuapp.com"}


def _norm(value: str) -> str:
    return re.sub(r"[^a-z0-9 ]+", " ", value.lower()).strip()


def resolve_distributor(studio: Optional[str]) -> Optional[DistributorSource]:
    """Match a catalogue ``studio`` value to a registry entry (longest alias wins)."""
    if not studio:
        return None
    text = f" {_norm(studio)} "
    best: tuple[int, Optional[DistributorSource]] = (0, None)
    for dist in DISTRIBUTORS:
        for alias in dist.aliases:
            if f" {_norm(alias)} " in text and len(alias) > best[0]:
                best = (len(alias), dist)
    return best[1]


def url_domain(url: str) -> str:
    host = (urlparse(url).hostname or "").lower()
    return host[4:] if host.startswith("www.") else host


def domain_matches(url: str, domains: tuple[str, ...] | frozenset[str]) -> bool:
    host = url_domain(url)
    return any(host == d or host.endswith("." + d) for d in domains)


def is_blocked_url(url: str) -> bool:
    return domain_matches(url, BLOCKED_DOMAINS)


def is_allowed_url(url: str, dist: Optional[DistributorSource]) -> bool:
    if not url or is_blocked_url(url):
        return False
    return bool(dist) and domain_matches(url, dist.all_domains)
