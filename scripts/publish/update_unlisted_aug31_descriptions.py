#!/usr/bin/env python3
"""One-off: enrich Aug 31 UNLISTED Shopify products with spoiler-free descriptions + metafields."""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from dotenv import load_dotenv

load_dotenv(".env.prod", override=True)

from app.clients.shopify_client import ShopifyClient
from app.clients.supabase_client import create_fresh_client
from app.helpers.text_helpers import clean_text
from app.services.catalog_shopify_publish_service import (
    build_metafields,
    normalize_country_of_origin_for_shopify,
)

PRODUCT_UPDATE = """
mutation ProductUpdate($input: ProductInput!) {
  productUpdate(input: $input) {
    product {
      id
      title
      status
      descriptionHtml
      seo { title description }
    }
    userErrors { field message }
  }
}
"""

METAFIELDS_SET = """
mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields { id namespace key }
    userErrors { field message }
  }
}
"""

# Scope: UNLISTED products created 2026-08-31 up to and including 10147586605280.
UPDATES = [
    {
        "product_id": "gid://shopify/Product/10147584016608",
        "barcode": "5060697923889",
        "director": "Geoffrey Wright",
        "genres": "Crime, Drama",
        "top_cast": "Russell Crowe, Daniel Pollock, Jacqueline McKenzie, Tony Le-Nguyen, Alex Scott",
        "film_released": "1992-11-12",
        "country_of_origin": "Australia",
        "description": (
            "Geoffrey Wright's blistering debut follows a volatile neo-Nazi skinhead gang "
            "in early-1990s Melbourne, led by the magnetic and terrifying Hando (Russell Crowe). "
            "As turf tensions escalate with the local Vietnamese community, the arrival of "
            "troubled outsider Gabrielle (Jacqueline McKenzie) unsettles the bond between Hando "
            "and his loyal lieutenant Davey (Daniel Pollock)."
            "\n\n"
            "Raw, confrontational and still controversial, Romper Stomper is a landmark of "
            "Australian cinema — an unflinching portrait of extremism, loyalty and urban fury "
            "that announced Crowe as a major screen presence."
        ),
    },
    {
        "product_id": "gid://shopify/Product/10147584082144",
        "barcode": "5060697923759",
        "director": "Brian Trenchard-Smith",
        "genres": "Action, Science Fiction, Thriller",
        "top_cast": "Steve Railsback, Olivia Hussey, Michael Craig, Lynda Stoner, Carmen Duncan, Noel Ferrier",
        "film_released": "1982-10-14",
        "country_of_origin": "Australia",
        "description": (
            "In a totalitarian near-future, so-called social deviants are sent to Camp 47 for "
            "brutal re-education. New arrivals Paul (Steve Railsback), Chris (Olivia Hussey) and "
            "Rita (Lynda Stoner) quickly learn the camp's darkest entertainment: a deadly hunt "
            "staged for the amusement of the powerful."
            "\n\n"
            "Brian Trenchard-Smith's notorious Ozploitation classic blends dystopian satire with "
            "full-throttle action and excess — a cult favourite celebrated for its pulp energy "
            "and gleefully unrestrained set pieces."
        ),
    },
    {
        "product_id": "gid://shopify/Product/10147584213216",
        "barcode": "5060974684069",
        "director": "George Armitage",
        "genres": "Crime, Comedy, Thriller",
        "top_cast": "Alec Baldwin, Fred Ward, Jennifer Jason Leigh, Nora Dunn, Charles Napier",
        "film_released": "1990-04-20",
        "country_of_origin": "United States",
        "description": (
            "Fresh out of prison, charismatic sociopath Junior Frenger (Alec Baldwin) arrives in "
            "Miami looking for a fresh start — and promptly resumes his old habits. He falls in "
            "with naïve college student Susie (Jennifer Jason Leigh), while weary detective Hoke "
            "Moseley (Fred Ward) begins circling closer."
            "\n\n"
            "Adapted from Charles Willeford's novel, George Armitage's Miami Blues is a sharp "
            "neo-noir black comedy of crime, impersonation and doomed domestic fantasy on the "
            "Florida coast."
        ),
    },
    {
        "product_id": "gid://shopify/Product/10147584377056",
        "barcode": "5060974682935",
        "director": "Michael Almereyda",
        "genres": "Horror, Drama, Fantasy",
        "top_cast": "Elina Löwensohn, Peter Fonda, Jared Harris, Suzy Amis, Galaxy Craze, Martin Donovan",
        "film_released": "1994-09-11",
        "country_of_origin": "United States",
        "description": (
            "After the death of Count Dracula, his enigmatic daughter Nadja (Elina Löwensohn) "
            "drifts through a dreamlike New York, seeking a new existence beyond her father's "
            "shadow. Vampire hunter Van Helsing (Peter Fonda) is not finished with the family, "
            "and Nadja's reunion with her ailing twin brother Edgar (Jared Harris) draws others "
            "into her nocturnal orbit."
            "\n\n"
            "Executive produced by David Lynch, Michael Almereyda's stylish indie vampire tale "
            "mixes monochrome atmosphere, Pixelvision textures and deadpan humour into a hypnotic "
            "reinvention of the Dracula myth."
        ),
    },
    {
        "product_id": "gid://shopify/Product/10147584868576",
        "barcode": "5060710976533",
        "director": "Mark Neveldine, Brian Taylor",
        "genres": "Action, Science Fiction, Thriller",
        "top_cast": "Gerard Butler, Michael C. Hall, Logan Lerman, Amber Valletta, Terry Crews, Ludacris",
        "film_released": "2009-09-04",
        "country_of_origin": "United States",
        "description": (
            "In a near-future where death-row inmates are wired as live avatars in a brutal online "
            "combat game, champion fighter Kable (Gerard Butler) is controlled by teenage gamer "
            "Simon (Logan Lerman). Behind the spectacle sits tech visionary Ken Castle "
            "(Michael C. Hall), whose empire thrives on turning human lives into entertainment."
            "\n\n"
            "From Crank directors Mark Neveldine and Brian Taylor, Gamer is a hyperkinetic "
            "sci-fi action assault on gaming culture, celebrity and the ethics of control."
        ),
    },
    {
        "product_id": "gid://shopify/Product/10147584966880",
        "barcode": "5060710976618",
        "director": "Thom Eberhardt",
        "genres": "Horror, Comedy, Science Fiction",
        "top_cast": "Catherine Mary Stewart, Kelli Maroney, Robert Beltran, Mary Woronov, Geoffrey Lewis",
        "film_released": "1984-11-16",
        "country_of_origin": "United States",
        "description": (
            "When a passing comet leaves most of humanity as dust — or worse — Valley sisters "
            "Reggie (Catherine Mary Stewart) and Samantha (Kelli Maroney) wake to an eerily empty "
            "Los Angeles. Joined by fellow survivor Hector (Robert Beltran), they cruise deserted "
            "malls and empty streets while strange new threats gather in the shadows."
            "\n\n"
            "Thom Eberhardt's beloved cult gem blends 1980s teen comedy with sci-fi horror "
            "atmosphere — witty, stylish and endlessly rewatchable."
        ),
    },
    {
        "product_id": "gid://shopify/Product/10147585196256",
        "barcode": "5027035031187",
        "director": "Andrew Davis",
        "genres": "Action, Thriller, Crime",
        "top_cast": "Steven Seagal, Pam Grier, Henry Silva, Sharon Stone, Daniel Faraldo",
        "film_released": "1988-04-08",
        "country_of_origin": "United States",
        "description": (
            "Chicago detective Nico Toscani (Steven Seagal) is a former CIA operative and aikido "
            "specialist whose routine narcotics investigation uncovers something far larger. With "
            "partner Delores \"Jacks\" Jackson (Pam Grier), he pushes into a conspiracy that reaches "
            "back to his covert past — and toward ruthless adversary Kurt Zagon (Henry Silva)."
            "\n\n"
            "Andrew Davis's taut action thriller marked Seagal's screen debut and helped define "
            "late-80s martial-arts crime cinema."
        ),
    },
    {
        "product_id": "gid://shopify/Product/10147585327328",
        "barcode": "5027035031194",
        "director": "James Foley",
        "genres": "Action, Crime, Thriller",
        "top_cast": "Chow Yun-Fat, Mark Wahlberg, Ric Young, Byron Mann, Kim Chan",
        "film_released": "1999-03-12",
        "country_of_origin": "United States",
        "description": (
            "NYPD Lieutenant Nick Chen (Chow Yun-Fat) keeps a fragile peace in Chinatown between "
            "rival factions — until a bombing forces him into an uneasy partnership with ambitious "
            "newcomer Danny Wallace (Mark Wahlberg). As gang warfare intensifies, both men find "
            "loyalty, corruption and survival harder to separate."
            "\n\n"
            "James Foley's hard-edged crime thriller pairs Hong Kong star Chow Yun-Fat with "
            "Wahlberg in a story of undercover pressure and moral compromise on New York streets."
        ),
    },
    {
        "product_id": "gid://shopify/Product/10147585425632",
        "barcode": "5063894000292",
        "director": "Ian Tuason",
        "genres": "Horror, Mystery, Thriller",
        "top_cast": "Nina Kiri, Adam DiMarco, Michèle Duquet, Keana Lyn Bastidas, Jeff Yung",
        "film_released": "2026-03-13",
        "country_of_origin": "Canada",
        "description": (
            "Skeptical podcast host Evy (Nina Kiri) co-hosts a paranormal show with believer "
            "Justin (Adam DiMarco). When anonymous recordings from a couple plagued by strange "
            "household noises arrive, Evy — already stretched thin caring for her ailing mother — "
            "begins to hear unsettling echoes in her own life."
            "\n\n"
            "Ian Tuason's A24-backed debut is an intimate, sound-driven supernatural thriller "
            "built on mounting dread, grief and the terror of listening too closely."
        ),
    },
    {
        "product_id": "gid://shopify/Product/10147585720544",
        "barcode": "5056453209441",
        "director": "Keenen Ivory Wayans",
        "genres": "Comedy, Horror",
        "top_cast": "Anna Faris, Marlon Wayans, Shawn Wayans, Regina Hall, Shannon Elizabeth, Carmen Electra",
        "film_released": "2000-07-07",
        "country_of_origin": "United States",
        "description": (
            "A group of teenagers find their quiet suburban lives upended when a masked killer "
            "begins stalking them — and every horror-movie cliché comes along for the ride. "
            "Cindy Campbell (Anna Faris) and her friends race to survive while the film gleefully "
            "skewers late-90s slashers and teen-movie tropes."
            "\n\n"
            "Directed by Keenen Ivory Wayans, Scary Movie launched a smash parody franchise with "
            "broad, no-holds-barred comedy and a breakout turn from Faris."
        ),
    },
    {
        "product_id": "gid://shopify/Product/10147585818848",
        "barcode": "5027035030951",
        "director": "Michael Gornick",
        "genres": "Horror, Comedy",
        "top_cast": "Lois Chiles, George Kennedy, Dorothy Lamour, Page Hannah, Holt McCallany, Tom Savini",
        "film_released": "1987-05-01",
        "country_of_origin": "United States",
        "description": (
            "The Creep returns with three more comic-book nightmares adapted from Stephen King "
            "stories: a wooden Indian statue that exacts a terrible justice, teenagers stranded "
            "on a lake raft with something hungry in the water, and a hit-and-run driver pursued "
            "by an unkillable hitchhiker."
            "\n\n"
            "Directed by Michael Gornick from a George A. Romero screenplay, Creepshow 2 delivers "
            "anthology horror with EC Comics colour, practical effects and dark humour."
        ),
    },
    {
        "product_id": "gid://shopify/Product/10147586506976",
        "barcode": "5050630222216",
        "director": "John G. Avildsen",
        "genres": "Drama, Action, Family",
        "top_cast": "Ralph Macchio, Pat Morita, Elisabeth Shue, Martin Kove, Thomas Ian Griffith",
        "film_released": "1984-06-22",
        "country_of_origin": "United States",
        "description": (
            "Follow Daniel LaRusso (Ralph Macchio) from New Jersey newcomer to tournament "
            "challenger under the patient guidance of Mr. Miyagi (Pat Morita) across the first "
            "three Karate Kid films. Friendship, rivalry and hard-won confidence play out against "
            "Bill Conti's unforgettable score and John G. Avildsen's crowd-pleasing direction."
            "\n\n"
            "This collection brings together the original trilogy that made karate a cultural "
            "phenomenon and turned Macchio and Morita into enduring screen icons."
        ),
    },
    {
        "product_id": "gid://shopify/Product/10147586605280",
        "barcode": "5050630793013",
        "director": "Ivan Reitman",
        "genres": "Comedy, Fantasy, Science Fiction",
        "top_cast": "Bill Murray, Dan Aykroyd, Sigourney Weaver, Harold Ramis, Ernie Hudson, Rick Moranis",
        "film_released": "1984-06-08",
        "country_of_origin": "United States",
        "description": (
            "When New York is overrun by restless spirits, a trio of eccentric parapsychologists "
            "turn ghost-catching into a business — and a cultural phenomenon. Bill Murray, "
            "Dan Aykroyd and Harold Ramis star as the original Ghostbusters, with Sigourney Weaver "
            "and Rick Moranis caught in the supernatural crossfire."
            "\n\n"
            "This set pairs Ivan Reitman's landmark 1984 comedy with its 1989 sequel Ghostbusters II, "
            "bringing back the team for another spectral showdown beneath the city streets."
        ),
    },
]


def _html_description(text: str) -> str:
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    parts = []
    for p in paragraphs:
        escaped = (
            p.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        )
        parts.append(f"<p>{escaped}</p>")
    return "".join(parts)


def _row_for_metafields(p: dict, catalog_id: str | None, supplier_sku: str | None) -> dict:
    return {
        "id": catalog_id,
        "barcode": p["barcode"],
        "supplier": "moovies",
        "supplier_sku": supplier_sku,
        "director": p["director"],
        "studio": None,  # keep existing Shopify studio; do not overwrite via build
        "format": None,
        "genres": p["genres"],
        "top_cast": p["top_cast"],
        "film_released": p["film_released"],
        "media_release_date": None,
        "country_of_origin": p["country_of_origin"],
        "availability_status": "supplier_stock",
        "supplier_stock_status": 0,
    }


def main() -> int:
    dry_run = "--dry-run" in sys.argv
    shopify = ShopifyClient(api_version="2026-04")
    sb = create_fresh_client(".env.prod")

    ok = failed = 0
    for p in UPDATES:
        barcode = p["barcode"]
        product_id = p["product_id"]
        try:
            # Load live product for sku / existing studio/format/region/preorder fields
            live = shopify.graphql(
                """
                query($id: ID!) {
                  product(id: $id) {
                    id title status
                    metafields(first: 40, namespace: "custom") {
                      nodes { key value type }
                    }
                    variants(first: 1) { nodes { sku barcode } }
                  }
                }
                """,
                {"id": product_id},
            )["product"]
            if not live:
                raise RuntimeError("product not found")
            if live.get("status") != "UNLISTED":
                raise RuntimeError(f"expected UNLISTED, got {live.get('status')}")

            existing_mf = {m["key"]: m for m in live["metafields"]["nodes"]}
            variant = (live["variants"]["nodes"] or [{}])[0]
            supplier_sku = clean_text(variant.get("sku")) or clean_text(
                (existing_mf.get("source_supplier_sku") or {}).get("value")
            )

            cat_rows = (
                sb.table("catalog_items")
                .select("id,barcode,studio,format,media_release_date,supplier_sku")
                .eq("barcode", barcode)
                .eq("active", True)
                .execute()
                .data
                or []
            )
            cat = cat_rows[0] if cat_rows else {}
            catalog_id = str(cat.get("id")) if cat.get("id") else None

            html = _html_description(p["description"])
            seo_desc = " ".join(p["description"].split())[:320]

            # Metafields to set: enrich missing film fields; preserve studio/format/region/preorder/media_release
            row = _row_for_metafields(p, catalog_id, supplier_sku)
            # Feed studio/format from live so build_metafields can include them if we want full set;
            # we selectively send only enrichment keys below.
            built = build_metafields(
                {
                    **row,
                    "studio": clean_text((existing_mf.get("studio") or {}).get("value"))
                    or clean_text(cat.get("studio")),
                    "format": clean_text((existing_mf.get("format") or {}).get("value"))
                    or clean_text(cat.get("format")),
                    "media_release_date": clean_text(
                        (existing_mf.get("media_release_date") or {}).get("value")
                    )
                    or clean_text(cat.get("media_release_date")),
                }
            )
            enrich_keys = {
                "director",
                "starring",
                "genre",
                "film_released",
                "country_of_origin",
                "source_supplier_sku",
                "catalog_item_id",
            }
            metafields = []
            for m in built:
                if m["key"] not in enrich_keys:
                    continue
                metafields.append(
                    {
                        "ownerId": product_id,
                        "namespace": m["namespace"],
                        "key": m["key"],
                        "type": m["type"],
                        "value": m["value"],
                    }
                )

            # Ensure country normalization landed
            country = normalize_country_of_origin_for_shopify(p["country_of_origin"])
            if country and not any(m["key"] == "country_of_origin" for m in metafields):
                metafields.append(
                    {
                        "ownerId": product_id,
                        "namespace": "custom",
                        "key": "country_of_origin",
                        "type": "single_line_text_field",
                        "value": country,
                    }
                )

            print(
                f"{'DRY ' if dry_run else ''}UPDATE {barcode} {live['title'][:50]} "
                f"mf={sorted(m['key'] for m in metafields)}"
            )
            if dry_run:
                print("  desc:", seo_desc[:120], "...")
                ok += 1
                continue

            upd = shopify.graphql(
                PRODUCT_UPDATE,
                {
                    "input": {
                        "id": product_id,
                        "descriptionHtml": html,
                        "seo": {"title": live["title"], "description": seo_desc},
                    }
                },
            )
            errs = (upd.get("productUpdate") or {}).get("userErrors") or []
            if errs:
                raise RuntimeError(f"productUpdate: {errs}")

            if metafields:
                mf = shopify.graphql(METAFIELDS_SET, {"metafields": metafields})
                mf_errs = (mf.get("metafieldsSet") or {}).get("userErrors") or []
                if mf_errs:
                    raise RuntimeError(f"metafieldsSet: {mf_errs}")

            # Mirror enrichment into catalog_items
            if catalog_id:
                patch = {
                    "director": p["director"],
                    "genres": p["genres"],
                    "top_cast": p["top_cast"],
                    "film_released": p["film_released"],
                    "country_of_origin": country or p["country_of_origin"],
                }
                if supplier_sku:
                    patch["supplier_sku"] = supplier_sku
                try:
                    sb.table("catalog_items").update(patch).eq("id", catalog_id).execute()
                except Exception as cat_exc:
                    print(f"  WARN catalog update {barcode}: {cat_exc}")

            print(f"  OK product={product_id}")
            ok += 1
            time.sleep(0.2)
        except Exception as exc:
            print(f"FAIL {barcode}: {exc}")
            failed += 1
            time.sleep(0.3)

    print(f"\nDone. ok={ok} failed={failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
