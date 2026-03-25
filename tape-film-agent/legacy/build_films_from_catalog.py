"""
Build / link the `films` table from already-enriched `catalog_items`.

This script does NOT call TMDB or run enrichment.

--------------------------------------------------------------------------------
HOW TO RUN (copy/paste from repo root)
--------------------------------------------------------------------------------

  Daily / operational (default — RECOMMENDED for cron and normal use):
    - Only links rows that still need a film: film_id IS NULL
    - Safe: does not relink or touch already-linked catalog rows

    venv/bin/python build_films_from_catalog.py

  Recovery / explicit full resync (NOT for daily use):
    - Relinks EVERY matched row with a tmdb_id, even if film_id is already set
    - Use after: matching-logic changes, suspicious relink cleanup, one-off repair

    venv/bin/python build_films_from_catalog.py --full-rebuild

  Help:
    venv/bin/python build_films_from_catalog.py -h

--------------------------------------------------------------------------------
ROW FILTERS
--------------------------------------------------------------------------------

  Always (both modes):
    active = true, tmdb_match_status = 'matched', tmdb_id IS NOT NULL

  Default mode only (adds):
    film_id IS NULL

--------------------------------------------------------------------------------
Resilience: retries transient httpx errors; prefetches existing `films` rows
to avoid one HTTP request per tmdb_id.
"""
import argparse
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List

import httpx
from dotenv import load_dotenv
from supabase import create_client

# Supabase long runs often hit HTTP/2 stream limits if we open thousands of requests.
_TRANSIENT_EXC = (
    httpx.RemoteProtocolError,
    httpx.ConnectError,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.ConnectTimeout,
    httpx.PoolTimeout,
)


def execute_with_retry(build_execute: Callable[[], Any], *, max_retries: int = 10, label: str = "") -> Any:
    """Run a Supabase query (.execute()). Rebuild the chain each attempt via build_execute()."""
    delay = 1.0
    last_err: Exception | None = None
    for attempt in range(max_retries):
        try:
            return build_execute().execute()
        except _TRANSIENT_EXC as exc:
            last_err = exc
            if attempt == max_retries - 1:
                raise
            tag = f" [{label}]" if label else ""
            print(f"WARN: transient HTTP error{tag} ({exc!r}); retry {attempt + 1}/{max_retries} in {delay:.1f}s")
            time.sleep(delay)
            delay = min(delay * 2, 90.0)
    raise RuntimeError("unreachable") from last_err


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def chunked(items: Iterable[Any], size: int) -> Iterable[list[Any]]:
    batch: list[Any] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def fetch_matched_catalog_rows(
    supabase,
    select_cols: str,
    page_size: int = 300,
    *,
    only_unlinked: bool = True,
) -> List[Dict[str, Any]]:
    """Smaller pages + retries reduce chance of huge responses killing HTTP/2."""
    out: List[Dict[str, Any]] = []
    offset = 0
    while True:
        off = offset

        def _page():
            q = (
                supabase.table("catalog_items")
                .select(select_cols)
                .eq("active", True)
                .eq("tmdb_match_status", "matched")
                .not_.is_("tmdb_id", "null")
            )
            if only_unlinked:
                q = q.is_("film_id", "null")
            return q.range(off, off + page_size - 1)

        label = f"catalog_items offset={off}"
        if only_unlinked:
            label += " (film_id is null)"
        resp = execute_with_retry(_page, label=label)
        page = resp.data or []
        if not page:
            break
        out.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return out


def fetch_existing_films_by_tmdb_id(supabase, page_size: int = 1000) -> Dict[int, Any]:
    """One paginated scan — avoids per-tmdb_id lookups (major HTTP/2 saver)."""
    by_tmdb: Dict[int, Any] = {}
    offset = 0
    while True:
        off = offset

        def _page():
            return (
                supabase.table("films")
                .select("id,tmdb_id")
                .not_.is_("tmdb_id", "null")
                .range(off, off + page_size - 1)
            )

        resp = execute_with_retry(_page, label=f"films offset={off}")
        rows = resp.data or []
        if not rows:
            break
        for row in rows:
            tid = row.get("tmdb_id")
            if tid is not None:
                by_tmdb[int(tid)] = row["id"]
        if len(rows) < page_size:
            break
        offset += page_size
    return by_tmdb


def pick_representative(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    # Prefer metadata-rich rows first, then supplier priority.
    priority = {"Tape Film": 0, "moovies": 1, "Moovies": 1, "lasgo": 2, "Lasgo": 2}

    def richness_score(row: Dict[str, Any]) -> int:
        score = 0
        if row.get("tmdb_id"):
            score += 100
        if clean_text(row.get("tmdb_title")):
            score += 50
        if clean_text(row.get("genres")):
            score += 10
        if clean_text(row.get("top_cast")):
            score += 10
        if clean_text(row.get("country_of_origin")):
            score += 10
        if row.get("tmdb_vote_count") is not None:
            score += 5
        if row.get("tmdb_vote_average") is not None:
            score += 5
        if row.get("tmdb_popularity") is not None:
            score += 5
        if clean_text(row.get("tmdb_poster_path")):
            score += 3
        if clean_text(row.get("tmdb_backdrop_path")):
            score += 3
        return score

    return sorted(
        rows,
        key=lambda r: (
            -richness_score(r),
            priority.get(clean_text(r.get("supplier")) or "", 9),
            0 if clean_text(r.get("title")) else 1,
        ),
    )[0]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Link catalog_items to films by tmdb_id (no TMDB calls)."
    )
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help=(
            "Process every matched row with tmdb_id, even if film_id is already set. "
            "For recovery, relink repair, or explicit resync only — not for daily ops."
        ),
    )
    args = parser.parse_args()
    only_unlinked = not args.full_rebuild

    load_dotenv(".env")
    sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))

    mode = "operational (only film_id IS NULL)" if only_unlinked else "FULL REBUILD (relink all matched)"
    print(f"Mode: {mode}")

    print("Loading matched catalog_items (paginated)…")
    rows = fetch_matched_catalog_rows(
        sb,
        "id,supplier,barcode,title,director,film_released,studio,genres,top_cast,country_of_origin,tmdb_id,tmdb_title,tmdb_poster_path,tmdb_backdrop_path,tmdb_vote_average,tmdb_vote_count,tmdb_popularity,film_id",
        only_unlinked=only_unlinked,
    )
    print(f"Loaded {len(rows)} catalog rows.")
    if not rows:
        print("Nothing to do. Created 0 films and linked 0 catalog rows (skipped_no_tmdb=0)")
        return

    print("Loading existing films (tmdb_id -> id)…")
    films_by_tmdb = fetch_existing_films_by_tmdb_id(sb)
    print(f"Loaded {len(films_by_tmdb)} existing films.")

    groups: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        tmdb_id = row.get("tmdb_id")
        barcode = clean_text(row.get("barcode"))
        if tmdb_id:
            key = f"tmdb:{tmdb_id}"
        elif barcode:
            key = f"bc:{barcode}"
        else:
            continue
        groups.setdefault(key, []).append(row)

    linked_count = 0
    films_created = 0
    skipped_no_tmdb = 0

    for key, grouped in groups.items():
        rep = pick_representative(grouped)
        tmdb_id = rep.get("tmdb_id")
        if not tmdb_id:
            skipped_no_tmdb += len(grouped)
            continue
        film_id = films_by_tmdb.get(int(tmdb_id))

        if not film_id:
            # tmdb-only insert path: no fallback film creation in this production builder.
            payload = {
                "title": clean_text(rep.get("tmdb_title")) or clean_text(rep.get("title")),
                "original_title": None,
                "film_released": rep.get("film_released"),
                "director": clean_text(rep.get("director")),
                "tmdb_id": tmdb_id,
                "tmdb_title": clean_text(rep.get("tmdb_title")),
                "genres": clean_text(rep.get("genres")),
                "top_cast": clean_text(rep.get("top_cast")),
                "country_of_origin": clean_text(rep.get("country_of_origin")),
                "tmdb_poster_path": clean_text(rep.get("tmdb_poster_path")),
                "tmdb_backdrop_path": clean_text(rep.get("tmdb_backdrop_path")),
                "tmdb_vote_average": rep.get("tmdb_vote_average"),
                "tmdb_vote_count": rep.get("tmdb_vote_count"),
                "tmdb_popularity": rep.get("tmdb_popularity"),
                "metadata_source": "tmdb",
            }

            def _insert():
                return sb.table("films").insert(payload)

            ins = execute_with_retry(_insert, label=f"insert film tmdb_id={tmdb_id}")
            inserted = ins.data or []
            if not inserted:
                continue
            film_id = inserted[0]["id"]
            films_by_tmdb[int(tmdb_id)] = film_id
            films_created += 1

        row_ids = [r["id"] for r in grouped]
        for batch in chunked(row_ids, 500):
            ids = list(batch)

            def _upd():
                return (
                    sb.table("catalog_items")
                    .update(
                        {
                            "film_id": film_id,
                            "film_link_status": "linked",
                            "film_link_method": "tmdb_id" if key.startswith("tmdb:") else "barcode",
                            "film_linked_at": now_iso(),
                        }
                    )
                    .in_("id", ids)
                )

            execute_with_retry(_upd, label=f"link {len(ids)} catalog_items")
            linked_count += len(batch)

    print(
        f"Created {films_created} films and linked {linked_count} catalog rows "
        f"(skipped_no_tmdb={skipped_no_tmdb})"
    )


if __name__ == "__main__":
    main()

