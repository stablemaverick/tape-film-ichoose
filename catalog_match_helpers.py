import re


def clean_text(value):
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def normalize_title(value):
    if not value:
        return ""

    text = str(value).lower().strip()

    text = re.sub(r"\b4k\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\buhd\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bblu[\s-]?ray\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bdvd\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\blimited edition\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bcollector'?s edition\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bsteelbook\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bbox set\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bdeluxe edition\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bdeluxe\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bslipcase\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bslipcover\b", "", text, flags=re.IGNORECASE)

    text = re.sub(r"\b3d\s*\+\s*2d\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bseason\s+\d+\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bseries\s+\d+\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bthe collection\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bcomplete legacy collection\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bmovie collection\b", "", text, flags=re.IGNORECASE)

    # normalize separators / wording
    text = re.sub(r"\s*&\s*", " and ", text, flags=re.IGNORECASE)
    text = re.sub(r"\band\b", " and ", text, flags=re.IGNORECASE)

    # common sequel / subtitle forms
    text = re.sub(r"\bvolume\s+1\b", "vol 1", text, flags=re.IGNORECASE)
    text = re.sub(r"\bvolume\s+2\b", "vol 2", text, flags=re.IGNORECASE)
    text = re.sub(r"\bvolume\s+3\b", "vol 3", text, flags=re.IGNORECASE)

    # common supplier-title normalizations
    text = re.sub(r"\bfantastic 4\b", "fantastic four", text, flags=re.IGNORECASE)
    text = re.sub(r"\bmonty pythons\b", "monty python s", text, flags=re.IGNORECASE)
    text = re.sub(r"\bferris buellers\b", "ferris bueller s", text, flags=re.IGNORECASE)
    text = re.sub(r"\bdevils own\b", "devil s own", text, flags=re.IGNORECASE)
    text = re.sub(r"\b310 to yuma\b", "3 10 to yuma", text, flags=re.IGNORECASE)
    text = re.sub(r"\bwall e\b", "wall e", text, flags=re.IGNORECASE)
    text = re.sub(r"\bthe fantastic four\b", "fantastic four", text, flags=re.IGNORECASE)
    text = re.sub(r"\bfantastic 4\b", "fantastic four", text, flags=re.IGNORECASE)
    text = re.sub(r"\bfour\b", "four", text, flags=re.IGNORECASE)


    text = re.sub(r"\(.*?\)|\[.*?\]", "", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"\s-\s", " ", text)
    text = re.sub(r"\s:\s", " ", text)

    return text

def fetch_all_films(supabase):
    response = (
        supabase.table("films")
        .select("""
            id,
            title,
            tmdb_id,
            tmdb_title,
            director,
            film_released,
            country_of_origin,
            genres,
            top_cast,
            tmdb_poster_path,
            tmdb_backdrop_path,
            tmdb_vote_average,
            tmdb_vote_count,
            tmdb_popularity
        """)
        .execute()
    )
    return response.data or []


def find_existing_film_match_by_barcode(supabase, barcode, current_supplier=None):
    barcode = clean_text(barcode)
    if not barcode:
        return None, None, None

    query = (
        supabase.table("catalog_items")
        .select("""
            supplier,
            film_id,
            tmdb_id,
            tmdb_title,
            tmdb_match_status,
            director,
            film_released,
            country_of_origin,
            genres,
            top_cast,
            tmdb_poster_path,
            tmdb_backdrop_path,
            tmdb_vote_average,
            tmdb_vote_count,
            tmdb_popularity,
            tmdb_last_refreshed_at
        """)
        .eq("barcode", barcode)
        .not_.is_("film_id", "null")
    )

    if current_supplier:
        query = query.neq("supplier", current_supplier)

    response = query.execute()
    rows = response.data or []

    if not rows:
        return None, None, None

    def donor_score(row):
        score = 0
        if row.get("tmdb_id"): score += 10
        if row.get("tmdb_title"): score += 5
        if row.get("genres"): score += 4
        if row.get("top_cast"): score += 4
        if row.get("country_of_origin"): score += 3
        if row.get("director"): score += 2
        if row.get("film_released"): score += 2
        if row.get("tmdb_poster_path"): score += 1
        if row.get("tmdb_last_refreshed_at"): score += 1
        return score

    best_row = sorted(rows, key=donor_score, reverse=True)[0]
    return best_row, best_row.get("film_id"), "barcode"


def find_existing_film_by_clean_title(cleaned_title, films_cache):
    if not cleaned_title:
        return None, None

    for film in films_cache:
        film_title = normalize_title(film.get("title"))
        tmdb_title = normalize_title(film.get("tmdb_title"))

        if cleaned_title == film_title or cleaned_title == tmdb_title:
            return film, "local_tmdb_title"

    return None, None


def build_linked_metadata_from_film(film, method):
    if not film:
        return {}

    return {
        "film_id": film.get("id"),
        "film_link_status": "linked",
        "film_link_method": method,
        "tmdb_id": film.get("tmdb_id"),
        "tmdb_title": film.get("tmdb_title") or film.get("title"),
        "tmdb_match_status": "matched" if film.get("tmdb_id") else None,
        "director": film.get("director"),
        "film_released": film.get("film_released"),
        "country_of_origin": film.get("country_of_origin"),
        "genres": film.get("genres"),
        "top_cast": film.get("top_cast"),
        "tmdb_poster_path": film.get("tmdb_poster_path"),
        "tmdb_backdrop_path": film.get("tmdb_backdrop_path"),
        "tmdb_vote_average": film.get("tmdb_vote_average"),
        "tmdb_vote_count": film.get("tmdb_vote_count"),
        "tmdb_popularity": film.get("tmdb_popularity"),
    }


def build_linked_metadata_from_catalog_row(row, method):
    if not row:
        return {}

    return {
        "film_id": row.get("film_id"),
        "film_link_status": "linked",
        "film_link_method": method,
        "tmdb_id": row.get("tmdb_id"),
        "tmdb_title": row.get("tmdb_title"),
        "tmdb_match_status": row.get("tmdb_match_status"),
        "director": row.get("director"),
        "film_released": row.get("film_released"),
        "country_of_origin": row.get("country_of_origin"),
        "genres": row.get("genres"),
        "top_cast": row.get("top_cast"),
        "tmdb_poster_path": row.get("tmdb_poster_path"),
        "tmdb_backdrop_path": row.get("tmdb_backdrop_path"),
        "tmdb_vote_average": row.get("tmdb_vote_average"),
        "tmdb_vote_count": row.get("tmdb_vote_count"),
        "tmdb_popularity": row.get("tmdb_popularity"),
        "tmdb_last_refreshed_at": row.get("tmdb_last_refreshed_at"),
    }


def resolve_existing_film_metadata(supabase, row, films_cache):
    barcode = clean_text(row.get("barcode"))
    title = row.get("title") or ""
    cleaned_title = normalize_title(title)
    current_supplier = row.get("supplier")

    matched_catalog_row, film_id, method = find_existing_film_match_by_barcode(
        supabase,
        barcode,
        current_supplier=current_supplier,
    )
    if matched_catalog_row:
        return build_linked_metadata_from_catalog_row(matched_catalog_row, method)

    film, method = find_existing_film_by_clean_title(cleaned_title, films_cache)
    if film:
        return build_linked_metadata_from_film(film, method)

    return {
        "film_id": None,
        "film_link_status": None,
        "film_link_method": None,
        "tmdb_id": None,
        "tmdb_title": None,
        "tmdb_match_status": None,
        "director": None,
        "film_released": None,
        "country_of_origin": None,
        "genres": None,
        "top_cast": None,
        "tmdb_poster_path": None,
        "tmdb_backdrop_path": None,
        "tmdb_vote_average": None,
        "tmdb_vote_count": None,
        "tmdb_popularity": None,
        "tmdb_last_refreshed_at": None,
    }
