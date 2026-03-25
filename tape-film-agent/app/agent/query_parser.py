def parse_query(query: str) -> dict:
    return {
        "raw_query": query,
        "normalized_query": query.strip().lower(),
    }
