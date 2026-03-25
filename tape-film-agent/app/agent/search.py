def search_catalog(parsed_query: dict, customer_id: str | None = None) -> list[dict]:
    return [
        {
            "title": "Alien",
            "format": "4K UHD",
            "customer_id": customer_id,
            "query_used": parsed_query["normalized_query"],
        }
    ]# Search orchestration
