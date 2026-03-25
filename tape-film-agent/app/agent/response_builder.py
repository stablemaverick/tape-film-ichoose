def build_response(
    parsed_query: dict,
    ranked_results: list[dict],
    selected_offer: dict | None,
) -> dict:
    return {
        "parsed_query": parsed_query,
        "results": ranked_results,
        "selected_offer": selected_offer,
    }# Response builder
