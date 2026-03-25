def select_offer(parsed_query: dict, ranked_results: list[dict]) -> dict | None:
    return ranked_results[0] if ranked_results else None# Offer selection logic
