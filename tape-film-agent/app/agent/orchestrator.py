from app.types import RequestContext
from app.agent.query_parser import parse_query
from app.agent.search import search_catalog
from app.agent.ranking import rank_results
from app.agent.offer_selection import select_offer
from app.agent.response_builder import build_response


def run_agent_search(query: str, context: RequestContext):
    parsed = parse_query(query)

    candidates = search_catalog(
        parsed_query=parsed,
        customer_id=context.customer_id,
    )

    ranked = rank_results(
        parsed_query=parsed,
        candidates=candidates,
    )

    selected = select_offer(
        parsed_query=parsed,
        ranked_results=ranked,
    )

    return build_response(
        parsed_query=parsed,
        ranked_results=ranked,
        selected_offer=selected,
    )
