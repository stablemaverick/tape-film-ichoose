from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.types import RequestContext
from app.agent.orchestrator import run_agent_search

router = APIRouter(prefix="/agent", tags=["storefront-agent"])


class AgentSearchRequest(BaseModel):
    query: str
    customer_id: str | None = None


@router.post("/search")
def agent_search(payload: AgentSearchRequest):
    context = RequestContext(
        shop_domain="unknown-for-now",
        customer_id=payload.customer_id,
        channel="storefront",
    )

    try:
        result = run_agent_search(
            query=payload.query,
            context=context,
        )

        return {
            "success": True,
            "data": result,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
