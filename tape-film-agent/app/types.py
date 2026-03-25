from dataclasses import dataclass


@dataclass
class RequestContext:
    shop_domain: str
    customer_id: str | None
    channel: str  # storefront / proxy / admin / webhook / internal
