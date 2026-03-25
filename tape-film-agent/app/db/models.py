from enum import Enum
from pydantic import BaseModel
from typing import Optional


class TmdbMatchStatus(str, Enum):
    matched = 'matched'
    not_found = 'not_found'
    no_clean_title = 'no_clean_title'


class CatalogItemOperationalUpdate(BaseModel):
    supplier_stock_status: int
    availability_status: Optional[str] = None
    cost_price: Optional[float] = None
    calculated_sale_price: Optional[float] = None
    media_release_date: Optional[str] = None
    supplier_last_seen_at: str
