from typing import List, Dict

from app.db.client import get_db_client


class WishlistsRepository:
    def __init__(self):
        self.db = get_db_client()

    def list_for_customer(self, customer_id: str) -> List[Dict]:
        return (
            self.db.table('wishlists')
            .select('*')
            .eq('customer_id', customer_id)
            .execute()
            .data
            or []
        )
