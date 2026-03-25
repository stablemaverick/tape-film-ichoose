from typing import Any, Dict, List

from app.db.client import get_db_client


class SupplierOffersRepository:
    def __init__(self):
        self.db = get_db_client()

    def fetch_latest_batch_id(self, raw_table: str) -> str | None:
        row = (
            self.db.table(raw_table)
            .select('import_batch_id,imported_at')
            .order('imported_at', desc=True)
            .limit(1)
            .execute()
            .data
            or []
        )
        return row[0].get('import_batch_id') if row else None

    def count_all(self) -> int:
        return self.db.table('staging_supplier_offers').select('id', count='exact').execute().count or 0

    def fetch_operational_offers(self, limit: int = 2000) -> List[Dict[str, Any]]:
        return (
            self.db.table('staging_supplier_offers')
            .select('supplier,barcode,shopify_variant_id,cost_price,calculated_sale_price,availability_status,supplier_stock_status,media_release_date,active')
            .limit(limit)
            .execute()
            .data
            or []
        )
