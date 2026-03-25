from app.db.client import get_db_client


class RawImportsRepository:
    def __init__(self):
        self.db = get_db_client()

    def latest_batch_id(self, table: str):
        rows = (
            self.db.table(table)
            .select('import_batch_id,imported_at')
            .order('imported_at', desc=True)
            .limit(1)
            .execute()
            .data
            or []
        )
        return rows[0].get('import_batch_id') if rows else None
