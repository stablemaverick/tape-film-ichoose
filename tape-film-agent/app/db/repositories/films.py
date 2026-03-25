from typing import Any, Dict, Optional

from app.db.client import get_db_client


class FilmsRepository:
    def __init__(self):
        self.db = get_db_client()

    def find_by_tmdb_id(self, tmdb_id: int) -> Optional[Dict[str, Any]]:
        rows = (
            self.db.table('films')
            .select('*')
            .eq('tmdb_id', tmdb_id)
            .limit(1)
            .execute()
            .data
            or []
        )
        return rows[0] if rows else None

    def insert(self, payload: Dict[str, Any]) -> Dict[str, Any] | None:
        rows = self.db.table('films').insert(payload).execute().data or []
        return rows[0] if rows else None
