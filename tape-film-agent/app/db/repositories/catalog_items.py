from typing import Any, Dict, List

from app.db.client import get_db_client


class CatalogItemsRepository:
    def __init__(self):
        self.db = get_db_client()

    def count_by_tmdb_status(self) -> Dict[str, int]:
        out: Dict[str, int] = {}
        for status in ('matched', 'not_found', 'no_clean_title'):
            out[status] = self.db.table('catalog_items').select('id', count='exact').eq('tmdb_match_status', status).execute().count or 0
        out['pending'] = self.db.table('catalog_items').select('id', count='exact').is_('tmdb_last_refreshed_at', 'null').execute().count or 0
        return out

    def update_operational_fields(self, catalog_id: str, payload: Dict[str, Any]) -> None:
        self.db.table('catalog_items').update(payload).eq('id', catalog_id).execute()

    def linked_count(self) -> int:
        return self.db.table('catalog_items').select('id', count='exact').not_.is_('film_id', 'null').execute().count or 0

    def fetch_for_film_linking(self, limit: int = 2000) -> List[Dict[str, Any]]:
        return (
            self.db.table('catalog_items')
            .select('id,supplier,barcode,title,director,film_released,tmdb_id,tmdb_title,genres,top_cast,country_of_origin,tmdb_poster_path,tmdb_backdrop_path,tmdb_vote_average,tmdb_vote_count,tmdb_popularity')
            .eq('active', True)
            .eq('tmdb_match_status', 'matched')
            .not_.is_('tmdb_id', 'null')
            .limit(limit)
            .execute()
            .data
            or []
        )
