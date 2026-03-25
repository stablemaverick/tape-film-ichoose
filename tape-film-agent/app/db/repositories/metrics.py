from typing import Dict

from app.db.client import get_db_client


class MetricsRepository:
    def __init__(self):
        self.db = get_db_client()

    def catalog_snapshot(self) -> Dict[str, int]:
        c = lambda q: q.execute().count or 0
        return {
            'catalog_total': c(self.db.table('catalog_items').select('id', count='exact')),
            'films_total': c(self.db.table('films').select('id', count='exact')),
            'matched': c(self.db.table('catalog_items').select('id', count='exact').eq('tmdb_match_status', 'matched')),
            'not_found': c(self.db.table('catalog_items').select('id', count='exact').eq('tmdb_match_status', 'not_found')),
            'pending': c(self.db.table('catalog_items').select('id', count='exact').is_('tmdb_last_refreshed_at', 'null')),
        }
