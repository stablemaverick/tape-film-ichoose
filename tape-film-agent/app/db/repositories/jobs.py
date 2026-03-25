from datetime import datetime, timezone
from typing import Any, Dict

from app.db.client import get_db_client


class JobsRepository:
    def __init__(self):
        self.db = get_db_client()

    def record_job_event(self, job_name: str, status: str, payload: Dict[str, Any] | None = None) -> None:
        # Placeholder table: internal_job_runs
        self.db.table('internal_job_runs').insert({
            'job_name': job_name,
            'status': status,
            'payload': payload or {},
            'created_at': datetime.now(timezone.utc).isoformat(),
        }).execute()
