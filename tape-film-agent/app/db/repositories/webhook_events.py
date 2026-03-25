from datetime import datetime, timezone

from app.db.client import get_db_client


class WebhookEventsRepository:
    def __init__(self):
        self.db = get_db_client()

    def exists(self, topic: str, webhook_id: str) -> bool:
        count = (
            self.db.table('webhook_events')
            .select('id', count='exact')
            .eq('topic', topic)
            .eq('webhook_id', webhook_id)
            .execute()
            .count
            or 0
        )
        return count > 0

    def insert_event(self, topic: str, webhook_id: str, payload: dict) -> None:
        self.db.table('webhook_events').insert({
            'topic': topic,
            'webhook_id': webhook_id,
            'payload': payload,
            'received_at': datetime.now(timezone.utc).isoformat(),
        }).execute()
