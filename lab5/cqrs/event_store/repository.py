import json
from datetime import datetime
from typing import List, Dict, Any, Optional
import psycopg2  # noqa: F401 #type:ignore

from cqrs.events import StoredEvent


class EventStoreRepository:
    def __init__(self, db_connection: psycopg2.extensions.connection):
        self.db = db_connection

    def append_event(
        self,
        event_type: str,
        aggregate_id: Optional[int],
        event_data: Dict[str, Any],
        command_id: str
    ) -> StoredEvent:
        next_version = 1
        if aggregate_id is not None:
            with self.db.cursor() as cur:
                cur.execute(
                    "SELECT MAX(version) FROM event_store WHERE aggregate_id = %s",
                    (aggregate_id,)
                )
                result = cur.fetchone()
                if result and result[0] is not None:
                    next_version = result[0] + 1

        metadata = {
            "version": next_version,
            "command_id": command_id,
            "timestamp": datetime.utcnow().isoformat()
        }

        with self.db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO event_store
                    (event_type, aggregate_id, aggregate_type, event_data, metadata, version)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING event_id, created_at
                """,
                (
                    event_type,
                    aggregate_id,
                    "User",
                    json.dumps(event_data),
                    json.dumps(metadata),
                    next_version
                )
            )
            result = cur.fetchone()
            event_id, created_at = result
            self.db.commit()

        stored_event: StoredEvent = {
            "event_id": event_id,
            "event_type": event_type,
            "aggregate_id": aggregate_id,
            "aggregate_type": "User",
            "event_data": event_data,
            "metadata": metadata,
            "created_at": created_at.isoformat() if created_at else datetime.utcnow().isoformat(),
            "version": next_version
        }
        return stored_event

    def get_aggregate_events(self, aggregate_id: int) -> List[StoredEvent]:
        """Retrieve all events for an aggregate, ordered by version"""
        with self.db.cursor() as cur:
            cur.execute(
                """
                SELECT
                    event_id, event_type, aggregate_id, aggregate_type,
                    event_data, metadata, created_at, version
                FROM event_store
                WHERE aggregate_id = %s
                ORDER BY version ASC
                """,
                (aggregate_id,)
            )
            rows = cur.fetchall()

        return [self._row_to_stored_event(row) for row in rows]

    def get_events_since(self, since_timestamp: datetime) -> List[StoredEvent]:
        """Get all events since a specific timestamp (for projection catchup)"""
        with self.db.cursor() as cur:
            cur.execute(
                """
                SELECT
                    event_id, event_type, aggregate_id, aggregate_type,
                    event_data, metadata, created_at, version
                FROM event_store
                WHERE created_at >= %s
                ORDER BY created_at ASC
                """,
                (since_timestamp,)
            )
            rows = cur.fetchall()

        return [self._row_to_stored_event(row) for row in rows]

    def get_events_after_id(self, event_id: int) -> List[StoredEvent]:
        """Get all events after a specific event_id (for projection catchup)"""
        with self.db.cursor() as cur:
            cur.execute(
                """
                SELECT
                    event_id, event_type, aggregate_id, aggregate_type,
                    event_data, metadata, created_at, version
                FROM event_store
                WHERE event_id > %s
                ORDER BY event_id ASC
                """,
                (event_id,)
            )
            rows = cur.fetchall()

        return [self._row_to_stored_event(row) for row in rows]

    def get_all_events(self) -> List[StoredEvent]:
        with self.db.cursor() as cur:
            cur.execute(
                """
                SELECT
                    event_id, event_type, aggregate_id, aggregate_type,
                    event_data, metadata, created_at, version
                FROM event_store
                ORDER BY event_id ASC
                """
            )
            rows = cur.fetchall()

        return [self._row_to_stored_event(row) for row in rows]

    def get_latest_event_id(self) -> int:
        """Get the ID of the last stored event"""
        with self.db.cursor() as cur:
            cur.execute("SELECT MAX(event_id) FROM event_store")
            result = cur.fetchone()

        return result[0] if result[0] is not None else 0

    def _row_to_stored_event(self, row) -> StoredEvent:
        """Convert database row to StoredEvent"""
        return {
            "event_id": row[0],
            "event_type": row[1],
            "aggregate_id": row[2],
            "aggregate_type": row[3],
            "event_data": json.loads(row[4]) if isinstance(row[4], str) else row[4],
            "metadata": json.loads(row[5]) if isinstance(row[5], str) else row[5],
            "created_at": row[6].isoformat() if row[6] else "",
            "version": row[7]
        }
