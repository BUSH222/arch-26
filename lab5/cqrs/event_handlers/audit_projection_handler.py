import psycopg2  # noqa: F401 #type:ignore
from cqrs.events import StoredEvent
from cqrs.event_handlers.base import EventHandler


class AuditProjectionEventHandler(EventHandler):
    def __init__(self, db_connection: psycopg2.extensions.connection):
        self.db = db_connection

    def can_handle(self, event: StoredEvent) -> bool:
        """This handler processes all user-related events"""
        return event["event_type"] in [
            "UserCreatedEvent",
            "UserUpdatedEvent",
            "UserDeletedEvent"
        ]

    def handle(self, event: StoredEvent) -> None:
        """Record audit entry based on the event"""
        event_type = event["event_type"]

        if event_type == "UserCreatedEvent":
            self._handle_user_created(event)
        elif event_type == "UserUpdatedEvent":
            self._handle_user_updated(event)
        elif event_type == "UserDeletedEvent":
            self._handle_user_deleted(event)

    def _handle_user_created(self, event: StoredEvent) -> None:
        """Record user creation in audit trail"""
        event_data = event["event_data"]
        aggregate_id = event["aggregate_id"]

        with self.db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_audit_projection
                    (aggregate_id, change_type, new_value, changed_at, changed_by)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    aggregate_id,
                    "created",
                    self._event_data_to_json(event_data),
                    event_data["created_at"],
                    "system"
                )
            )
            self.db.commit()

    def _handle_user_updated(self, event: StoredEvent) -> None:
        """Record user update in audit trail"""
        event_data = event["event_data"]
        aggregate_id = event["aggregate_id"]

        with self.db.cursor() as cur:
            cur.execute(
                "SELECT name, email FROM user_projections WHERE id = %s",
                (aggregate_id,)
            )
            row = cur.fetchone()

        old_value = None
        if row:
            old_value = self._dict_to_json({"name": row[0], "email": row[1]})

        with self.db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_audit_projection
                    (aggregate_id, change_type, old_value, new_value, changed_at, changed_by)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    aggregate_id,
                    "updated",
                    old_value,
                    self._event_data_to_json(event_data),
                    event_data["updated_at"],
                    "system"
                )
            )
            self.db.commit()

    def _handle_user_deleted(self, event: StoredEvent) -> None:
        """Record user deletion in audit trail"""
        event_data = event["event_data"]
        aggregate_id = event["aggregate_id"]

        with self.db.cursor() as cur:
            cur.execute(
                "SELECT name, email FROM user_projections WHERE id = %s",
                (aggregate_id,)
            )
            row = cur.fetchone()

        old_value = None
        if row:
            old_value = self._dict_to_json({"name": row[0], "email": row[1]})

        with self.db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_audit_projection
                    (aggregate_id, change_type, old_value, changed_at, changed_by)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    aggregate_id,
                    "deleted",
                    old_value,
                    event_data["deleted_at"],
                    "system"
                )
            )
            self.db.commit()

    @staticmethod
    def _event_data_to_json(event_data: dict) -> str:
        """Convert event data dict to JSON for storage"""
        import json
        return json.dumps(event_data)

    @staticmethod
    def _dict_to_json(data: dict) -> str:
        """Convert dict to JSON for storage"""
        import json
        return json.dumps(data)
