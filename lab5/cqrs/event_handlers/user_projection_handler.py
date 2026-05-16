import psycopg2  # noqa: F401 #type:ignore
from cqrs.events import StoredEvent
from cqrs.event_handlers.base import EventHandler


class UserProjectionEventHandler(EventHandler):
    """
    Event handler that updates the user_projections table (read model)
    based on domain events. This projection is the single source of truth
    for read operations.
    """

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
        """Update the user_projections table based on the event"""
        event_type = event["event_type"]

        if event_type == "UserCreatedEvent":
            self._handle_user_created(event)
        elif event_type == "UserUpdatedEvent":
            self._handle_user_updated(event)
        elif event_type == "UserDeletedEvent":
            self._handle_user_deleted(event)

    def _handle_user_created(self, event: StoredEvent) -> None:
        """Handle UserCreatedEvent: Insert into user_projections"""
        event_data = event["event_data"]
        aggregate_id = event["aggregate_id"]

        with self.db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_projections (id, name, email, created_at, projection_version)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    email = EXCLUDED.email,
                    created_at = EXCLUDED.created_at,
                    projection_version = EXCLUDED.projection_version
                """,
                (
                    aggregate_id,
                    event_data["name"],
                    event_data["email"],
                    event_data["created_at"],
                    event["version"]
                )
            )

            cur.execute(
                """
                INSERT INTO users_list_projection (id, name, email, created_at, is_active)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    email = EXCLUDED.email,
                    created_at = EXCLUDED.created_at,
                    is_active = EXCLUDED.is_active
                """,
                (
                    aggregate_id,
                    event_data["name"],
                    event_data["email"],
                    event_data["created_at"],
                    True
                )
            )
        self.db.commit()
        print(f"[UserProjection] Created user {aggregate_id}")

    def _handle_user_updated(self, event: StoredEvent) -> None:
        """Handle UserUpdatedEvent: Update user_projections"""
        event_data = event["event_data"]
        aggregate_id = event["aggregate_id"]

        with self.db.cursor() as cur:
            cur.execute(
                """
                UPDATE user_projections
                SET name = %s, email = %s, updated_at = %s, projection_version = %s
                WHERE id = %s
                """,
                (
                    event_data["name"],
                    event_data["email"],
                    event_data["updated_at"],
                    event["version"],
                    aggregate_id
                )
            )
            cur.execute(
                """
                UPDATE users_list_projection
                SET name = %s, email = %s
                WHERE id = %s
                """,
                (
                    event_data["name"],
                    event_data["email"],
                    aggregate_id
                )
            )
        self.db.commit()
        print(f"[UserProjection] Updated user {aggregate_id}")

    def _handle_user_deleted(self, event: StoredEvent) -> None:
        """Handle UserDeletedEvent: Mark as deleted in projectors"""
        event_data = event["event_data"]
        aggregate_id = event["aggregate_id"]

        with self.db.cursor() as cur:
            cur.execute(
                """
                UPDATE user_projections
                SET deleted_at = %s, projection_version = %s
                WHERE id = %s
                """,
                (
                    event_data["deleted_at"],
                    event["version"],
                    aggregate_id
                )
            )
            cur.execute(
                """
                UPDATE users_list_projection
                SET is_active = %s
                WHERE id = %s
                """,
                (
                    False,
                    aggregate_id
                )
            )
        self.db.commit()
        print(f"[UserProjection] Deleted user {aggregate_id}")
