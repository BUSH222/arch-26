import psycopg2
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
            self.db.commit()
    
    def _handle_user_updated(self, event: StoredEvent) -> None:
        """Handle UserUpdatedEvent: Update user_projections"""
        event_data = event["event_data"]
        aggregate_id = event["aggregate_id"]
        
        # Get current user data
        with self.db.cursor() as cur:
            cur.execute(
                "SELECT name, email FROM user_projections WHERE id = %s",
                (aggregate_id,)
            )
            row = cur.fetchone()
        
        if not row:
            return  # User doesn't exist in projection yet
        
        current_name, current_email = row
        
        # Apply partial updates (only update non-None fields)
        new_name = event_data.get("name") or current_name
        new_email = event_data.get("email") or current_email
        
        with self.db.cursor() as cur:
            cur.execute(
                """
                UPDATE user_projections
                SET name = %s, email = %s, updated_at = %s, projection_version = %s
                WHERE id = %s
                """,
                (
                    new_name,
                    new_email,
                    event_data["updated_at"],
                    event["version"],
                    aggregate_id
                )
            )
            self.db.commit()
    
    def _handle_user_deleted(self, event: StoredEvent) -> None:
        """Handle UserDeletedEvent: Mark as deleted in user_projections"""
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
            self.db.commit()
