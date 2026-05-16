import redis  # noqa: F401 #type:ignore
from cqrs.events import StoredEvent
from cqrs.event_handlers.base import EventHandler


class CacheInvalidationEventHandler(EventHandler):
    """
    Event handler that invalidates Redis cache entries when user data changes.
    Ensures read model freshness by clearing stale cache on events.
    """

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    @staticmethod
    def cache_key(user_id: int) -> str:
        return f"user:{user_id}"

    @staticmethod
    def all_users_cache_key() -> str:
        return "users:all"

    def can_handle(self, event: StoredEvent) -> bool:
        """This handler processes all user-related events"""
        return event["event_type"] in [
            "UserCreatedEvent",
            "UserUpdatedEvent",
            "UserDeletedEvent"
        ]

    def handle(self, event: StoredEvent) -> None:
        """Invalidate cache based on the event"""
        event_type = event["event_type"]

        if event_type == "UserCreatedEvent":
            self._handle_user_created(event)
        elif event_type == "UserUpdatedEvent":
            self._handle_user_updated(event)
        elif event_type == "UserDeletedEvent":
            self._handle_user_deleted(event)

    def _handle_user_created(self, event: StoredEvent) -> None:
        """Invalidate all-users cache on creation"""
        self.redis.delete(self.all_users_cache_key())

    def _handle_user_updated(self, event: StoredEvent) -> None:
        """Invalidate specific user and all-users cache on update"""
        aggregate_id = event["aggregate_id"]
        self.redis.delete(self.cache_key(aggregate_id))
        self.redis.delete(self.all_users_cache_key())

    def _handle_user_deleted(self, event: StoredEvent) -> None:
        """Invalidate specific user and all-users cache on deletion"""
        aggregate_id = event["aggregate_id"]
        self.redis.delete(self.cache_key(aggregate_id))
        self.redis.delete(self.all_users_cache_key())
