from typing import TypedDict, Optional


class UserCreatedEvent(TypedDict):
    """Domain event: User was created"""
    aggregate_id: Optional[str]
    name: str
    email: str
    created_at: str


class UserUpdatedEvent(TypedDict):
    """Domain event: User was updated"""
    aggregate_id: str
    name: Optional[str]
    email: Optional[str]
    updated_at: str


class UserDeletedEvent(TypedDict):
    """Domain event: User was deleted"""
    aggregate_id: str
    deleted_at: str


class StoredEventMetadata(TypedDict):
    """Metadata about a stored event"""
    version: int
    command_id: str
    timestamp: str


class StoredEvent(TypedDict):
    """Event as persisted in event store"""
    event_id: int
    event_type: str                    # "UserCreatedEvent", etc.
    aggregate_id: str
    aggregate_type: str                # "User"
    event_data: dict                   # The domain event payload
    metadata: StoredEventMetadata
    created_at: str
    version: int
