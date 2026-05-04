from typing import TypedDict, Optional
from datetime import datetime


# ============== DOMAIN EVENTS (business events) ==============

class UserCreatedEvent(TypedDict):
    """Domain event: User was created"""
    aggregate_id: Optional[int]  # None initially, assigned after store
    name: str
    email: str
    created_at: str


class UserUpdatedEvent(TypedDict):
    """Domain event: User was updated"""
    aggregate_id: int
    name: Optional[str]
    email: Optional[str]
    updated_at: str


class UserDeletedEvent(TypedDict):
    """Domain event: User was deleted"""
    aggregate_id: int
    deleted_at: str


# ============== STORED EVENTS (what persists in event store) ==============

class StoredEventMetadata(TypedDict):
    """Metadata about a stored event"""
    version: int
    command_id: str
    timestamp: str


class StoredEvent(TypedDict):
    """Event as persisted in event store"""
    event_id: int
    event_type: str                    # "UserCreatedEvent", etc.
    aggregate_id: int
    aggregate_type: str                # "User"
    event_data: dict                   # The domain event payload
    metadata: StoredEventMetadata
    created_at: str
    version: int
