from typing import TypedDict
from datetime import datetime


class UserCreatedEvent(TypedDict):
    """Event published when a user is created"""
    id: int
    name: str
    email: str
    created_at: str


class UserUpdatedEvent(TypedDict):
    """Event published when a user is updated"""
    id: int
    name: str
    email: str
    updated_at: str


class UserDeletedEvent(TypedDict):
    """Event published when a user is deleted"""
    id: int
    deleted_at: str
