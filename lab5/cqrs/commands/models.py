from dataclasses import dataclass, field
from typing import Optional
import uuid
from datetime import datetime


@dataclass
class CreateUserCommand:
    """Command to create a new user"""
    name: str
    email: str
    command_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class UpdateUserCommand:
    """Command to update an existing user"""
    user_id: int
    name: Optional[str] = None
    email: Optional[str] = None
    command_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class DeleteUserCommand:
    """Command to delete a user"""
    user_id: int
    command_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=datetime.utcnow)
