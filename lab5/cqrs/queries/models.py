from dataclasses import dataclass, field
import uuid
from datetime import datetime


@dataclass
class GetUserQuery:
    """Query to retrieve a single user by ID"""
    user_id: int
    query_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class GetAllUsersQuery:
    """Query to retrieve all users"""
    query_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=datetime.utcnow)
