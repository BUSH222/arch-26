import json
from typing import Optional, Dict, Any, List
import redis
import psycopg2


class ReadUserRepository:
    """Repository for read operations on users (Redis + PostgreSQL)"""
    
    CACHE_TTL = 60
    
    def __init__(
        self,
        redis_client: redis.Redis,
        db_connection: psycopg2.extensions.connection
    ):
        self.redis = redis_client
        self.db = db_connection
    
    @staticmethod
    def cache_key(user_id: int) -> str:
        return f"user:{user_id}"
    
    @staticmethod
    def all_users_cache_key() -> str:
        return "users:all"
    
    def get_user(self, user_id: int) -> tuple[Optional[Dict[str, Any]], str]:
        """
        Retrieve a user by ID using cache-aside pattern.
        Returns: (user_data, cache_state) where cache_state is "hit" or "miss"
        """
        key = self.cache_key(user_id)
        
        # Try cache first
        cached = self.redis.get(key)
        if cached:
            return (json.loads(cached), "hit")
        
        # Cache miss - fetch from database
        with self.db.cursor() as cur:
            cur.execute(
                "SELECT id, name, email FROM users WHERE id=%s",
                (user_id,)
            )
            row = cur.fetchone()
        
        if not row:
            return (None, "miss")
        
        user = {
            "id": row[0],
            "name": row[1],
            "email": row[2]
        }
        
        # Populate cache
        self.redis.setex(key, self.CACHE_TTL, json.dumps(user))
        
        return (user, "miss")
    
    def get_all_users(self) -> tuple[List[Dict[str, Any]], str]:
        """
        Retrieve all users using cache-aside pattern.
        Returns: (users_list, cache_state) where cache_state is "hit" or "miss"
        """
        key = self.all_users_cache_key()
        
        # Try cache first
        cached = self.redis.get(key)
        if cached:
            return (json.loads(cached), "hit")
        
        # Cache miss - fetch from database
        with self.db.cursor() as cur:
            cur.execute("SELECT id, name, email FROM users ORDER BY id")
            rows = cur.fetchall()
        
        users = [
            {
                "id": r[0],
                "name": r[1],
                "email": r[2]
            }
            for r in rows
        ]
        
        # Populate cache
        self.redis.setex(key, self.CACHE_TTL, json.dumps(users))
        
        return (users, "miss")
