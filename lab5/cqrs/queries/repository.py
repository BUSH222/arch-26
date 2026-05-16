import json
from typing import Optional, Dict, Any, List
import redis  # noqa: F401 #type:ignore
import psycopg2  # noqa: F401 #type:ignore


class ReadUserRepository:
    CACHE_TTL = 60

    def __init__(
        self,
        redis_client: redis.Redis,
        db_connection: psycopg2.extensions.connection
    ):
        self.redis = redis_client
        self.db = db_connection

    @staticmethod
    def cache_key(user_id: str) -> str:
        return f"user:{user_id}"

    @staticmethod
    def all_users_cache_key() -> str:
        return "users:all"

    def get_user(self, user_id: str) -> tuple[Optional[Dict[str, Any]], str]:
        key = self.cache_key(user_id)

        # Try cache first
        cached = self.redis.get(key)
        if cached:
            return (json.loads(cached), "hit")

        # Cache miss - try projections
        user = self._get_user_from_projection(user_id)

        if not user:
            return (None, "miss")

        # Populate cache
        self.redis.setex(key, self.CACHE_TTL, json.dumps(user))

        return (user, "miss")

    def get_all_users(self) -> tuple[List[Dict[str, Any]], str]:
        key = self.all_users_cache_key()

        # Try cache first
        cached = self.redis.get(key)
        if cached:
            return (json.loads(cached), "hit")

        # Cache miss - try projection
        users = self._get_all_users_from_projection()

        # Populate cache
        self.redis.setex(key, self.CACHE_TTL, json.dumps(users))

        return (users, "miss")

    def _get_user_from_projection(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user from user_projections table (read model built from events)"""
        with self.db.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, email FROM user_projections
                WHERE id = %s AND deleted_at IS NULL
                """,
                (user_id,)
            )
            row = cur.fetchone()

        if not row:
            return None

        return {
            "id": row[0],
            "name": row[1],
            "email": row[2]
        }

    def _get_all_users_from_projection(self) -> List[Dict[str, Any]]:
        """Get all users from users_list_projection table (read model)"""
        with self.db.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, email FROM users_list_projection
                WHERE is_active = TRUE
                ORDER BY id
                """
            )
            rows = cur.fetchall()

        return [
            {
                "id": r[0],
                "name": r[1],
                "email": r[2]
            }
            for r in rows
        ]
