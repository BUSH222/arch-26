import psycopg2
from typing import Optional, Dict, Any


class WriteUserRepository:
    """Repository for write operations on users (PostgreSQL)"""
    
    def __init__(self, db_connection: psycopg2.extensions.connection):
        self.db = db_connection
    
    def create_user(self, name: str, email: str) -> Dict[str, Any]:
        """Create a new user and return the created user data"""
        with self.db.cursor() as cur:
            cur.execute(
                "INSERT INTO users (name, email) VALUES (%s, %s) RETURNING id",
                (name, email)
            )
            user_id = cur.fetchone()[0]
            self.db.commit()
        
        return {
            "id": user_id,
            "name": name,
            "email": email
        }
    
    def update_user(
        self, 
        user_id: int, 
        name: Optional[str] = None, 
        email: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Update an existing user and return the updated user data"""
        # First fetch the user to get current values
        with self.db.cursor() as cur:
            cur.execute(
                "SELECT id, name, email FROM users WHERE id=%s",
                (user_id,)
            )
            row = cur.fetchone()
        
        if not row:
            return None
        
        current_name, current_email = row[1], row[2]
        new_name = name if name is not None else current_name
        new_email = email if email is not None else current_email
        
        with self.db.cursor() as cur:
            cur.execute(
                "UPDATE users SET name=%s, email=%s WHERE id=%s",
                (new_name, new_email, user_id)
            )
            self.db.commit()
        
        return {
            "id": user_id,
            "name": new_name,
            "email": new_email
        }
    
    def delete_user(self, user_id: int) -> bool:
        """Delete a user by ID. Returns True if successful, False if user not found"""
        with self.db.cursor() as cur:
            cur.execute(
                "DELETE FROM users WHERE id=%s RETURNING id",
                (user_id,)
            )
            result = cur.fetchone()
            self.db.commit()
        
        return result is not None
