from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis
import psycopg2
import json
import time


time.sleep(2)


app = FastAPI(title="Cache-Aside Demo API")

redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)

db = psycopg2.connect(
    host="postgres",
    database="demo",
    user="demo",
    password="demo"
)

CACHE_TTL = 60


class UserCreate(BaseModel):
    name: str
    email: str


class UserUpdate(BaseModel):
    name: str | None = None
    email: str | None = None


def cache_key(user_id: int):
    return f"user:{user_id}"


def fetch_user_from_db(user_id: int):
    with db.cursor() as cur:
        cur.execute(
            "SELECT id, name, email FROM users WHERE id=%s",
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


@app.get("/ping")
def ping():
    return {"message": "pong"}


@app.post("/init_db")
def init_db():
    init_command = "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT, email TEXT);"
    try:
        with db.cursor() as cur:
            cur.execute(init_command)
            db.commit()

        return {
            "status": "hi"
        }

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/users")
def get_users():
    with db.cursor() as cur:
        cur.execute("SELECT id, name, email FROM users")
        rows = cur.fetchall()

    users = [
        {"id": r[0], "name": r[1], "email": r[2]}
        for r in rows
    ]

    return {"cache_state": "none", "users": users}


@app.get("/users/{user_id}")
def get_user(user_id: int):
    key = cache_key(user_id)

    cached = redis_client.get(key)
    if cached:
        return {
            "cache_state": "hit",
            "user": json.loads(cached)
        }

    user = fetch_user_from_db(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    redis_client.setex(key, CACHE_TTL, json.dumps(user))

    return {
        "cache_state": "miss",
        "user": user
    }


@app.post("/users")
def create_user(user: UserCreate):

    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO users (name,email) VALUES (%s,%s) RETURNING id",
            (user.name, user.email)
        )
        new_id = cur.fetchone()[0]
        db.commit()

    created_user = {
        "id": new_id,
        "name": user.name,
        "email": user.email
    }

    redis_client.setex(
        cache_key(new_id),
        CACHE_TTL,
        json.dumps(created_user)
    )

    return {
        "cache_state": "miss",
        "user": created_user
    }


@app.patch("/users/{user_id}")
def update_user(user_id: int, update: UserUpdate):

    user = fetch_user_from_db(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    new_name = update.name if update.name else user["name"]
    new_email = update.email if update.email else user["email"]

    with db.cursor() as cur:
        cur.execute(
            "UPDATE users SET name=%s, email=%s WHERE id=%s",
            (new_name, new_email, user_id)
        )
        db.commit()

    redis_client.delete(cache_key(user_id))

    updated_user = {
        "id": user_id,
        "name": new_name,
        "email": new_email
    }

    return {
        "cache_state": "miss",
        "user": updated_user
    }


@app.delete("/users/{user_id}")
def delete_user(user_id: int):

    with db.cursor() as cur:
        cur.execute(
            "DELETE FROM users WHERE id=%s RETURNING id",
            (user_id,)
        )
        result = cur.fetchone()
        db.commit()

    if not result:
        raise HTTPException(status_code=404, detail="User not found")

    redis_client.delete(cache_key(user_id))

    return {
        "cache_state": "miss",
        "deleted_user_id": user_id
    }
