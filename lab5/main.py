from fastapi import FastAPI, HTTPException, Request  # noqa: F401 #type:ignore
import redis  # noqa: F401 #type:ignore
import psycopg2  # noqa: F401 #type:ignore
import time
from rabbitmq_client import setup_rabbitmq
from prometheus_client import make_asgi_app, Counter, Histogram  # noqa: F401 #type:ignore

# Import CQRS routers
from routers import command_routes, query_routes
from cqrs.commands.handlers import (
    CreateUserCommandHandler,
    UpdateUserCommandHandler,
    DeleteUserCommandHandler
)
from cqrs.queries.handlers import GetUserQueryHandler, GetAllUsersQueryHandler
from cqrs.queries.repository import ReadUserRepository
from cqrs.event_store.repository import EventStoreRepository

time.sleep(2)

setup_rabbitmq()

app = FastAPI(
    title="Arch project",
    description="gauuau"
)

# Prometheus Metrics
http_requests_total = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint"]
)

http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency",
    ["method", "endpoint"]
)

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.middleware("http")
async def prometheus_middleware(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)

    process_time = time.time() - start_time
    if request.url.path != "/metrics":
        http_requests_total.labels(method=request.method, endpoint=request.url.path).inc()
        http_request_duration_seconds.labels(method=request.method, endpoint=request.url.path).observe(process_time)

    return response

redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)

db = psycopg2.connect(
    host="postgres",
    database="demo",
    user="demo",
    password="demo"
)


def init_event_store_schema():
    """Initialize event store tables if they don't exist"""
    try:
        with open('init_event_store.sql', 'r') as f:
            schema_sql = f.read()

        with db.cursor() as cur:
            cur.execute(schema_sql)
            db.commit()
        print("✓ Event store schema initialized")
    except FileNotFoundError:
        print("⚠ init_event_store.sql not found, skipping schema initialization")
    except Exception as e:
        print(f"⚠ Error initializing event store schema: {e}")


# Initialize Event Store

init_event_store_schema()


# CQRS Handler init

def init_command_handlers():
    """Initialize all command handlers with event sourcing"""
    event_store_repo = EventStoreRepository(db)

    return {
        "create_user": CreateUserCommandHandler(event_store_repo),
        "update_user": UpdateUserCommandHandler(event_store_repo),
        "delete_user": DeleteUserCommandHandler(event_store_repo),
    }


def init_query_handlers():
    """Initialize all query handlers"""
    read_repo = ReadUserRepository(redis_client, db)

    return {
        "get_user": GetUserQueryHandler(read_repo),
        "get_all_users": GetAllUsersQueryHandler(read_repo),
    }


# Set up app state with handlers for dependency injection
app.state.command_handlers = init_command_handlers()
app.state.query_handlers = init_query_handlers()


# ============== Route Registration ==============

app.include_router(command_routes.router)
app.include_router(query_routes.router)


# ============== Utility Endpoints ==============

@app.get("/ping")
def ping():
    return {"message": "pong"}


@app.get("/testalert")
def test_alert():
    return {"message": "Triggered test alert endpoint"}


@app.post("/init_db")
def init_db():
    init_command = "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT, email TEXT);"
    try:
        with db.cursor() as cur:
            cur.execute(init_command)
            db.commit()

        return {
            "status": "Database initialized successfully"
        }

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


# ============== Event Store Inspection Endpoints ==============

@app.get("/event-store")
def get_all_events():
    """Retrieve all events from the event store"""
    try:
        event_store = EventStoreRepository(db)
        events = event_store.get_all_events()

        return {
            "total_events": len(events),
            "events": [
                {
                    "event_id": e["event_id"],
                    "event_type": e["event_type"],
                    "aggregate_id": e["aggregate_id"],
                    "aggregate_type": e["aggregate_type"],
                    "event_data": e["event_data"],
                    "metadata": e["metadata"],
                    "created_at": e["created_at"],
                    "version": e["version"]
                }
                for e in events
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/event-store/aggregate/{aggregate_id}")
def get_aggregate_events(aggregate_id: int):
    """Retrieve all events for a specific aggregate (user)"""
    try:
        event_store = EventStoreRepository(db)
        events = event_store.get_aggregate_events(aggregate_id)

        if not events:
            return {
                "aggregate_id": aggregate_id,
                "total_events": 0,
                "events": []
            }

        return {
            "aggregate_id": aggregate_id,
            "total_events": len(events),
            "events": [
                {
                    "event_id": e["event_id"],
                    "event_type": e["event_type"],
                    "aggregate_id": e["aggregate_id"],
                    "event_data": e["event_data"],
                    "metadata": e["metadata"],
                    "created_at": e["created_at"],
                    "version": e["version"]
                }
                for e in events
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/event-store/type/{event_type}")
def get_events_by_type(event_type: str):
    """Retrieve all events of a specific type"""
    try:
        with db.cursor() as cur:
            cur.execute(
                """
                SELECT event_id, event_type, aggregate_id, aggregate_type,
                       event_data, metadata, created_at, version
                FROM event_store
                WHERE event_type = %s
                ORDER BY event_id ASC
                """,
                (event_type,)
            )
            rows = cur.fetchall()

        events = []
        for row in rows:
            import json
            events.append({
                "event_id": row[0],
                "event_type": row[1],
                "aggregate_id": row[2],
                "aggregate_type": row[3],
                "event_data": json.loads(row[4]) if isinstance(row[4], str) else row[4],
                "metadata": json.loads(row[5]) if isinstance(row[5], str) else row[5],
                "created_at": row[6].isoformat() if row[6] else "",
                "version": row[7]
            })

        return {
            "event_type": event_type,
            "total_events": len(events),
            "events": events
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/event-store/stats")
def get_event_store_stats():
    """Get event store statistics"""
    try:
        with db.cursor() as cur:
            # Total events
            cur.execute("SELECT COUNT(*) FROM event_store")
            total_events = cur.fetchone()[0]

            # Events by type
            cur.execute(
                "SELECT event_type, COUNT(*) as count FROM event_store GROUP BY event_type"
            )
            events_by_type = {row[0]: row[1] for row in cur.fetchall()}

            # Total aggregates
            cur.execute("SELECT COUNT(DISTINCT aggregate_id) FROM event_store WHERE aggregate_id IS NOT NULL")
            total_aggregates = cur.fetchone()[0]

            # Latest event
            cur.execute(
                "SELECT event_id, event_type, created_at FROM event_store ORDER BY event_id DESC LIMIT 1"
            )
            latest = cur.fetchone()

        return {
            "total_events": total_events,
            "total_aggregates": total_aggregates,
            "events_by_type": events_by_type,
            "latest_event": {
                "event_id": latest[0],
                "event_type": latest[1],
                "created_at": latest[2].isoformat() if latest[2] else ""
            } if latest else None
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
