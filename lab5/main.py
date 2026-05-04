from fastapi import FastAPI, HTTPException, Request
import redis
import psycopg2
import time
from rabbitmq_client import setup_rabbitmq
from prometheus_client import make_asgi_app, Counter, Histogram

# Import CQRS routers
from routers import command_routes, query_routes
from cqrs.commands.handlers import (
    CreateUserCommandHandler,
    UpdateUserCommandHandler,
    DeleteUserCommandHandler
)
from cqrs.commands.repository import WriteUserRepository
from cqrs.queries.handlers import GetUserQueryHandler, GetAllUsersQueryHandler
from cqrs.queries.repository import ReadUserRepository

time.sleep(2)

setup_rabbitmq()

app = FastAPI(
    title="Cache-Aside Demo API with CQRS",
    description="Demonstrates CQRS pattern with separate command and query sides"
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


# ============== CQRS Handler Initialization ==============

def init_command_handlers():
    """Initialize all command handlers"""
    write_repo = WriteUserRepository(db)
    
    return {
        "create_user": CreateUserCommandHandler(write_repo, redis_client),
        "update_user": UpdateUserCommandHandler(write_repo, redis_client),
        "delete_user": DeleteUserCommandHandler(write_repo, redis_client),
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

