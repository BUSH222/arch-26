from fastapi import APIRouter, Request  # noqa: F401 #type:ignore
import redis  # noqa: F401 #type:ignore
import psycopg2  # noqa: F401 #type:ignore

from cqrs.dtos import GetUserResponseDTO, GetAllUsersResponseDTO
from cqrs.queries.models import GetUserQuery, GetAllUsersQuery
# from cqrs.queries.handlers import GetUserQueryHandler, GetAllUsersQueryHandler
# from cqrs.queries.repository import ReadUserRepository


router = APIRouter(prefix="/queries", tags=["Queries"])


def get_query_handlers(request: Request):
    """Extract handlers from app state"""
    return request.app.state.query_handlers


@router.get(
    "/users/{user_id}",
    response_model=GetUserResponseDTO,
    summary="Get a user by ID",
    description="Query to retrieve a single user. Uses cache-aside pattern."
)
def get_user_query(
    user_id: str,
    http_request: Request
) -> GetUserResponseDTO:
    handlers = get_query_handlers(http_request)
    query = GetUserQuery(user_id=user_id)
    return handlers["get_user"].handle(query)


@router.get(
    "/users",
    response_model=GetAllUsersResponseDTO,
    summary="Get all users",
    description="Query to retrieve all users. Uses cache-aside pattern."
)
def get_all_users_query(
    http_request: Request
) -> GetAllUsersResponseDTO:
    handlers = get_query_handlers(http_request)
    query = GetAllUsersQuery()
    return handlers["get_all_users"].handle(query)
