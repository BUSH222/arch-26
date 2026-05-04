from fastapi import APIRouter, Request, HTTPException
import redis
import psycopg2

from cqrs.dtos import (
    CreateUserRequestDTO,
    UpdateUserRequestDTO,
    CreateUserCommandResponseDTO,
    CommandResponseDTO
)
from cqrs.commands.models import (
    CreateUserCommand,
    UpdateUserCommand,
    DeleteUserCommand
)
from cqrs.commands.handlers import (
    CreateUserCommandHandler,
    UpdateUserCommandHandler,
    DeleteUserCommandHandler
)
from cqrs.commands.repository import WriteUserRepository


router = APIRouter(prefix="/commands", tags=["Commands"])


def get_command_handlers(request: Request):
    """Extract handlers from app state"""
    return request.app.state.command_handlers


@router.post(
    "/users/create",
    response_model=CreateUserCommandResponseDTO,
    summary="Create a new user",
    description="Command to create a new user. Publishes UserCreatedEvent."
)
def create_user_command(
    request: CreateUserRequestDTO,
    http_request: Request
) -> CreateUserCommandResponseDTO:
    """
    Create a new user via command.
    
    **Command Flow:**
    1. Write user to PostgreSQL
    2. Publish UserCreatedEvent to RabbitMQ
    3. Invalidate cache
    4. Return command result
    """
    handlers = get_command_handlers(http_request)
    command = CreateUserCommand(name=request.name, email=request.email)
    return handlers["create_user"].handle(command)


@router.patch(
    "/users/{user_id}/update",
    response_model=CommandResponseDTO,
    summary="Update a user",
    description="Command to update an existing user. Publishes UserUpdatedEvent."
)
def update_user_command(
    user_id: int,
    request: UpdateUserRequestDTO,
    http_request: Request
) -> CommandResponseDTO:
    """
    Update an existing user via command.
    
    **Command Flow:**
    1. Update user in PostgreSQL
    2. Publish UserUpdatedEvent to RabbitMQ
    3. Invalidate cache
    4. Return command result
    """
    handlers = get_command_handlers(http_request)
    command = UpdateUserCommand(
        user_id=user_id,
        name=request.name,
        email=request.email
    )
    return handlers["update_user"].handle(command)


@router.delete(
    "/users/{user_id}/delete",
    response_model=CommandResponseDTO,
    summary="Delete a user",
    description="Command to delete a user. Publishes UserDeletedEvent."
)
def delete_user_command(
    user_id: int,
    http_request: Request
) -> CommandResponseDTO:
    """
    Delete a user via command.
    
    **Command Flow:**
    1. Delete user from PostgreSQL
    2. Publish UserDeletedEvent to RabbitMQ
    3. Invalidate cache
    4. Return command result
    """
    handlers = get_command_handlers(http_request)
    command = DeleteUserCommand(user_id=user_id)
    return handlers["delete_user"].handle(command)
