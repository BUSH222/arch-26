from pydantic import BaseModel  # noqa: F401 #type:ignore
from typing import Optional


# ============== COMMAND REQUEST DTOs ==============

class CreateUserRequestDTO(BaseModel):
    name: str
    email: str


class UpdateUserRequestDTO(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None


class DeleteUserRequestDTO(BaseModel):
    pass


# ============== COMMAND RESPONSE DTOs ==============

class CommandResponseDTO(BaseModel):
    command_id: str
    status: str
    message: Optional[str] = None


class CreateUserCommandResponseDTO(CommandResponseDTO):
    user_id: Optional[int] = None


# ============== QUERY REQUEST DTOs ==============

class GetUserQueryDTO(BaseModel):
    user_id: int


class GetAllUsersQueryDTO(BaseModel):
    pass


# ============== QUERY RESPONSE DTOs ==============

class UserDTO(BaseModel):
    id: int
    name: str
    email: str


class GetUserResponseDTO(BaseModel):
    cache_state: str
    user: UserDTO


class GetAllUsersResponseDTO(BaseModel):
    cache_state: str
    users: list[UserDTO]
