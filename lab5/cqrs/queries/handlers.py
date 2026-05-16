from fastapi import HTTPException   # noqa: F401 #type:ignore

from cqrs.queries.models import GetUserQuery, GetAllUsersQuery
from cqrs.queries.repository import ReadUserRepository
from cqrs.dtos import UserDTO, GetUserResponseDTO, GetAllUsersResponseDTO


class GetUserQueryHandler:
    """Handler for GetUserQuery"""

    def __init__(self, repository: ReadUserRepository):
        self.repository = repository

    def handle(self, query: GetUserQuery) -> GetUserResponseDTO:
        """
        Execute the GetUserQuery:
        1. Try to fetch from cache
        2. Fall back to database
        3. Return with cache state
        """
        try:
            user_data, cache_state = self.repository.get_user(query.user_id)

            if not user_data:
                raise HTTPException(status_code=404, detail="User not found")

            user = UserDTO(**user_data)

            return GetUserResponseDTO(
                cache_state=cache_state,
                user=user
            )

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


class GetAllUsersQueryHandler:
    """Handler for GetAllUsersQuery"""

    def __init__(self, repository: ReadUserRepository):
        self.repository = repository

    def handle(self, query: GetAllUsersQuery) -> GetAllUsersResponseDTO:
        """
        Execute the GetAllUsersQuery:
        1. Try to fetch all users from cache
        2. Fall back to database
        3. Return with cache state
        """
        try:
            users_data, cache_state = self.repository.get_all_users()

            users = [UserDTO(**user_data) for user_data in users_data]

            return GetAllUsersResponseDTO(
                cache_state=cache_state,
                users=users
            )

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
