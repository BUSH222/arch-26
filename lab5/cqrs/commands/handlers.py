import json
from datetime import datetime
from typing import Dict, Any, Optional
import redis

from cqrs.commands.models import CreateUserCommand, UpdateUserCommand, DeleteUserCommand
from cqrs.commands.repository import WriteUserRepository
from cqrs.events import UserCreatedEvent, UserUpdatedEvent, UserDeletedEvent
from cqrs.dtos import CreateUserCommandResponseDTO, CommandResponseDTO


class EventPublisher:
    """Publishes events to RabbitMQ"""
    
    @staticmethod
    def publish_user_created_event(event: UserCreatedEvent):
        """Publish UserCreatedEvent"""
        try:
            from rabbitmq_client import get_rabbitmq_connection, EXCHANGE_NAME
            connection = get_rabbitmq_connection()
            channel = connection.channel()
            
            channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key='',
                body=json.dumps(event)
            )
            connection.close()
        except Exception as e:
            print(f"Failed to publish UserCreatedEvent: {e}")
    
    @staticmethod
    def publish_user_updated_event(event: UserUpdatedEvent):
        """Publish UserUpdatedEvent"""
        try:
            from rabbitmq_client import get_rabbitmq_connection, EXCHANGE_NAME
            connection = get_rabbitmq_connection()
            channel = connection.channel()
            
            channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key='',
                body=json.dumps(event)
            )
            connection.close()
        except Exception as e:
            print(f"Failed to publish UserUpdatedEvent: {e}")
    
    @staticmethod
    def publish_user_deleted_event(event: UserDeletedEvent):
        """Publish UserDeletedEvent"""
        try:
            from rabbitmq_client import get_rabbitmq_connection, EXCHANGE_NAME
            connection = get_rabbitmq_connection()
            channel = connection.channel()
            
            channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key='',
                body=json.dumps(event)
            )
            connection.close()
        except Exception as e:
            print(f"Failed to publish UserDeletedEvent: {e}")


class CacheInvalidator:
    """Invalidates cache entries in Redis"""
    
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
    
    @staticmethod
    def cache_key(user_id: int) -> str:
        return f"user:{user_id}"
    
    def invalidate_user_cache(self, user_id: int):
        """Remove a user from cache"""
        key = self.cache_key(user_id)
        self.redis.delete(key)
    
    def invalidate_all_users_cache(self):
        """Invalidate the all-users cache"""
        # Pattern-based invalidation for all users list
        self.redis.delete("users:all")


class CreateUserCommandHandler:
    """Handler for CreateUserCommand"""
    
    def __init__(
        self,
        repository: WriteUserRepository,
        redis_client: redis.Redis
    ):
        self.repository = repository
        self.cache_invalidator = CacheInvalidator(redis_client)
        self.event_publisher = EventPublisher()
    
    def handle(self, command: CreateUserCommand) -> CreateUserCommandResponseDTO:
        """
        Execute the CreateUserCommand:
        1. Write to database
        2. Publish event
        3. Invalidate cache
        """
        try:
            # Write to database
            created_user = self.repository.create_user(command.name, command.email)
            
            # Publish event
            event: UserCreatedEvent = {
                "id": created_user["id"],
                "name": created_user["name"],
                "email": created_user["email"],
                "created_at": datetime.utcnow().isoformat()
            }
            self.event_publisher.publish_user_created_event(event)
            
            # Invalidate all-users cache
            self.cache_invalidator.invalidate_all_users_cache()
            
            return CreateUserCommandResponseDTO(
                command_id=command.command_id,
                status="success",
                user_id=created_user["id"]
            )
        
        except Exception as e:
            return CreateUserCommandResponseDTO(
                command_id=command.command_id,
                status="failed",
                message=str(e)
            )


class UpdateUserCommandHandler:
    """Handler for UpdateUserCommand"""
    
    def __init__(
        self,
        repository: WriteUserRepository,
        redis_client: redis.Redis
    ):
        self.repository = repository
        self.cache_invalidator = CacheInvalidator(redis_client)
        self.event_publisher = EventPublisher()
    
    def handle(self, command: UpdateUserCommand) -> CommandResponseDTO:
        """
        Execute the UpdateUserCommand:
        1. Update in database
        2. Publish event
        3. Invalidate cache
        """
        try:
            updated_user = self.repository.update_user(
                command.user_id,
                command.name,
                command.email
            )
            
            if not updated_user:
                return CommandResponseDTO(
                    command_id=command.command_id,
                    status="failed",
                    message="User not found"
                )
            
            # Publish event
            event: UserUpdatedEvent = {
                "id": updated_user["id"],
                "name": updated_user["name"],
                "email": updated_user["email"],
                "updated_at": datetime.utcnow().isoformat()
            }
            self.event_publisher.publish_user_updated_event(event)
            
            # Invalidate cache
            self.cache_invalidator.invalidate_user_cache(command.user_id)
            self.cache_invalidator.invalidate_all_users_cache()
            
            return CommandResponseDTO(
                command_id=command.command_id,
                status="success"
            )
        
        except Exception as e:
            return CommandResponseDTO(
                command_id=command.command_id,
                status="failed",
                message=str(e)
            )


class DeleteUserCommandHandler:
    """Handler for DeleteUserCommand"""
    
    def __init__(
        self,
        repository: WriteUserRepository,
        redis_client: redis.Redis
    ):
        self.repository = repository
        self.cache_invalidator = CacheInvalidator(redis_client)
        self.event_publisher = EventPublisher()
    
    def handle(self, command: DeleteUserCommand) -> CommandResponseDTO:
        """
        Execute the DeleteUserCommand:
        1. Delete from database
        2. Publish event
        3. Invalidate cache
        """
        try:
            success = self.repository.delete_user(command.user_id)
            
            if not success:
                return CommandResponseDTO(
                    command_id=command.command_id,
                    status="failed",
                    message="User not found"
                )
            
            # Publish event
            event: UserDeletedEvent = {
                "id": command.user_id,
                "deleted_at": datetime.utcnow().isoformat()
            }
            self.event_publisher.publish_user_deleted_event(event)
            
            # Invalidate cache
            self.cache_invalidator.invalidate_user_cache(command.user_id)
            self.cache_invalidator.invalidate_all_users_cache()
            
            return CommandResponseDTO(
                command_id=command.command_id,
                status="success"
            )
        
        except Exception as e:
            return CommandResponseDTO(
                command_id=command.command_id,
                status="failed",
                message=str(e)
            )
