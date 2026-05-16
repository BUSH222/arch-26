import json
import uuid
from datetime import datetime
import redis  # noqa: F401 # type: ignore

from cqrs.commands.models import CreateUserCommand, UpdateUserCommand, DeleteUserCommand
from cqrs.events import UserCreatedEvent, UserUpdatedEvent, UserDeletedEvent, StoredEvent
from cqrs.event_store.repository import EventStoreRepository
from cqrs.dtos import CreateUserCommandResponseDTO, CommandResponseDTO


class EventPublisher:
    """Publishes events to RabbitMQ for external subscribers"""

    @staticmethod
    def publish_stored_event(stored_event: StoredEvent):
        """Publish a stored event to RabbitMQ"""
        try:
            from rabbitmq_client import get_rabbitmq_connection, EXCHANGE_NAME
            connection = get_rabbitmq_connection()
            channel = connection.channel()

            channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key='',
                body=json.dumps(stored_event["event_data"])
            )
            connection.close()
        except Exception as e:
            print(f"Failed to publish event to RabbitMQ: {e}")


class CreateUserCommandHandler:
    def __init__(
        self,
        event_store_repo: EventStoreRepository
    ):
        self.event_store = event_store_repo
        self.event_publisher = EventPublisher()

    def handle(self, command: CreateUserCommand) -> CreateUserCommandResponseDTO:
        """Execute the CreateUserCommand"""
        try:
            user_id = str(uuid.uuid4())

            event_data: UserCreatedEvent = {
                "aggregate_id": user_id,
                "name": command.name,
                "email": command.email,
                "created_at": datetime.utcnow().isoformat()
            }

            stored_event = self.event_store.append_event(
                event_type="UserCreatedEvent",
                aggregate_id=user_id,
                event_data=event_data,
                command_id=command.command_id
            )

            self.event_publisher.publish_stored_event(stored_event)

            return CreateUserCommandResponseDTO(
                command_id=command.command_id,
                status="success",
                user_id=user_id
            )

        except Exception as e:
            return CreateUserCommandResponseDTO(
                command_id=command.command_id,
                status="failed",
                message=str(e)
            )


class UpdateUserCommandHandler:
    def __init__(
        self,
        event_store_repo: EventStoreRepository
    ):
        self.event_store = event_store_repo
        self.event_publisher = EventPublisher()

    def handle(self, command: UpdateUserCommand) -> CommandResponseDTO:
        try:
            # Check if user exists via Event Store
            past_events = self.event_store.get_aggregate_events(command.user_id)
            if not past_events or past_events[-1]["event_type"] == "UserDeletedEvent":
                return CommandResponseDTO(
                    command_id=command.command_id,
                    status="failed",
                    message="User not found"
                )

            event_data: UserUpdatedEvent = {
                "aggregate_id": command.user_id,
                "name": command.name,
                "email": command.email,
                "updated_at": datetime.utcnow().isoformat()
            }

            stored_event = self.event_store.append_event(
                event_type="UserUpdatedEvent",
                aggregate_id=command.user_id,
                event_data=event_data,
                command_id=command.command_id
            )

            self.event_publisher.publish_stored_event(stored_event)

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
    def __init__(
        self,
        event_store_repo: EventStoreRepository
    ):
        self.event_store = event_store_repo
        self.event_publisher = EventPublisher()

    def handle(self, command: DeleteUserCommand) -> CommandResponseDTO:
        try:
            # Check if user exists via Event Store
            past_events = self.event_store.get_aggregate_events(command.user_id)
            if not past_events or past_events[-1]["event_type"] == "UserDeletedEvent":
                return CommandResponseDTO(
                    command_id=command.command_id,
                    status="failed",
                    message="User not found"
                )

            event_data: UserDeletedEvent = {
                "aggregate_id": command.user_id,
                "deleted_at": datetime.utcnow().isoformat()
            }

            stored_event = self.event_store.append_event(
                event_type="UserDeletedEvent",
                aggregate_id=command.user_id,
                event_data=event_data,
                command_id=command.command_id
            )

            self.event_publisher.publish_stored_event(stored_event)

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
