import json
import pika  # noqa: F401 #type:ignore
from typing import TypedDict
import time

RABBITMQ_HOST = "rabbitmq"
EXCHANGE_NAME = "user_events"


class UserCreatedEvent(TypedDict):
    id: int
    name: str
    email: str


def get_rabbitmq_connection():
    for _ in range(10):
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST)
            )
            return connection
        except pika.exceptions.AMQPConnectionError:
            time.sleep(2)
    raise Exception("Could not connect to RabbitMQ")


def setup_rabbitmq():
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='fanout')
    connection.close()


def publish_user_created_event(event: UserCreatedEvent):
    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()

        channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key='',
            body=json.dumps(event)
        )
        connection.close()
    except Exception as e:
        print(f"Failed to publish event: {e}")
