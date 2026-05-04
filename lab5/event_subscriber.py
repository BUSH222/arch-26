"""
Event Subscriber Service

This service subscribes to domain events from RabbitMQ and updates read model projections.
It runs as a separate process/container alongside the main API.

Key responsibility:
- Listen to events from RabbitMQ
- Route events to appropriate handlers
- Update projections (user_projections, users_list_projection, user_audit_projection)
- Invalidate caches

This ensures read models are built ONLY from events, not in parallel with event publishing.
"""

import json
import time
import psycopg2
import redis
from datetime import datetime
from typing import List

from cqrs.event_store.repository import EventStoreRepository
from cqrs.event_handlers.base import EventHandler
from cqrs.event_handlers.user_projection_handler import UserProjectionEventHandler
from cqrs.event_handlers.cache_invalidation_handler import CacheInvalidationEventHandler
from cqrs.event_handlers.audit_projection_handler import AuditProjectionEventHandler
from rabbitmq_client import get_rabbitmq_connection, EXCHANGE_NAME


class EventSubscriber:
    """
    Subscribes to domain events and updates projections.
    
    This is the "read model builder" - it takes events from RabbitMQ
    and updates all read model projections.
    """
    
    def __init__(
        self,
        db_connection: psycopg2.extensions.connection,
        redis_client: redis.Redis
    ):
        self.db = db_connection
        self.redis = redis_client
        self.event_store = EventStoreRepository(db_connection)
        self.handlers = self._init_handlers()
    
    def _init_handlers(self) -> List[EventHandler]:
        """Initialize all event handlers"""
        return [
            UserProjectionEventHandler(self.db),
            CacheInvalidationEventHandler(self.redis),
            AuditProjectionEventHandler(self.db),
        ]
    
    def start(self) -> None:
        """Start listening to events from RabbitMQ"""
        print("Event Subscriber starting...")
        
        try:
            connection = get_rabbitmq_connection()
            channel = connection.channel()
            
            # Declare exchange (should exist from setup_rabbitmq)
            channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='fanout')
            
            # Create exclusive queue for this subscriber
            result = channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            
            # Bind queue to exchange
            channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name)
            
            print(f"Event Subscriber listening on queue: {queue_name}")
            
            def on_event_received(ch, method, properties, body):
                """Handle incoming event from RabbitMQ"""
                try:
                    event_data = json.loads(body)
                    
                    print(f"[Event] Received: {event_data.get('aggregate_id', 'N/A')}")
                    
                    # The event is published, but we need to find it in the Event Store
                    # to get full metadata (event_id, version, etc.)
                    self._process_event(event_data)
                    
                except Exception as e:
                    print(f"[Error] Failed to process event: {e}")
            
            # Set up consumer
            channel.basic_consume(
                queue=queue_name,
                on_message_callback=on_event_received,
                auto_ack=True
            )
            
            print("[*] Waiting for events. To exit press CTRL+C")
            channel.start_consuming()
            
        except KeyboardInterrupt:
            print("\nEvent Subscriber stopping...")
            connection.close()
        except Exception as e:
            print(f"[Error] Event Subscriber failed: {e}")
    
    def _process_event(self, event_data: dict) -> None:
        """
        Process an event by finding it in Event Store and routing to handlers.
        
        Args:
            event_data: The event data published to RabbitMQ
        """
        # Find the corresponding stored event in event store
        # We match by trying to find the most recent event that matches
        latest_event_id = self.event_store.get_latest_event_id()
        
        # Get recent events (last 10) to find our event
        with self.db.cursor() as cur:
            cur.execute(
                """
                SELECT event_id, event_type, aggregate_id, aggregate_type, 
                       event_data, metadata, created_at, version
                FROM event_store
                WHERE event_id >= %s - 10
                ORDER BY event_id DESC
                LIMIT 10
                """,
                (latest_event_id,)
            )
            rows = cur.fetchall()
        
        # Find matching stored event
        stored_event = None
        for row in rows:
            stored_event_data = json.loads(row[4]) if isinstance(row[4], str) else row[4]
            if self._events_match(event_data, stored_event_data):
                stored_event = {
                    "event_id": row[0],
                    "event_type": row[1],
                    "aggregate_id": row[2],
                    "aggregate_type": row[3],
                    "event_data": stored_event_data,
                    "metadata": json.loads(row[5]) if isinstance(row[5], str) else row[5],
                    "created_at": row[6].isoformat() if row[6] else "",
                    "version": row[7]
                }
                break
        
        if not stored_event:
            print(f"[Warning] Could not find stored event for: {event_data}")
            return
        
        # Route to handlers
        for handler in self.handlers:
            if handler.can_handle(stored_event):
                try:
                    handler.handle(stored_event)
                except Exception as e:
                    print(f"[Error] Handler {handler.__class__.__name__} failed: {e}")
    
    def _events_match(self, published_event: dict, stored_event_data: dict) -> bool:
        """Check if published event matches stored event data"""
        # Compare key fields (aggregate_id and timestamp)
        return (
            published_event.get("aggregate_id") == stored_event_data.get("aggregate_id") and
            published_event.get("name") == stored_event_data.get("name") and
            published_event.get("email") == stored_event_data.get("email")
        )


def main():
    """Main entry point for event subscriber service"""
    print('Event Subscriber Service initializing...')
    time.sleep(5)  # Wait for services to be ready
    
    # Connect to database
    db = psycopg2.connect(
        host="postgres",
        database="demo",
        user="demo",
        password="demo"
    )
    
    # Connect to Redis
    redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)
    
    # Create and start subscriber
    subscriber = EventSubscriber(db, redis_client)
    subscriber.start()


if __name__ == "__main__":
    main()
