import json
import time
from rabbitmq_client import get_rabbitmq_connection, EXCHANGE_NAME

def main():
    print("Notification Service starting up...")
    

    connection = get_rabbitmq_connection()
    channel = connection.channel()
    

    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='fanout')
    

    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    

    channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name)
    
    def callback(ch, method, properties, body):
        event_data = json.loads(body)
        print(f"[x] Notification sent! New user created:")
        print(f"    ID: {event_data.get('id')}")
        print(f"    Name: {event_data.get('name')}")
        print(f"    Email: {event_data.get('email')}")
        print("-" * 30)

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
        auto_ack=True
    )

    print("[*] Waiting for user events. To exit press CTRL+C")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Notification Service stopping...")
        connection.close()


if 1:  # __name__ == "__main__":
    print('hello')
    time.sleep(5)
    main()
