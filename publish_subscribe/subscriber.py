from typing import Optional
import pika


EXCHANGE_NAME = "publications"


if __name__ == '__main__':
    connection: Optional[pika.BlockingConnection] = None
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        channel = connection.channel()
        channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="fanout")
        queue = channel.queue_declare(queue='', exclusive=True)
        queue_name = queue.method.queue
        channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name)
        print(f"Subscribe. Queue name: {queue_name}")
        channel.basic_consume(
            queue=queue_name,
            on_message_callback=lambda ch, method, properties, body: print(f"Message: {body.decode()}"),
            auto_ack=True
        )
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Exit")
