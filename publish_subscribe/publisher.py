from typing import Optional
import pika


EXCHANGE_NAME = "publications"


if __name__ == '__main__':
    connection: Optional[pika.BlockingConnection] = None
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        channel = connection.channel()
        channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="fanout")
        while True:
            message = input("Message: ")
            channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key='',
                body=message.encode()
            )
    except KeyboardInterrupt:
        print("Exit")
    finally:
        if connection is not None:
            connection.close()
