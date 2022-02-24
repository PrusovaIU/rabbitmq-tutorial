from json import loads
from time import sleep
import pika


QUEUE_NAME = "work_queue"


def callback(ch, method, properties, body):
    message = loads(body)
    task_id = message['id']
    print(f"Received ID: {task_id}; Name: {message['name']}")
    sleep(message["time"])
    print(f"ID: {task_id} has been done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == '__main__':
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME)
        channel.basic_consume(
            queue=QUEUE_NAME,
            on_message_callback=callback
        )
        channel.start_consuming()
    except KeyboardInterrupt:
        pass
