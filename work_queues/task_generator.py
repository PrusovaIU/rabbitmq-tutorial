from json import dumps
from typing import Optional
import pika


QUEUE_NAME = "work_queue_durable"


if __name__ == '__main__':
    ID = 0
    connection: Optional[pika.BlockingConnection] = None
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        while True:
            task_name = input("Task name: ")
            execute_time = int(input("Execute time: "))
            task_amount = int(input("Task amount: "))
            message = {
                "name": task_name,
                "time": execute_time
            }
            for i in range(task_amount):
                message["id"] = ID
                channel.basic_publish(
                    exchange='',
                    routing_key=QUEUE_NAME,
                    body=dumps(message).encode(),
                    properties=pika.BasicProperties(
                        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                    )
                )
                print(f"Send ID: {ID}")
                ID += 1
    except KeyboardInterrupt:
        print("Exit")
    finally:
        if connection is not None:
            connection.close()
