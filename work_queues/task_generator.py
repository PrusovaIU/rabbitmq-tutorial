from json import dumps
import pika


QUEUE_NAME = "work_queue"


if __name__ == '__main__':
    try:
        ID = 0
        connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME)
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
                    body=dumps(message).encode()
                )
                print(f"Send ID: {ID}")
                ID += 1
    except KeyboardInterrupt:
        pass
