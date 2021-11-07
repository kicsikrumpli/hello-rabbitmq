"""
01 Hello World
https://www.rabbitmq.com/tutorials/tutorial-one-python.html
"""
import threading
import time

from pika import spec, BasicProperties
from pika.adapters.blocking_connection import BlockingChannel

from hello_rabbitmq.examples import connect, get_channel, QUEUE_NAME


def on_message(
        channel: BlockingChannel,
        method: spec.Basic.Deliver,
        properties: BasicProperties,
        body: bytes):
    print(f"[consumer] ### {body.decode('utf-8')}")


if __name__ == '__main__':
    # subscriber
    # shared connection raises
    subscriber_connection = connect()
    subscriber_channel = get_channel(subscriber_connection)
    subscriber_channel.queue_declare(QUEUE_NAME)

    def consume():
        subscriber_channel.basic_consume(
            queue=QUEUE_NAME,
            on_message_callback=on_message,
            auto_ack=True)
        print('subscribed to channel')
        subscriber_channel.start_consuming()
        while True:
            time.sleep(1)

    subscriber_thread = threading.Thread(target=consume, daemon=True)
    subscriber_thread.start()

    # producer
    producer_connection = connect()
    producer_channel = get_channel(producer_connection)
    producer_channel.queue_declare(QUEUE_NAME)

    def produce():
        producer_channel.queue_declare(queue=QUEUE_NAME)
        for i in range(10):
            msg = f'message #{i}'.encode('utf-8')
            producer_channel.basic_publish(exchange='',
                                           routing_key=QUEUE_NAME,
                                           body=msg)


    producer_thread = threading.Thread(target=produce)
    producer_thread.start()

    producer_thread.join()
    print('--- sleep(5) ---')
    time.sleep(5)


