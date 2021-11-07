"""
02 Work Queues.
Consumers receive messages in a round robin fashion
Competing Consumers Pattern: http://www.enterpriseintegrationpatterns.com/patterns/messaging/CompetingConsumers.html

https://www.rabbitmq.com/tutorials/tutorial-two-python.html

- basic_subscribe(...auto_ack=False...)
    ack is manually sent by consumer
- channel.basic_ack(method.delivery_tag)
    to ack from callback
    ack from the receiver callback only, don't mix delivery tags
- channel.queue_declare(... durable=True)
    makes channel durable
    cannot change durability once declared
- blocking connection is NOT thread-safe
    multiple channels on the same connection throw error!!!
"""
import threading
import time
from functools import partial

from pika import spec, BasicProperties
from pika.adapters.blocking_connection import BlockingChannel

from hello_rabbitmq.examples import DURABLE_QUEUE_NAME, get_channel, connect


def on_message(
        consumer_id: str,  # functools.partial
        channel: BlockingChannel,
        method: spec.Basic.Deliver,
        properties: BasicProperties,
        body: bytes):
    print(f"[consumer {consumer_id}] ### {body.decode('utf-8')}")
    channel.basic_ack(method.delivery_tag)


if __name__ == '__main__':
    # consumer
    def consume(channel: BlockingChannel, n: int):
        print(f'[consumer {n}] ', end='')

        channel.basic_consume(
            queue=DURABLE_QUEUE_NAME,
            on_message_callback=partial(on_message, n),
            auto_ack=False)
        print('subscribed to channel')
        channel.start_consuming()

        while True:
            time.sleep(1)

    for consumer_idx in range(3):
        subscriber_connection = connect()
        subscriber_channel = get_channel(subscriber_connection)
        subscriber_channel.queue_declare(DURABLE_QUEUE_NAME, durable=True)
        subscriber_thread = threading.Thread(target=partial(consume, subscriber_channel, consumer_idx), daemon=True)
        subscriber_thread.start()

    # producer
    producer_connection = connect()
    producer_channel = get_channel(producer_connection)
    producer_channel.queue_declare(DURABLE_QUEUE_NAME, durable=True)

    def produce():
        for i in range(10):
            msg = f'message #{i}'.encode('utf-8')
            producer_channel.basic_publish(exchange='',
                                           routing_key=DURABLE_QUEUE_NAME,
                                           body=msg)


    producer_thread = threading.Thread(target=produce)
    producer_thread.start()

    producer_thread.join()
    print('--- sleep(5) ---')
    time.sleep(5)


