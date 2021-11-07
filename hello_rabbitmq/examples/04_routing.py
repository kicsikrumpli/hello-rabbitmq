"""
04 Routing
Exchange routes to queues based on
- exchange type
- routing key

exchange types:
- direct – message goes to the queue with matching routing key
- topic - wilcards in routing key
- fanout – see example 03
- headers - ...

- producer:
    - declare exchange:
        exchange_declare(exchange='logs',
                         exchange_type='direct')
    - publish to exchange instead of queue:
        basic_publish(exchange=exchange_name,
                      routing_key='my-routing-key',
                      body=message)

- consumer:
    - declare exchange:
        exchange_declare(exchange='logs',
                         exchange_type='direct')
    - declare anonymous temporary queue:
        result = queue_declare(queue='',
                               exclusive=True)
        queue_name = result.method.queue
    - bind queue to exchange:
        routing key must match routing key of publish to receive
        queue_bind(exchange=EXCHANGE_NAME,
                   queue=queue_name,
                   routing_key='my-routing-key')  # <--- this
    - subscribe to queue:
        basic_consume(queue=q_name,
                      on_message_callback=on_message_fn)
        channel.start_consuming()
"""

import threading
import time
from functools import partial
from typing import Callable

from pika import spec, BasicProperties, frame
from pika.adapters.blocking_connection import BlockingChannel

from hello_rabbitmq.examples import get_channel, connect

EXCHANGE_NAME = 'direct_exchange_example'


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
    def consume(n: int,
                q_name: str,
                on_message_fn: Callable,
                channel: BlockingChannel):
        print(f'[consumer {n}] ', end='')

        channel.basic_consume(
            queue=q_name,
            on_message_callback=on_message_fn,
            auto_ack=False)

        print('subscribed to channel')
        channel.start_consuming()

        while True:
            time.sleep(1)


    for consumer_idx in range(3):
        subscriber_connection = connect()
        subscriber_channel = get_channel(subscriber_connection)
        subscriber_channel.exchange_declare(exchange=EXCHANGE_NAME,
                                            exchange_type='direct')
        result: frame.Method = subscriber_channel.queue_declare('', exclusive=True)
        queue_name = result.method.queue
        subscriber_channel.queue_bind(exchange=EXCHANGE_NAME,
                                      queue=queue_name,
                                      routing_key=f'{consumer_idx % 2}')
        subscriber_thread = threading.Thread(target=partial(consume,
                                                            consumer_idx,
                                                            queue_name,
                                                            partial(on_message, consumer_idx),
                                                            subscriber_channel),
                                             daemon=True)
        subscriber_thread.start()

    # producer
    producer_connection = connect()
    producer_channel = get_channel(producer_connection)
    producer_channel.exchange_declare(
        exchange=EXCHANGE_NAME,
        exchange_type='direct'
    )


    def produce():
        for i in range(10):
            msg = f'message #{i}'.encode('utf-8')
            producer_channel.basic_publish(exchange=EXCHANGE_NAME,
                                           routing_key=f'{i % 2}',  # odd / even msg to odd / even consumer
                                           body=msg)


    producer_thread = threading.Thread(target=produce)
    producer_thread.start()

    producer_thread.join()
    print('--- sleep(5) ---')
    time.sleep(5)
