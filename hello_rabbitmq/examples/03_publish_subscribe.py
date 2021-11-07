"""
03 Publish-Subscribe
All subscribers receive the same messages.

https://www.rabbitmq.com/tutorials/tutorial-three-python.html

Internally consumers subscribe to exchanges
each consumer is assigned a temporary queue with a generated name (declared as '')
to declare a queue as temporary, use param: exclusive=True
temporary queues are bound to exchanges.

exchange type determines how messages are routed to which queue.
NB! default anonymous exchange sends to named queues in RR

exchange types:
- direct
- topic
- headers
- fanout

- producer:
    - declare exchange:
        exchange_declare(exchange='logs',
                         exchange_type='fanout')
    - publish to exchange instead of queue:
        basic_publish(exchange=exchange_name,
                      routing_key='',
                      body=message)
- consumer:
    - declare exchange:
        exchange_declare(exchange='logs',
                         exchange_type='fanout')
    - declare anonymous temporary queue:
        result = queue_declare(queue='',
                               exclusive=True)
        queue_name = result.method.queue
    - bind queue to exchange:
        queue_bind(exchange=EXCHANGE_NAME,
                   queue=queue_name)
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

EXCHANGE_NAME = 'logs'


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
        subscriber_channel.exchange_declare(exchange='logs',
                                            exchange_type='fanout')
        result: frame.Method = subscriber_channel.queue_declare('', exclusive=True)
        queue_name = result.method.queue
        subscriber_channel.queue_bind(exchange=EXCHANGE_NAME,
                                      queue=queue_name)
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
        exchange='logs',
        exchange_type='fanout'
    )


    def produce():
        for i in range(10):
            msg = f'message #{i}'.encode('utf-8')
            producer_channel.basic_publish(exchange=EXCHANGE_NAME,
                                           routing_key='',
                                           body=msg)


    producer_thread = threading.Thread(target=produce)
    producer_thread.start()

    producer_thread.join()
    print('--- sleep(5) ---')
    time.sleep(5)
