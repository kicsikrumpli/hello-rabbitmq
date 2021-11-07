import time

from pika import BlockingConnection, ConnectionParameters, BasicProperties
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError
from retry import retry

from hello_rabbitmq import QUEUE_NAME, HOST, PORT, declare_queues


def connection_channel() -> BlockingChannel:
    # todo! add auth / connection details here
    return BlockingConnection(
        ConnectionParameters(
            host=HOST,
            port=PORT,
        )
    ).channel()


def hello():
    print('hello producer')


@retry(AMQPConnectionError, delay=5, jitter=(1, 3))
def produce():
    channel = connection_channel()
    declare_queues(channel)
    print(f'[producer] connected to {channel}')
    while True:
        msg = f'hello, {time.time()}'.encode('utf8')
        print(f'[producer] {msg}')
        channel.basic_publish(exchange='',  # goes to topic of the name routing key
                              routing_key=QUEUE_NAME,
                              body=msg,
                              properties=BasicProperties(delivery_mode=2)  # make message persistent
                              )
        time.sleep(3)
