from pika import BlockingConnection, ConnectionParameters, BasicProperties, spec
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError
from retry import retry

from hello_rabbitmq import QUEUE_NAME, declare_queues, HOST, PORT


def connection_channel() -> BlockingChannel:
    # todo! add auth / connection details here
    return BlockingConnection(
        ConnectionParameters(
            host=HOST,
            port=PORT,
        )
    ).channel()


def hello():
    print('hello consumer')


def on_message_callback(
        channel: BlockingChannel,
        method: spec.Basic.Deliver,
        properties: BasicProperties,
        body: bytes):
    # print(f'channel: {channel}')
    # print(f'method: {method}')
    # print(f'properties: {properties}')
    print(f'body: {body}')
    print('-' * 10)
    channel.basic_ack(delivery_tag=method.delivery_tag)
    # reject:
    # channel.basic_reject(
    #    delivery_tag=method.delivery_tag,
    #    requeue=True)


@retry(AMQPConnectionError, delay=5, jitter=(1, 3))
def consume():
    print('[consumer] connecting')
    channel: BlockingChannel = connection_channel()
    print(f'[consumer] connected to {channel}')
    declare_queues(channel)
    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=on_message_callback,
        auto_ack=False,  # let's do it manually, todo! combine w/ db transaction
    )

    # exceptions caught by retry
    channel.start_consuming()


