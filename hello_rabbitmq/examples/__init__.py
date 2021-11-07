from pika import BlockingConnection
from pika.adapters.blocking_connection import BlockingChannel


def connect() -> BlockingConnection:
    return BlockingConnection()


def get_channel(conn: BlockingConnection) -> BlockingChannel:
    return conn.channel()


QUEUE_NAME = 'example_queue'
DURABLE_QUEUE_NAME = 'durable_example_queue'
