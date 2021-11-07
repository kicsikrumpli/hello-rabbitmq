__version__ = '0.1.0'

from pika.adapters.blocking_connection import BlockingChannel

QUEUE_NAME = 'test'
HOST = 'broker'
PORT = 5672


# common queue declare
# re-declaring with changed durability will raise error
def declare_queues(channel: BlockingChannel):
    channel.queue_declare(
        queue=QUEUE_NAME,
        durable=True
    )

