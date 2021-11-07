import queue
import time

from pika import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel

from hello_rabbitmq import HOST, PORT
from hello_rabbitmq.rabbit import ReconnectingRabbitChannel


class Producer(ReconnectingRabbitChannel):
    PERSISTENT = BasicProperties(delivery_mode=2)

    def __init__(self,
                 queue_name: str,
                 host: str = HOST,
                 port: int = PORT):
        super().__init__(host=host, port=port)
        self.mailbox = queue.Queue()
        self.queue_name = queue_name
        self.is_daemon = False

    @property
    def exchange(self):
        return ''

    @property
    def routing_key(self):
        return self.queue_name

    def do_work(self, channel: BlockingChannel):
        channel.queue_declare(queue=self.queue_name, durable=True)
        while True:
            message: bytes
            message = self.mailbox.get()
            channel.basic_publish(
                exchange=self.exchange,
                routing_key=self.routing_key,
                body=message,
                properties=self.PERSISTENT
            )

    def publish(self, message: bytes):
        self.mailbox.put(message)


if __name__ == '__main__':
    with Producer(host='localhost', port=5672, queue_name='test') as p:
        for _ in range(20):
            p.publish('hello'.encode('utf-8'))
            time.sleep(1)
            print('.', end='')

    print('done')
