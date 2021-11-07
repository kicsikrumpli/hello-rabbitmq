import time

from pika import BasicProperties, spec
from pika.adapters.blocking_connection import BlockingChannel

from hello_rabbitmq.rabbit import ReconnectingRabbitChannel


class Consumer(ReconnectingRabbitChannel):
    def __init__(self,
                 queue_name: str,
                 host: str,
                 port: int):
        super().__init__(host, port)
        self.queue_name = queue_name

    def on_message_callback(
            self,
            channel: BlockingChannel,
            method: spec.Basic.Deliver,
            properties: BasicProperties,
            body: bytes):
        # print(f'channel: {channel}')
        # print(f'method: {method}')
        # print(f'properties: {properties}')
        print(f'[consumer] body: {body}')
        print('-' * 10)
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def do_work(self, channel: BlockingChannel):
        channel.queue_declare(queue=self.queue_name, durable=True)
        channel.basic_consume(queue=self.queue_name,
                              on_message_callback=self.on_message_callback)
        channel.start_consuming()


if __name__ == '__main__':
    with Consumer(host='localhost', port=5672, queue_name='test') as c:
        time.sleep(50)

    print('done')