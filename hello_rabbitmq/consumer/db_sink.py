from typing import Optional

from pika import spec, BasicProperties
from pika.adapters.blocking_connection import BlockingChannel
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from hello_rabbitmq.consumer.mt_consumer import Consumer


class DbSink(Consumer):
    def __init__(self,
                 connection_string: str,
                 queue_name: str,
                 host: str,
                 port: int):
        super().__init__(queue_name, host, port)
        self.connection_string = connection_string
        self.engine: Optional[Engine] = None

    def __enter__(self):
        self.engine = create_engine(self.connection_string)
        return super(DbSink, self).__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        has_error = super(DbSink, self).__exit__(exc_type, exc_val, exc_tb)
        return has_error

    def on_message_callback(
            self,
            channel: BlockingChannel,
            method: spec.Basic.Deliver,
            properties: BasicProperties,
            body: bytes):
        with self.engine.connect() as conn:
            msg = body.decode('utf-8')
            print(f'[consumer] insert: {msg}')
            conn.execute(text(f"""INSERT INTO sandbox(msg) VALUES ('{msg}');"""))
        channel.basic_ack(delivery_tag=method.delivery_tag)
