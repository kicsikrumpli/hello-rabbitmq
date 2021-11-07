import abc
from threading import Thread
from typing import Optional

from pika import ConnectionParameters
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.exceptions import AMQPConnectionError
from retry import retry


class ReconnectingRabbitChannel:
    def __init__(self,
                 host: str,
                 port: int):
        self.connection_parameters = ConnectionParameters(host=host, port=port)
        self.rabbit_thread: Optional[Thread] = None
        self.is_daemon = True

    @retry((AMQPConnectionError, OSError), delay=5, jitter=(1, 3))
    def thread_target(self):
        with BlockingConnection(self.connection_parameters) as conn, \
                conn.channel() as channel:
            print('connected...')
            self.do_work(channel)

    @abc.abstractmethod
    def do_work(self, channel: BlockingChannel):
        pass

    def __enter__(self):
        self.rabbit_thread = Thread(target=self.thread_target, daemon=self.is_daemon)
        self.rabbit_thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return exc_type is None
