from hello_rabbitmq import QUEUE_NAME, HOST, PORT, CONN_STR
from hello_rabbitmq.consumer.db_sink import DbSink
from hello_rabbitmq.consumer.mt_consumer import Consumer


if __name__ == '__main__':
    # with Consumer(queue_name=QUEUE_NAME, host=HOST, port=PORT) as consumer:
    with DbSink(connection_string=CONN_STR, queue_name=QUEUE_NAME, host=HOST, port=PORT) as consumer:
        print('consuming')
        consumer.rabbit_thread.join()

    print('done')
