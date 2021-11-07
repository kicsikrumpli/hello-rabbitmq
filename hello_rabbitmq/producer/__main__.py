import time
from datetime import datetime

from hello_rabbitmq import QUEUE_NAME, HOST, PORT
from hello_rabbitmq.producer.mt_producer import Producer

if __name__ == '__main__':
    with Producer(queue_name=QUEUE_NAME, host=HOST, port=PORT) as producer:
        print('producing')
        while True:
            msg = f'hello # {datetime.now()}'.encode('utf-8')
            producer.publish(message=msg)
            time.sleep(3)

    print('done')
