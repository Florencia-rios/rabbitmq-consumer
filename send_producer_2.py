import os
import pika
import sys


def send():

    credentials = pika.PlainCredentials('flor-rabbit', 'flor1234')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=credentials))
    channel = connection.channel()

    # Cola de tipo quorum, por default es durable=true
    channel.queue_declare(queue='prueba_quorum', durable=True, arguments={"x-queue-type": "quorum"})

    # Publica un nuevo mensaje
    channel.basic_publish(exchange='',
                          routing_key='prueba_quorum',
                          body='Hello World!')
    print(" [x] Sent 'Hello World!'")

    connection.close()

if __name__ == '__main__':
    try:
        send()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(1)
        except SystemExit:
            os._exit(1)