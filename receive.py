import pika
import sys
import os

def callback(ch, method, properties, body):
    print(f" [x] Received {body.decode()}")

def consume():
    credentials = pika.PlainCredentials('flor-rabbit', 'flor1234')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue='main_queue', durable=True, arguments={
        "x-queue-type": "quorum",
        'x-dead-letter-exchange': 'retry_exchange',
        # Los mensajes rechazados en la cola principal se enviarán a la cola de reintentos, éste es el enrutamiento de los mensajes
        'x-message-ttl': 10000,  # Tiempo de vida del mensaje en la cola principal
        'x-dead-letter-routing-key': 'retry_queue'
    })

    channel.queue_declare(queue='retry_queue', durable=True, arguments={
        "x-queue-type": "quorum",
        'x-dead-letter-exchange': 'main_exchange',
        # Para poder reintentar el procesamiento de los mensajes que hayan sido rechazados por la principal
        'x-message-ttl': 10000,  # Tiempo de vida del mensaje en la cola de reintentos
        'x-dead-letter-routing-key': 'dead_letter_queue',
        # Si se alcanza la cantidad máxima de reintentos los mensajes quedarán en la cola de dead letter
        'x-delivery-limit': 5  # Número máximo de reintentos de los mensajes en ésta cola
    })

    channel.exchange_declare(exchange='main_exchange', exchange_type='direct')
    channel.exchange_declare(exchange='retry_exchange', exchange_type='direct')

    # la routing key se establece como una cadena vacía ''. Esto significa que cualquier mensaje enviado al intercambio
    # main_exchange será enrutado directamente a la cola main_queue
    channel.queue_bind(exchange='main_exchange', queue='main_queue', routing_key='')
    channel.queue_bind(exchange='retry_exchange', queue='retry_queue', routing_key='')
    channel.queue_bind(exchange='retry_exchange', queue='dead_letter_queue', routing_key='')

    # Consume los mensajes de todas las colas especificadas, una vez recibido un mensaje ejecuta la funcion callback
    # y hace el commit automaticamente con el flag auto_ack=True, esto indica que llego correctamente el mensaje, hay
    # otra funcionalidad que idica que no llego el mensaje correctamente
    channel.basic_consume(queue='main_queue', on_message_callback=callback, auto_ack=True)
    channel.basic_consume(queue='retry_queue', on_message_callback=callback, auto_ack=True)
    channel.basic_consume(queue='dead_letter_queue', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        consume()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(1)
        except SystemExit:
            os._exit(1)
