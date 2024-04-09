import pika
import sys
import os

def callback(ch, method, properties, body):
    print(f" [x] Received {body.decode()}")

    ### Acá ocurre el procesamiento del mensaje

    # TODO Confirmar manualmente el mensaje
    ch.basic_ack(delivery_tag=method.delivery_tag)

def consume_ocr_worker():
    credentials = pika.PlainCredentials('flor-rabbit', 'flor1234')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue='jobs_ocr', durable=True, arguments={
        "x-queue-type": "quorum"
    })

    channel.exchange_declare(exchange='jobs_ocr_exchange', exchange_type='direct')

    # la routing key se establece como una cadena vacía ''. Esto significa que cualquier mensaje enviado al intercambio
    # main_exchange será enrutado directamente a la cola main_queue
    channel.queue_bind(exchange='jobs_ocr_exchange', queue='jobs_ocr', routing_key='')

    # Para que envíe un mensaje a la vez, hasta que se confirme y luego le pueda enviar otro
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='jobs_ocr', on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        consume_ocr_worker()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(1)
        except SystemExit:
            os._exit(1)