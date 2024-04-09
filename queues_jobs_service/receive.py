import pika
import sys
import os

def callback(ch, method, properties, body):
    print(f" [x] Received {body.decode()}")

    ### Acá ocurre el procesamiento del mensaje

    # TODO Confirmar manualmente el mensaje
    ch.basic_ack(delivery_tag=method.delivery_tag)

def consume_jobs_service():
    credentials = pika.PlainCredentials('flor-rabbit', 'flor1234')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue='jobs_with_task_finished', durable=True, arguments={
        "x-queue-type": "quorum"
    })

    channel.exchange_declare(exchange='jobs_with_task_finished_exchange', exchange_type='direct')

    # la routing key se establece como una cadena vacía ''. Esto significa que cualquier mensaje enviado al intercambio
    # main_exchange será enrutado directamente a la cola main_queue
    channel.queue_bind(exchange='jobs_with_task_finished_exchange', queue='jobs', routing_key='')

    # TODO Para que envíe un mensaje a la vez, hasta que se confirme y luego le pueda enviar otro
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='jobs_with_task_finished', on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        consume_jobs_service()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(1)
        except SystemExit:
            os._exit(1)
