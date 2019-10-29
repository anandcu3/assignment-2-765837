import threading
import pika

def SubscribeThread(topic):
    credentials = pika.PlainCredentials('user', 'zfknBQbCRZ7u')
    parameters = pika.ConnectionParameters('35.236.51.168',5672,'/',credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue=topic)
    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)
    channel.basic_consume(queue=topic, on_message_callback=callback, auto_ack=True)
    print(' [*] Waiting for messages on topic', topic, 'To exit press CTRL+C')
    channel.start_consuming()
