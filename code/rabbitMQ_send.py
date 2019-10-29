#!/usr/bin/env python
import pika
credentials = pika.PlainCredentials('user','zfknBQbCRZ7u')
params = pika.ConnectionParameters('34.94.61.75','5672','/',credentials)
connection = pika.BlockingConnection(params)
channel = connection.channel()

channel.queue_declare(queue='hello')

channel.basic_publish(exchange='', routing_key='hello', body='Hello World!')
print(" [x] Sent 'Hello World!'")
connection.close()
