import pika
import argparse
import threading
import time
import pandas as pd
import random
import json
import sys

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--client_id', type=str, help='Set the client ID.', default='customer1')
    return parser.parse_args()
args = parse_args()
topics = []
credentials = pika.PlainCredentials('user', 'zfknBQbCRZ7u')
parameters = pika.ConnectionParameters('35.236.51.168',5672,'/',credentials)

class GetAvailableTopics(threading.Thread):
    def __init__(self, clientID):
        super(GetAvailableTopics, self).__init__()
        self.clientID = clientID
    def run(self):
        def callback(ch, method, properties, body):
            global topics
            topics = body.decode().split(",")
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue='topicnames_' + self.clientID)
        channel.basic_consume(queue='topicnames_' + self.clientID, on_message_callback=callback, auto_ack=True)
        channel.start_consuming()

t1 = GetAvailableTopics(args.client_id)
t1.start()

while len(topics) == 0:
    time.sleep(1)

if args.client_id == "customer1":
    pd_data = pd.read_csv("../data/googleplaystore.csv")
elif args.client_id == "customer2":
    pd_data = pd.read_csv("../data/googleplaystore_user_reviews.csv")

connection = pika.BlockingConnection(parameters)
channel = connection.channel()
start = time.time()
for ix in pd_data.index:
    data_row = pd_data.loc[[ix],].to_dict(orient='records')[0]
    topicName = random.choice(topics)
    channel.queue_declare(queue=topicName)
    channel.basic_publish(exchange='', routing_key=topicName, body=json.dumps(data_row))
connection.close()
print("Done : ", time.time()-start)
sys.exit()
