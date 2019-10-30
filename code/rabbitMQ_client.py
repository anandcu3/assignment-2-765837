import pika
import argparse
import threading
import time
import pandas as pd
import random
import json
import sys
import ctypes

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--client_id', type=str, help='Set the client ID.', default='customer1')
    return parser.parse_args()
args = parse_args()
topics = []
credentials = pika.PlainCredentials('user', 'zfknBQbCRZ7u')
parameters = pika.ConnectionParameters('34.94.61.75',5672,'/',credentials)

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
        channel.basic_consume(queue='topicnames_' + self.clientID, on_message_callback=callback, auto_ack=True)
        channel.start_consuming()
    def get_id(self):
       if hasattr(self, '_thread_id'):
           return self._thread_id
       for id, thread in threading._active.items():
           if thread is self:
               return id
    def raise_exception(self):
        thread_id = self.get_id()
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, ctypes.py_object(SystemExit))
t1 = GetAvailableTopics(args.client_id)
t1.start()

if args.client_id == "customer1":
    pd_data = pd.read_csv("../data/googleplaystore.csv")
elif args.client_id == "customer2":
    pd_data = pd.read_csv("../data/googleplaystore_user_reviews.csv")

while len(topics) == 0:
    time.sleep(1)

connection = pika.BlockingConnection(parameters)
channel = connection.channel()
start = time.time()
for ix in pd_data.index[:10]:
    data_row = pd_data.loc[[ix],].to_dict(orient='records')[0]
    data_row["input_time"] = time.time()
    topicName = random.choice(topics)
    channel.basic_publish(exchange='', routing_key=topicName, body=json.dumps(data_row))
connection.close()
print("Done : ", time.time()-start)
t1.raise_exception()
