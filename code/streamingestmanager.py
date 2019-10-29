import pika
import json
import time
import subprocess
import multiprocessing

available_topics = {"customer1":[],"customer2":[]}
jobs = {"customer1":[],"customer2":[]}
credentials = pika.PlainCredentials('user','zfknBQbCRZ7u')
params = pika.ConnectionParameters('34.94.61.75','5672','/',credentials)
connection = pika.BlockingConnection(params)

class PublishAvaialbleTopics(threading.Thread):
    def __init__(self):
        super(PublishAvaialbleTopics, self).__init__()
    def run(self):
        while True:
            channel = connection.channel()
            for item in available_topics.keys():
                channel.queue_declare(queue='topicnames_'+item)
                channel.basic_publish(exchange='topicnames_'+item, routing_key='hello', body=json.dumps(available_topics[item]))
            connection.close()
            time.sleep(20)

for a in available_topics.keys():
    topic_name = a+"_"+str(len(available_topics[a]))
    a = subprocess.Popen(['python', 'clientstreamingestapp.py', topic_name])
    available_topics[a].append(topic_name)
    jobs[a].append(p)
