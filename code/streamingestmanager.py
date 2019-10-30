import pika
import json
import time
import subprocess
import threading
import os
import signal

available_topics = {"customer1":[],"customer2":[]}
jobs = {"customer1":[],"customer2":[]}
credentials = pika.PlainCredentials('user','zfknBQbCRZ7u')
parameters = pika.ConnectionParameters('34.94.61.75','5672','/',credentials)

class PublishAvaialbleTopics(threading.Thread):
    def __init__(self):
        super(PublishAvaialbleTopics, self).__init__()
    def run(self):
        while True:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            for item in available_topics.keys():
                channel.queue_declare(queue='topicnames_'+item)
                channel.basic_publish(exchange='', routing_key='topicnames_'+item, body=",".join(available_topics[item]))
            connection.close()
            time.sleep(20)

class ReceiveReports(threading.Thread):
    def __init__(self):
        super(ReceiveReports, self).__init__()
    def run(self):
        def callback(ch, method, properties, body):
            global jobs, available_topics
            report = json.loads(body.decode())
            if report["total_time"] > 10:
                topic_name = topic+"_"+str(len(available_topics[topic]))
                print("Starting new topic for", report["clientID"])
                a = subprocess.Popen(['python', 'clientstreamingestapp.py', topic_name])
                available_topics[topic].append(topic_name)
                jobs[topic].append(a)
            if report["total_time"] < 10 and len(available_topics[report["clientID"]]) > 1:
                for i in range(jobs):
                    if i==0:
                        continue
                    os.killpg(os.getpgid(jobs[i].pid), signal.SIGTERM)
                jobs = [jobs[0]]
                available_topics = [available_topics[0]]
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue='reporting_channel')
        channel.basic_consume(queue='reporting_channel', on_message_callback=callback, auto_ack=True)
        channel.start_consuming()

t = PublishAvaialbleTopics()
t.start()

t2 = ReceiveReports()
t2.start()

for topic in available_topics.keys():
    topic_name = topic+"_"+str(len(available_topics[topic]))
    a = subprocess.Popen(['python', 'clientstreamingestapp.py', topic_name])
    available_topics[topic].append(topic_name)
    jobs[topic].append(a)
