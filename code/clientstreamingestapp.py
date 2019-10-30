import threading
import pika
import sys
import pymongo
import json
import time

class ReportStats(threading.Thread):
    def __init__(self, data, end, start):
        super(ReportStats, self).__init__()
        self.end = end
        self.start = start
        self.data = data
    def run(self):
        collection_stats = db.command("collstats", table_name)
        report = {
                    'clientID':client,
                    'ingestion_time':self.end-self.start,
                    'total_time':self.end-self.data["input_time"],
                    'input_time':self.data["input_time"],
                    'collection_size':collection_stats['size'],
                    'collection_count':collection_stats['count'],
                    'collection_avgObjSize':collection_stats['avgObjSize']
                    }
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.basic_publish(exchange='', routing_key='reporting_channel', body=json.dumps(report))
        connection.close()
        db["reports"].insert(report)

topic = sys.argv[1]
clientmongo = pymongo.MongoClient("mongodb://anandcu3:h3GuvlswXSUsRgCb@cluster0-shard-00-00-fbaws.gcp.mongodb.net:27017,cluster0-shard-00-01-fbaws.gcp.mongodb.net:27017,cluster0-shard-00-02-fbaws.gcp.mongodb.net:27017/admin?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true&w=majority")
print(clientmongo.test, file=open("clientstreamingestapp.log","a"))
db = clientmongo["google_play_store"]
if "customer1" in topic:
    table_name = "app_information"
    client = "customer1"
    table = db["app_information"]
elif "customer2" in topic:
    table_name = "app_reviews"
    client = "customer2"
    table = db["app_reviews"]
credentials = pika.PlainCredentials('user', 'zfknBQbCRZ7u')
parameters = pika.ConnectionParameters('34.94.61.75',5672,'/',credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue=topic)
def callback(ch, method, properties, body):
    print("Received"+body.decode(), file=open("clientstreamingestapp.log","a"))
    data = json.loads(body.decode())
    start = time.time()
    table.insert(data)
    end = time.time()
    ReportStats(data, end, start).start()

channel.basic_consume(queue=topic, on_message_callback=callback, auto_ack=True)
print(' [*] Waiting for messages on topic', topic, 'To exit press CTRL+C', file=open("clientstreamingestapp.log","a"))
channel.start_consuming()
