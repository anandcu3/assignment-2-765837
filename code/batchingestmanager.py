from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os, importlib, time
import pandas as pd
import subprocess

class MyHandler(FileSystemEventHandler):
    def on_created(self, event):
        time.sleep(20)
        print('event type:',event,  'path :',event.src_path, file=open("batchingestmanager.log","a"))
        filename = event.src_path
        if "client1" in filename:
            clientN = "customer1"
        elif "client2" in filename:
            clientN = "customer2"
        a = subprocess.Popen(["python", "clientbatchingestapp.py", clientN])
        report = ingestion('code/client-stage-directory/')
        print('the ingestion report is:', file=open("batchingestmanager.log","a"))
        os.remove("/home/anandcu3/assignment-2-765837/data/staging_dir/"+first_batch_filename)
