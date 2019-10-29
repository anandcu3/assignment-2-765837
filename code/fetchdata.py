from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import pandas as pd
import yaml
import time
import os
import shutil
import csv
import json
import math

constraints = yaml.load(open('constraints.yaml'))
print(constraints)

class MyHandler(FileSystemEventHandler):
    def microbatch(self, filename, required_size, clientN):
        numOfFiles = math.ceil(os.path.getsize(filename)/required_size)
        file_name, file_extension = filename.split("/")[-1].split(".")
        with open(filename) as inputfile:
            if file_extension == "csv":
                data = pd.read_csv(filename)
            elif file_extension == "json":
                data = pd.read_json(filename)
        for i in range(numOfFiles):
            a = data.iloc[int(i*data.shape[0]/numOfFiles):int((i+1)*data.shape[0]/numOfFiles)]
            if file_extension == "csv":
                a.to_csv("/home/anandcu3/assignment-2-765837/data/staging_dir/"+file_name+"_"+str(i)+".csv")
            elif file_extension == "json":
                a.to_json("/home/anandcu3/assignment-2-765837/data/staging_dir/"+file_name+"_"+str(i)+".json")
    def on_created(self, event):
        time.sleep(20)
        print('event type:',event,  'path :',event.src_path)
        filename = event.src_path
        if "client1" in filename:
            clientN = "customer1"
        elif "client2" in filename:
            clientN = "customer2"
        else:
            print("Doesn't have the proper client number!" + filename, file=open("fetchData.log", "a"))
            os.remove(filename)
            return
        if not constraints[clientN]["filetype"] in filename:
            print("Doesn't have the proper file_extension!" + filename, file=open("fetchData.log", "a"))
            os.remove(filename)
            return
        if os.path.getsize(filename) > constraints[clientN]["filesize"]:
            print("Needs microbatching. Exceeds the file size for the client" + filename +" "+ str(os.path.getsize(filename)), file=open("fetchData.log", "a"))
            self.microbatch(filename, constraints[clientN]["filesize"], clientN)
            os.remove(filename)
        else:
            print("Passes all the constraints. Moving to staging dir" + filename +" "+ str(os.path.getsize(filename)), file=open("fetchData.log", "a"))
            shutil.move(filename, "/home/anandcu3/assignment-2-765837/data/staging_dir/" )

event_handler = MyHandler()
observer = Observer()
observer.schedule(event_handler, path='/home/ftpserver/client-input-directory/', recursive=False)
observer.start()
try:
     while True:
         time.sleep(1)
except KeyboardInterrupt:
     observer.stop()
observer.join()
