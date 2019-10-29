import yaml
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

my_dict = yaml.load(open('constraints.yaml'))
print(my_dict)

class MyHandler(FileSystemEventHandler):
    def on_modified(self, event):
        print('event type:',event.event_type,  'path :',event.src_path)

event_handler = MyHandler()
observer = Observer()
observer.schedule(event_handler, path='/home/ftpserver/client-input-directory/ ', recursive=False)
observer.start()
try:
     while True:
         time.sleep(1)
except KeyboardInterrupt:
     observer.stop()
observer.join()
