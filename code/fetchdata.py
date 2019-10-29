import yaml
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

my_dict = yaml.load(open('constraints.yaml'))
print(my_dict)

class MyHandler(FileSystemEventHandler):
    def on_modified(self, event):
        print(f'event type: {event.event_type}  path : {event.src_path}')


if __name__ == "__main__":
    event_handler = MyHandler()
    observer = Observer()
    observer.schedule(event_handler, path='../data/', recursive=False)
    observer.start()
