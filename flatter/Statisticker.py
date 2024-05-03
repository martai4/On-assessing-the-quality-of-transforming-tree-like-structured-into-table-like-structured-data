import psutil
import time

class Statisticker:
    def __init__(self):
        self.start = time.time()

    def measure_time_start(self):
        self.start = time.time()

    def measure_time_stop(self, name:str=None):
        print(f"Time{' of ' + name if name is not None else ''}: {time.time() - self.start}\n")

    def monitor_usage():
        while True:
            cpu_usage = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            print(f"Memory usage: {memory.percent}% ---- CPU useage: {cpu_usage}%")
            time.sleep(2)
