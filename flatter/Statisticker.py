import psutil
import time

class Statisticker:
    def __init__(self):
        self.start = time.time()
        self.monitor = False

    def start_measuring_time(self):
        self.start = time.time()

    def stop_measuring_time(self, name:str=None):
        print(f"Time{' of ' + name if name is not None else ''}: {time.time() - self.start}\n")

    def start_monitoring(self, filename:str, loop_break:float=0.5):
        self.monitor = True

        with open(f'{filename}.txt', 'w') as file:
            file.write("Memory usage; CPU usage\n")
            while self.monitor:
                cpu_usage = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory()
                file.write(f"{memory.percent}%; {cpu_usage}%\n")
                time.sleep(loop_break)

    def stop_monitoring(self):
        self.monitor = False
