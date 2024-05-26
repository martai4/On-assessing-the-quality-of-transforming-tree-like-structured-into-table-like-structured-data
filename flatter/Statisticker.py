import psutil
import time


class Statisticker:
    def __init__(self):
        self.start = time.time()
        self.monitor = False

    def start_measuring_time(self):
        self.start = time.time()

    def stop_measuring_time(self, filename: str):
        time_duration = str(round(time.time() - self.start, 3))
        print(f"Duration: {time_duration}\n")
        with open(f'{filename}.txt', 'a') as file:
            file.write(f"{time_duration}\n")

    def start_monitoring(self, filename: str, loop_break: float = 0.2):
        self.monitor = True

        with open(f'{filename}.txt', 'w') as file:
            file.write("Memory usage [%]; CPU usage [%]\n")
            while self.monitor:
                cpu_usage = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory()
                file.write(f"{memory.percent}; {cpu_usage}\n")
                time.sleep(loop_break)

    def stop_monitoring(self):
        self.monitor = False
