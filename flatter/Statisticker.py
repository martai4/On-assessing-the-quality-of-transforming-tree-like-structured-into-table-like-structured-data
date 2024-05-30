import psutil
import time

class Statisticker:
    def __init__(self):
        self.start_time = 0
        self.start_cycles = 0
        self.monitor = False

    def start_measuring_time(self):
        self.start_time = time.time()
        self.start_cycles = psutil.cpu_stats().ctx_switches

    def stop_measuring_time(self, filename: str):
        end_cycles = psutil.cpu_stats().ctx_switches
        end_time = time.time()

        time_duration = str(round(end_time - self.start_time, 3))
        cpu_cycles = str(round(end_cycles - self.start_cycles, 3))

        print(f"Duration: {time_duration}")
        print(f"CPU Cycles: {cpu_cycles}\n")

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
