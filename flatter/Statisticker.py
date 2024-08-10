from hwcounter import count, count_end
import psutil
import time

class Statisticker:
    def __init__(self):
        self.start_time = 0
        self.start_cpu = 0
        self.monitor = False

# =====================================================================

    def start_measuring_time(self):
        self.start_time = time.time()

    def stop_measuring_time(self, filename: str):
        end_time = time.time()
        time_duration = str(round(end_time - self.start_time, 3))
        print(f"Duration: {time_duration}")

        # with open(f'{filename}.txt', 'a') as file:
        #     file.write(f"{time_duration}\n")

# =====================================================================

    def start_measuring_cpu(self):
        self.start_cpu = count()

    def stop_measuring_cpu(self, filename: str):
        end_cpu = count_end()
        cpu_duration = str(round(end_cpu - self.start_cpu, 3))
        print(f"CPU: {cpu_duration}")

        with open(f'{filename}.txt', 'a') as file:
            file.write(f"{cpu_duration}\n")

# =====================================================================

    def start_monitoring(self, filename: str, loop_break: float = 0.2):
        self.monitor = True

        with open(f'{filename}.txt', 'w') as file:
            while self.monitor:
                memory = psutil.virtual_memory()
                file.write(f"{memory.used}\n")
                time.sleep(loop_break)

    def stop_monitoring(self):
        self.monitor = False

# =====================================================================
