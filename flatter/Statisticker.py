import psutil
import time
# from hwcounter import count, count_end

class Statisticker:
    def __init__(self):
        self.start_time = 0
        self.start_cycles = 0
        self.monitor = False

    def start_measuring_time(self):
        self.start_time = time.time()
        self.start_cycles = count()

    def stop_measuring_time(self, filename: str):
        # end_cycles = count_end()
        end_time = time.time()

        time_duration = str(round(end_time - self.start_time, 3))
        # cpu_cycles = str(end_cycles - self.start_cycles)

        print(f"Duration: {time_duration}")
        # print(f"CPU Cycles: {cpu_cycles}\n")

        # with open(f'{filename}.txt', 'a') as file:
            # file.write(f"{time_duration} {cpu_cycles}\n")

    def start_monitoring(self, filename: str, loop_break: float = 0.2):
        self.monitor = True

        with open(f'{filename}.txt', 'w') as file:
            while self.monitor:
                memory = psutil.virtual_memory()
                file.write(f"{memory.used}\n")
                time.sleep(loop_break)

    def stop_monitoring(self):
        self.monitor = False
