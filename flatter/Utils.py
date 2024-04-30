import psutil
import time

def monitor_usage():
    while True:
        cpu_usage = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        print(f"Memory usage: {memory.percent}% ---- CPU useage: {cpu_usage}%")
        time.sleep(2)
