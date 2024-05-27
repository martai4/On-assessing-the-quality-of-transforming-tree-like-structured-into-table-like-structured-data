import threading
from json import loads

from Statisticker import Statisticker
from methods.JSONFirstListFlattener import JSONFirstListFlattener
from methods.JSONPathFlattener import JSONPathFlattener
from methods.JSONListToTableConverter import JSONListToTableConverter
from methods.JSONFlatten import JSONFlatten
from methods.JSONDummy import JSONDummy

if __name__ == "__main__":
    json_file = '../data/testFiles/AIRLINES-15000000.txt'

    statisticker = Statisticker()
    # monitor_thread = threading.Thread(target=statisticker.start_monitoring, args=("statistics",))
    # monitor_thread.start()

    flatter_list = [
        (JSONPathFlattener(), 50051, "JSONPathFlattener"),
        (JSONFirstListFlattener(), 50052, "JSONFirstListFlattener"),
        (JSONListToTableConverter(), 50053, "JSONListToTableConverter"),
        # (JSONFlatten(), 50054, "JSONFlatten"),
        (JSONDummy(), 50055, "JSONDummy")
    ]

    for (flatter, port, method_name) in flatter_list:
        thread = threading.Thread(target=flatter.serve, args=(port,))
        thread.start()

        print(f"--- {method_name} ---")
        statisticker.start_measuring_time()

        with open(json_file, 'r') as f:
            buffer = ""
            while True:
                data = f.read()
                if not data:
                    break

                json = buffer + data
                json_objects = json.split("\n")
                buffer = json_objects.pop()

                flatter.do_put("TestDataset", json_objects)

        statisticker.stop_measuring_time(f"tests/time/files/temp-{method_name}.txt")

        flatter.server.stop()
        thread.join()

    statisticker.stop_monitoring()
    # monitor_thread.join()
