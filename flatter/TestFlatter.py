import threading

from Statisticker import Statisticker
from methods.JSONFirstListFlattener import JSONFirstListFlattener
from methods.JSONPathFlattener import JSONPathFlattener
from methods.JSONListToTableConverter import JSONListToTableConverter
from methods.FlattenJSON import FlattenJSON

if __name__ == "__main__":
    json_file_paths = ['../data/airlines.json',
                       '../data/gists.json',
                       '../data/movies.json',
                       '../data/reddit.json',
                       '../data/nasa.json']

    statisticker = Statisticker()
    monitor_thread = threading.Thread(target=statisticker.start_monitoring, args=("statistics",))
    monitor_thread.start()

    flatter_list = [
        (JSONPathFlattener(), 50051, "JSONPathFlattener"),
        (JSONFirstListFlattener(), 50052, "JSONFirstListFlattener"),
        (JSONListToTableConverter(), 50053, "JSONListToTableConverter"),
        (FlattenJSON(), 50054, "FlattenJSON")
    ]

    for (flatter, port, name) in flatter_list:
        thread = threading.Thread(target=flatter.serve, args=(port,))
        thread.start()

        print(f"--- {name} ---")
        statisticker.start_measuring_time()
        flatter.load_json_from_file(json_file_paths)
        statisticker.stop_measuring_time(name)

        flatter.server.stop()
        thread.join()
    
    statisticker.stop_monitoring()
    monitor_thread.join()