import threading

from Statisticker import Statisticker
from methods.JSONFirstListFlattener import JSONFirstListFlattener
from methods.JSONPathFlattener import JSONPathFlattener
from methods.JSONListToTableConverter import JSONListToTableConverter
from methods.FlattenJSON import FlattenJSON

# TODO - delete or rename this file
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

    # json_path_flattener = JSONPathFlattener()
    # json_path_flattener_thread = threading.Thread(target=json_path_flattener.serve, args=(50051, json_file_paths))
    # json_path_flattener_thread.start()

    # json_first_list_flattener = JSONFirstListFlattener()
    # json_first_list_flattener_thread = threading.Thread(target=json_first_list_flattener.serve, args=(50052, json_file_paths))
    # json_first_list_flattener_thread.start()

    # json_list_to_table_converter = JSONListToTableConverter()
    # json_list_to_table_converter_thread = threading.Thread(target=json_list_to_table_converter.serve, args=(50053, json_file_paths))
    # json_list_to_table_converter_thread.start()

    # flatten_json = FlattenJSON()
    # flatten_json_thread = threading.Thread(target=flatten_json.serve, args=(50054, json_file_paths))
    # flatten_json_thread.start()

    # json_path_flattener_thread.join()
    # json_first_list_flattener_thread.join()
    # flatten_json_thread.join()
    # json_list_to_table_converter_thread.join()
