import threading

from methods.JSONFirstListFlattener import JSONFirstListFlattener
from methods.JSONPathFlattener import JSONPathFlattener
from methods.JSONListToTableConverter import JSONListToTableConverter
from methods.FlattenJSON import FlattenJSON

# TODO - delete this file
if __name__ == "__main__": 
    json_file_paths = ['../data/airlines.json',
                       '../data/gists.json',
                       '../data/movies.json',
                       '../data/reddit.json',
                       '../data/nasa.json']

    json_path_flattener = JSONPathFlattener()
    json_path_flattener_thread = threading.Thread(target=json_path_flattener.serve, args=(50051, json_file_paths))
    json_path_flattener_thread.start()

    json_first_list_flattener = JSONFirstListFlattener()
    json_first_list_flattener_thread = threading.Thread(target=json_first_list_flattener.serve, args=(50052, json_file_paths))
    json_first_list_flattener_thread.start()

    json_list_to_table_converter = JSONListToTableConverter()
    json_list_to_table_converter_thread = threading.Thread(target=json_list_to_table_converter.serve, args=(50053,json_file_paths))
    json_list_to_table_converter_thread.start()

    flatten_json = FlattenJSON()
    flatten_json_thread = threading.Thread(target=flatten_json.serve, args=(50054, json_file_paths))
    flatten_json_thread.start()

    json_path_flattener_thread.join()
    json_first_list_flattener_thread.join()
    json_list_to_table_converter_thread.join()
    flatten_json_thread.join()
