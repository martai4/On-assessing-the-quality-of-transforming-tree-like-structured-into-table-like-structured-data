import threading

from methods.JSONFirstListFlattener import JSONFirstListFlattener
from methods.JSONPathFlattener import JSONPathFlattener
from methods.JSONListToTableConverter import JSONListToTableConverter
from methods.FlattenJSON import FlattenJSON


if __name__ == '__main__':
    """
    Main entry point of the script. It initializes instances of JSONPathFlattener, 
    JSONFirstListFlattener, and JSONListToTableConverter classes, and runs them in separate threads.
    """
    
    json_file_paths = [ '../data/airlines.json', 
                        '../data/gists.json',
                        '../data/movies.json',
                        '../data/reddit.json',
                        '../data/nasa.json']
    """
    List of paths to the JSON files to be processed.
    """
    
    json_path_flattener = JSONPathFlattener()
    json_path_flattener_thread = threading.Thread(target=json_path_flattener.serve_flattened_json, args=(json_file_paths, 50051))
    json_path_flattener_thread.start()

    json_first_list_flattener = JSONFirstListFlattener()
    json_first_list_flattener_thread = threading.Thread(target=json_first_list_flattener.serve_flattened_json, args=(json_file_paths, 50052))
    json_first_list_flattener_thread.start()

    json_list_to_table_converter = JSONListToTableConverter()
    json_list_to_table_converter_thread = threading.Thread(target=json_list_to_table_converter.serve_tables, args=(json_file_paths, 50053))
    json_list_to_table_converter_thread.start()

    flatten_json = FlattenJSON()
    json_first_list_flattener_thread = threading.Thread(target=json_first_list_flattener.serve_flattened_json, args=(json_file_paths, 50054))
    json_first_list_flattener_thread.start()


    json_path_flattener_thread.join()
    json_first_list_flattener_thread.join()
    json_list_to_table_converter_thread.join()
