import pyarrow as pa
import pandas as pd
import json, os

from FlightServer import FlightServer

class JSONPathFlattener:
    def __init__(self):
        self.server = None

    def flatten_json_structure(self, json_structure):
        flattened = {}

        def flatten(structure, path=''):
            if isinstance(structure, dict):
                for key in structure:
                    flatten(structure[key], path + key + '.')
            elif isinstance(structure, list):
                for i, item in enumerate(structure):
                    flatten(item, path.rstrip('.') + '[' + str(i) + '].')
            else:
                flattened[path.rstrip('.')] = structure

        flatten(json_structure)
        return flattened

    def load_json_and_flatten(self, file_path):
        with open(file_path, encoding='utf-8') as json_file:
            data = json.load(json_file)
            if isinstance(data, list):
                return [self.flatten_json_structure(item) for item in data]
            else:
                return [self.flatten_json_structure(data)]

    def serve(self, file_paths, server_port):
        flattened_data = {f"FlattenedJSON_{os.path.splitext(os.path.basename(path))[0]}": pa.Table.from_pandas(pd.DataFrame(self.load_json_and_flatten(path))) for path in file_paths}
        self.server = FlightServer(flattened_data, server_port)
        self.server.serve()

    def do_put(self, json_data):
        if self.server is not None:
            self.server.do_put(json_data)
        else:
            print("First, initialize the server")
