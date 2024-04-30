import pyarrow as pa
import pandas as pd
import json, os

from FlightServer import FlightServer

class JSONPathFlattener:
    def __init__(self):
        self.server = None

    def serve(self, server_port: int, file_paths=None) -> None:
        self.server = FlightServer(server_port)
        self.load_json_from_file(file_paths) if file_paths else None
        self.server.serve()

    def do_put(self, dataset_name: str, json) -> None:
        flat_data = {f"FlattenedJSON_{dataset_name}": self.flatten_json(json)}
        self.server.do_put(flat_data)

    def load_json_from_file(self, file_paths: list) -> None:
        flattened_data = {}
        for path in file_paths:
            json_data = json.load(open(path, encoding='utf-8'))
            flattened_data[f"FlattenedJSON_{os.path.basename(path)}"] = self.flatten_json(json_data)

        self.server.do_put(flattened_data)

    def flatten_json(self, json) -> pa.Table:
        result = [self.flatten_json_structure(item) for item in json] \
            if isinstance(json, list) \
            else [self.flatten_json_structure(json)]

        return pa.Table.from_pandas(pd.DataFrame(result))

    def flatten_json_structure(self, json_structure):
        def flatten(structure, path=''):
            if isinstance(structure, dict):
                for key in structure:
                    flatten(structure[key], path + key + '.')
            elif isinstance(structure, list):
                for i, item in enumerate(structure):
                    flatten(item, path.rstrip('.') + '[' + str(i) + '].')
            else:
                flattened[path.rstrip('.')] = structure

        flattened = {}
        flatten(json_structure)
        return flattened
