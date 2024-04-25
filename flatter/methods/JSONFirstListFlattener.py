import pyarrow as pa
import pandas as pd
import json, os

from FlightServer import FlightServer

class JSONFirstListFlattener:
    def __init__(self) -> None:
        self.server = None

    def load_json_and_flatten(self, file_path):
        return pd.json_normalize(json.load(open(file_path, encoding='utf-8')))

    def serve(self, file_paths, server_port):
        flattened_data = {f"FlattenedJSON_{os.path.splitext(os.path.basename(path))[0]}": pa.Table.from_pandas(self.load_json_and_flatten(path)) for path in file_paths}
        self.server = FlightServer(flattened_data, server_port)
        self.server.serve()

    def do_put(self, json_data):
        if self.server is not None:
            self.server.do_put(json_data)
        else:
            print("First, initialize the server")
