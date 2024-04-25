import pyarrow as pa
import pandas as pd
import json
import os
from flatten_json import flatten

from FlightServer import FlightServer

class FlattenJSON:
    def __init__(self):
        self.server = None

    def load_and_flatten_json(self, json_path):
        json_data = json.load(open(json_path, encoding='utf-8'))
        if isinstance(json_data, list):
            return pd.DataFrame([flatten(data) for data in json_data])
        elif isinstance(json_data, dict):
            return pd.DataFrame([flatten(json_data)])
        else:
            raise ValueError("Unsupported data type in JSON")

    def serve(self, json_paths, server_port):
        flattened_data = {f"SimpleMethod_{os.path.splitext(os.path.basename(path))[0]}": pa.Table.from_pandas(self.load_and_flatten_json(path)) for path in json_paths}
        self.server = FlightServer(flattened_data, server_port)
        self.server.serve()

    def do_put(self, flat_data: dict):
        if self.server is not None:
            self.server.do_put(flat_data)
        else:
            print("First, initialize the server")
