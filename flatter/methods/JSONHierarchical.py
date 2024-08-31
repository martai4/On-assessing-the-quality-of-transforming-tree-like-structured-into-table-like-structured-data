import pyarrow as pa
import pandas as pd
import json, os

from FlightServer import FlightServer

class JSONHierarchical:
    def __init__(self) -> None:
        self.server = None

    def serve(self, server_port: int, file_paths=None) -> None:
        self.server = FlightServer(server_port)
        if file_paths:
            self.load_json_from_file(file_paths)
        self.server.serve()

    def do_put(self, dataset_name: str, json_data) -> None:
        semi_structured_data = {f"HierarchicalJSON_{dataset_name}": self.convert_json_to_table(json_data)}
        self.server.do_put(semi_structured_data)

    def load_json_from_file(self, file_paths: list) -> None:
        semi_structured_data = {}
        for path in file_paths:
            with open(path, 'r', encoding='utf-8') as f:  # Specify the correct encoding
                json_data = json.load(f)  # Load the file contents as JSON
            semi_structured_data[f"HierarchicalJSON_{os.path.basename(path).split('.')[0]}"] = self.convert_json_to_table(json_data)

        self.server.do_put(semi_structured_data)

    def convert_json_to_table(self, json_data) -> pa.Table:
        if isinstance(json_data, list):
            df = pd.DataFrame(json_data)
            return pa.Table.from_pandas(df)
            
        elif isinstance(json_data, dict):
            df = pd.DataFrame([json_data])
            return pa.Table.from_pandas(df)