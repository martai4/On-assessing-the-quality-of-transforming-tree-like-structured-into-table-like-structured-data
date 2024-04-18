import pyarrow.flight as flight
import pyarrow as pa
import pandas as pd
import json, os

from FlightServer import FlightServer

class JSONPathFlattener:
    """
    A class used to convert nested JSON data into a flat structure, and then serve these data as Apache Arrow tables over gRPC.
    """

    def __init__(self) -> None:
        self.server = None

    def flatten_json_structure(self, json_structure):
        """
        Flatten a JSON structure into a dictionary where keys are paths to values in the original JSON.

        Parameters:
        json_structure (dict or list): The JSON structure to be flattened.

        Returns:
        dict: A dictionary with paths to values in the original JSON as keys and corresponding values.
        """
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
        """
        Load JSON data from a file and flatten the data.

        Parameters:
        file_path (str): The path to the JSON file.

        Returns:
        list: A list of dictionaries with paths to values in the original JSON as keys and corresponding values.
        """
        with open(file_path, encoding='utf-8') as json_file:
            data = json.load(json_file)
            if isinstance(data, list):
                return [self.flatten_json_structure(item) for item in data]
            else:
                return [self.flatten_json_structure(data)]

    def serve(self, file_paths, server_port):
        """
        Serve the flattened JSON data as Apache Arrow tables over gRPC.

        Parameters:
        file_paths (list): A list of paths to the JSON files.
        server_port (int): The port on which the server will run.

        Returns:
        None
        """
        flattened_data = {f"FlattenedJSON_{os.path.splitext(os.path.basename(path))[0]}": pa.Table.from_pandas(pd.DataFrame(self.load_json_and_flatten(path))) for path in file_paths}
        server_location = flight.Location.for_grpc_tcp("0.0.0.0", server_port)
        self.server = FlightServer(server_location, flattened_data)
        print("Serving on", server_location)
        self.server.serve()

    def do_put(self, json_data):
        if self.server is not None:
            self.server.do_put(json_data)
        else:
            print("First, initialize the server")
