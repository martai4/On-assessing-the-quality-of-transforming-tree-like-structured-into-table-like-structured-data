import pyarrow.flight as flight
import pyarrow as pa
import pandas as pd
import json
import os
from flatten_json import flatten

from FlightServer import FlightServer

class FlattenJSON:
    """
    This class provides methods to load and flatten JSON data, and to serve the data using FlightServer.
    """

    def load_and_flatten_json(self, json_path):
        """
        Load JSON data from a file and flatten it.

        Parameters:
        json_path (str): The path to the JSON file.

        Returns:
        DataFrame: A pandas DataFrame containing the flattened JSON data.
        """
        json_data = json.load(open(json_path, encoding='utf-8'))
        if isinstance(json_data, list):
            return pd.DataFrame([flatten(data) for data in json_data])
        elif isinstance(json_data, dict):
            return pd.DataFrame([flatten(json_data)])
        else:
            raise ValueError("Unsupported data type in JSON")

    def serve(self, json_paths, server_port):
        """
        Serve the flattened JSON data using FlightServer.

        Parameters:
        json_paths (list): A list of paths to the JSON files.
        server_port (int): The port to serve the data on.
        """
        flattened_data = {f"SimpleMethod_{os.path.splitext(os.path.basename(path))[0]}": pa.Table.from_pandas(self.load_and_flatten_json(path)) for path in json_paths}
        server_location = flight.Location.for_grpc_tcp("0.0.0.0", server_port)
        flight_server = FlightServer(server_location, flattened_data)
        print("Serving on", server_location)
        flight_server.serve()
