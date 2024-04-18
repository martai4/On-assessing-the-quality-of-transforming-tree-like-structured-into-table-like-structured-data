import pyarrow.flight as flight
import pyarrow as pa
import pandas as pd
import json, os

from FlightServer import FlightServer

class JSONFirstListFlattener:
    """
    A class used to flatten the first level of a JSON structure and serve the flattened data as Apache Arrow tables over gRPC.
    """
    def load_json_and_flatten(self, file_path):
        """
        Load JSON data from a file and flatten the first level of the JSON structure.

        Parameters:
        file_path (str): The path to the JSON file.

        Returns:
        DataFrame: A pandas DataFrame with the flattened JSON data.
        """
        return pd.json_normalize(json.load(open(file_path, encoding='utf-8')))

    def serve(self, file_paths, server_port):
        """
        Serve the flattened JSON data as Apache Arrow tables over gRPC.

        Parameters:
        file_paths (list): A list of paths to the JSON files.
        server_port (int): The port on which the server will run.

        Returns:
        None
        """
        flattened_data = {f"FlattenedJSON_{os.path.splitext(os.path.basename(path))[0]}": pa.Table.from_pandas(self.load_json_and_flatten(path)) for path in file_paths}
        server_location = flight.Location.for_grpc_tcp("0.0.0.0", server_port)
        server = FlightServer(server_location, flattened_data)
        print("Serving on", server_location)
        server.serve()
