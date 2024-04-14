import pyarrow.flight as flight
import pyarrow as pa
import pandas as pd
import json, os

from FlightServer import FlightServer

class JSONListToTableConverter:
    """
    A class used to convert JSON data that may contain lists into separate tables,
    and then serve these tables as Apache Arrow tables over gRPC.
    """
        
    def create_json_from_list(self, list_data, table_name):
        """
        Create a new JSON structure from a list.

        Parameters:
        list_data (list): The list of data to be converted into JSON.
        table_name (str): The name of the table.

        Returns:
        list: A list of dictionaries with row numbers and values for each item in the list.
        """
        json_data = []
        for row_number, item in enumerate(list_data[table_name]):
            if isinstance(item, list):
                for element in item or [None]:
                    json_data.append({"row_number": row_number, "value": element})
            elif item is not None:
                json_data.append({"row_number": row_number, "value": item})
        return json_data

    def convert_lists_to_tables(self, dataframe, table_name, file_path):
        """
        Convert lists in the data to separate tables.

        Parameters:
        dataframe (DataFrame): The pandas DataFrame to be converted.
        table_name (str): The name of the table.
        file_path (str): The path to the file.

        Returns:
        dict: A dictionary of tables with keys based on the file path and name.
        """
        dataframe = pd.json_normalize(dataframe)
        list_columns = dataframe.applymap(lambda x: isinstance(x, list)).any().loc[lambda x: x].index.tolist()

        if list_columns:
            list_dataframe = dataframe[list_columns].copy()
            dataframe = dataframe.drop(columns=list_columns)
            dataframe['row_number'] = range(1, len(dataframe) + 1)

        main_table = pa.Table.from_pandas(dataframe)
        tables = {f"TablesMethod_{file_path}{'_' if table_name != '' else ''}{table_name}": main_table}

        for dropped_list_column in list_columns:
            new_json = self.create_json_from_list(list_dataframe, dropped_list_column)
            tables.update(self.convert_lists_to_tables(new_json, dropped_list_column, file_path))

        return tables

    def serve_tables(self, file_paths, server_port):
        """
        Serve the converted tables over gRPC.

        Parameters:
        file_paths (list): A list of paths to the JSON files.
        server_port (int): The port on which the server will run.

        Returns:
        None
        """
        results = []
        for file_path in file_paths:
            with open(file_path, encoding='utf-8') as json_file:
                data = json.load(json_file)
            tables = self.convert_lists_to_tables(data, '', os.path.splitext(os.path.basename(file_path))[0])
            results.append(tables)

        merged_results = {k: v for tables in results for k, v in tables.items()}

        server_location = flight.Location.for_grpc_tcp("0.0.0.0", server_port)
        server = FlightServer(server_location, merged_results)
        print("Serving on", server_location)
        server.serve()
