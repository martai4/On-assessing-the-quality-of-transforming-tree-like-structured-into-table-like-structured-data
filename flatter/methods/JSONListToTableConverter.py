import pyarrow as pa
import pandas as pd
import json, os

from FlightServer import FlightServer

class JSONListToTableConverter:
    def __init__(self) -> None:
        self.server = None

    def create_json_from_list(self, list_data, table_name):
        json_data = []
        for row_number, item in enumerate(list_data[table_name]):
            if isinstance(item, list):
                for element in item or [None]:
                    json_data.append({"row_number": row_number, "value": element})
            elif item is not None:
                json_data.append({"row_number": row_number, "value": item})
        return json_data

    def convert_lists_to_tables(self, dataframe, table_name, file_path):
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

    def serve(self, file_paths, server_port):
        results = []
        for file_path in file_paths:
            with open(file_path, encoding='utf-8') as json_file:
                data = json.load(json_file)
            tables = self.convert_lists_to_tables(data, '', os.path.splitext(os.path.basename(file_path))[0])
            results.append(tables)

        merged_results = {k: v for tables in results for k, v in tables.items()}
        self.server = FlightServer(merged_results, server_port)
        self.server.serve()

    def do_put(self, flat_data: dict):
        if self.server is not None:
            self.server.do_put(flat_data)
        else:
            print("First, initialize the server")
