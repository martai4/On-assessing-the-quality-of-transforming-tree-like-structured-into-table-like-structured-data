import pyarrow as pa
import pandas as pd
import json, os
from typing import Union

from FlightServer import FlightServer

class JSONListToTableConverter:
    def __init__(self) -> None:
        self.server = None

    def serve(self, server_port: int, file_paths=None) -> None:
        self.server = FlightServer(server_port)
        self.load_json_from_file(file_paths) if file_paths else None
        self.server.serve()

    def do_put(self, dataset_name: str, json) -> None:
        flat_data = self.flatten_json(json, dataset_name)
        self.server.do_put(flat_data)

    def load_json_from_file(self, file_paths: list) -> None:
        results = []
        for path in file_paths:
            json_data = json.load(open(path, encoding='utf-8'))
            tables = self.convert_lists_to_tables(json_data, os.path.basename(path))
            results.append(tables)

        flattened_data = {k: v for tables in results for k, v in tables.items()}
        self.server.do_put(flattened_data)

    def flatten_json(self, json, dataset_name) -> pa.Table:
        return self.convert_lists_to_tables(json, dataset_name)

 # --------------------------------------------------------------------------------------------------

    def convert_lists_to_tables(self, json, dataset_name: str, table_name='') -> pa.Table:
        dataframe = pd.json_normalize(json)
        list_columns = dataframe.applymap(lambda x: isinstance(x, list)).any().loc[lambda x: x].index.tolist()

        if list_columns:
            list_dataframe = dataframe[list_columns].copy()
            dataframe = dataframe.drop(columns=list_columns)
            dataframe['row_number'] = range(1, len(dataframe) + 1)

        main_table = pa.Table.from_pandas(dataframe)
        tables = {f"TablesMethod_{dataset_name}{'_' if table_name != '' else ''}{table_name}": main_table}

        for dropped_list_column in list_columns:
            new_json = self.create_json_from_list(list_dataframe, dropped_list_column)
            tables.update(self.convert_lists_to_tables(new_json, dataset_name, dropped_list_column))

        return tables

    def create_json_from_list(self, list_data, table_name) -> Union[list, dict]:
        json_data = []
        for row_number, item in enumerate(list_data[table_name]):
            if isinstance(item, list):
                for element in item or [None]:
                    json_data.append({"row_number": row_number, "value": element})
            elif item is not None:
                json_data.append({"row_number": row_number, "value": item})

        return json_data

