import pyarrow.flight as flight
import pyarrow as pa
import pandas as pd


class FlightServer(flight.FlightServerBase):
    def __init__(self, tables, port, host="0.0.0.0"):
        location = flight.Location.for_grpc_tcp(host, port)
        print("Serving on", location)

        super(FlightServer, self).__init__(location)
        self.tables = tables

    def do_put(self, json_data):  # TODO implement me
        # JSON VALIDATION ...
        # for json in json_data:
        #     try:
        #         parsed_data = loads(json)
        #         print(f'Parsed data: {parsed_data}')
        #     except:
        #         print('Something is wrong with json!')

        pass

    def do_get(self, context, ticket):
        table_name = ticket.ticket.decode()
        if table_name == "get_all":
            arrow_tables = pa.Table.from_pandas(pd.DataFrame({"table_name": list(self.tables.keys())}))
            return flight.RecordBatchStream(arrow_tables)

        if table_name not in self.tables:
            raise ValueError("Table not found.")
        else:
            return flight.RecordBatchStream(self.tables[table_name])
