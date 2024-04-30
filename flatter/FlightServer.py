import pyarrow.flight as flight
import pyarrow as pa
import pandas as pd


class FlightServer(flight.FlightServerBase):
    def __init__(self, port: int, host="0.0.0.0"):
        self.tables = {} # dict of pa.Tables
        location = flight.Location.for_grpc_tcp(host, port)
        super(FlightServer, self).__init__(location)

        print("Serving on", location)

    def do_put(self, flat_data: dict):
        for key in flat_data.keys():
            self.tables[key] = pa.concat_tables([self.tables[key], flat_data[key]]).combine_chunks() \
                if key in self.tables.keys() \
                else flat_data[key]

    def do_get(self, context, ticket):
        table_name = ticket.ticket.decode()
        if table_name == "get_all":
            arrow_tables = pa.Table.from_pandas(pd.DataFrame({"table_name": list(self.tables.keys())}))
            return flight.RecordBatchStream(arrow_tables)

        if table_name in self.tables:
            return flight.RecordBatchStream(self.tables[table_name])
        else:
            raise ValueError("Table not found.")

    def stop(self):
        super().shutdown()
