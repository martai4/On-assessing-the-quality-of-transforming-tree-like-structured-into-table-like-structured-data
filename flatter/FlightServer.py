import pyarrow.flight as flight
import pyarrow as pa
import pandas as pd

class FlightServer(flight.FlightServerBase):
    """
    A FlightServer class that extends the FlightServerBase from the pyarrow.flight module.
    
    Attributes:
        tables (dict): A dictionary where the keys are table names and the values are pyarrow Tables.
    """
    
    def __init__(self, location, tables):
        """
        The constructor for FlightServer class.

        Parameters:
            location (str): The location where the server is hosted.
            tables (dict): A dictionary where the keys are table names and the values are pyarrow Tables.
        """
        super(FlightServer, self).__init__(location)
        self.tables = tables

    def do_get(self, context, ticket):
        """
        Overrides the do_get method from FlightServerBase. It is called when a client requests a data stream.

        Parameters:
            context (ServerContext): An instance of the ServerContext class that provides methods to examine and manipulate the call context.
            ticket (Ticket): An instance of the Ticket class that includes the ticket provided by the client.

        Returns:
            RecordBatchStream: A stream of record batches, which form a table when collected.

        Raises:
            ValueError: If the table name provided in the ticket is not found in the server's tables.
        """
        table_name = ticket.ticket.decode()
        if table_name == "get_table_names":
            table_names = pa.Table.from_pandas(pd.DataFrame({"table_name": list(self.tables.keys())}))
            return flight.RecordBatchStream(table_names)
        elif table_name not in self.tables:
            raise ValueError("Table not found.")
        else:
            return flight.RecordBatchStream(self.tables[table_name])
