import pyarrow.flight as flight
import pyarrow.compute as pc
import pyarrow as pa
import pandas as pd

def client_example():
    ports = [50051, 50052, 50053, 50054]
    for port in ports:
        client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
        reader = client.do_get(flight.Ticket("get_all".encode()))
        table_names = reader.read_all().to_pandas()["table_name"].tolist()

        for table_name in table_names:
            reader = client.do_get(flight.Ticket(table_name.encode()))
            data = reader.read_all()
            print(f"Data from table '{table_name}':")
            print(data)
#Movies
def client_reddit():
    port = 50051
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")

    table_name = 'FlattenedJSON_movies'
    reader = client.do_get(flight.Ticket(table_name.encode()))
    data = reader.read_all()
    print(data)

    # SELECTION
    # first level query
    ## to string
    print(data.select(['title']))
    ## to int
    print(data.select(['year']))
    ## object from list 
    ### low level of nulls - first element of list
    print(data.select(['cast[0]']))
    ### medium level of nulls
    print(data.select(['cast[9]']))
    ### high level of nulls - last element of list
    print(data.select(['cast[58]']))

    # # FILTRES
    print(pc.filter(data, pc.greater(data.column('year'), 2000)))

    # #SORT
    print(pc.take(data,pc.sort_indices(data, sort_keys=[("title", "ascending")])))
    # #SORT DESC
    print(pc.take(data,pc.sort_indices(data, sort_keys=[("title", "descending")])))

    #GROUP BY
    print(pa.TableGroupBy(data,'year'))
    print(pa.TableGroupBy(data,'genres[0]'))
    print(pa.TableGroupBy(data, ['genres[0]','genres[1]']))
    print(pa.TableGroupBy(data,'cast[0]'))

    #AGGREGATE FUNCTION
    print(pa.TableGroupBy(data,'year').aggregate([("year", "count")]))
    print(pa.TableGroupBy(data, 'genres[0]').aggregate([('genres[0]', "count")]))
    print(pa.TableGroupBy(data, ['genres[0]','genres[1]']).aggregate([('genres[0]', "count"),('genres[1]', "count")]))

    #HIERARCHICAL - not supported by data

# ·       Zapytania agregujące - obliczanie średniej, sumy, maksimum, minimum, itp. dla określonego atrybutu w zestawie danych
# ·        Zapytania sortujące
# ·        Zapytania filtrujące
# ·        Zapytania grupujące- grupowanie danych na podstawie jednego lub więcej atrybutów i wykonanie określonej operacji na każdej grupie
# ·        Zapytania hierarchiczne: zapytania, które nawigują po strukturze hierarchicznej
if __name__ == '__main__':
    # client_reddit()
    client_example()
