import pyarrow.flight as flight
import pyarrow.compute as pc
import pyarrow as pa
import pandas as pd

def client_example():
    ports = [50051, 50052, 50053, 50054]
    for port in ports:
        client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
        reader = client.do_get(flight.Ticket("get_table_names".encode())) 
        table_names = reader.read_all().to_pandas()["table_name"].tolist()

        for table_name in table_names:
            reader = client.do_get(flight.Ticket(table_name.encode()))
            data = reader.read_all()
            print(f"Data from table {port}:: '{table_name}':")
            # print(data)
#Movies
def client_reddit():
    # port = 50051
    # client = flight.FlightClient(f"grpc+tcp://localhost:{port}")

    # table_name = 'FlattenedJSON_movies'
    # reader = client.do_get(flight.Ticket(table_name.encode()))
    # data = reader.read_all()

    # # SELECTION
    # # first level query
    # ## to string
    # print(data.select(['title']))
    # ## to int
    # print(data.select(['year']))
    # ## object from list 
    # ### low level of nulls - first element of list
    # print(data.select(['cast[0]']))
    # ### medium level of nulls
    # print(data.select(['cast[9]']))
    # ### high level of nulls - last element of list
    # print(data.select(['cast[58]']))

    # # # FILTRES
    # print(pc.filter(data, pc.greater(data.column('year'), 2000)))

    # # #SORT
    # print(pc.take(data,pc.sort_indices(data, sort_keys=[("title", "ascending")])))
    # # #SORT DESC
    # print(pc.take(data,pc.sort_indices(data, sort_keys=[("title", "descending")])))

    # #GROUP BY
    # print(pa.TableGroupBy(data,'year'))
    # print(pa.TableGroupBy(data,'genres[0]'))
    # print(pa.TableGroupBy(data, ['genres[0]','genres[1]']))
    # print(pa.TableGroupBy(data,'cast[0]'))

    # #AGGREGATE FUNCTION
    # print(pa.TableGroupBy(data,'year').aggregate([("year", "count")]))
    # print(pa.TableGroupBy(data, 'genres[0]').aggregate([('genres[0]', "count")]))
    # print(pa.TableGroupBy(data, ['genres[0]','genres[1]']).aggregate([('genres[0]', "count"),('genres[1]', "count")]))


    # combined_df = []
    # for genre in filter(lambda x: ('genre' in x), data.schema.names):
    #     print(genre)
    #     df_genre = data.select([genre]).rename_columns(['genre'])
    #     combined_df.append(df_genre)
    
    # combined_df = pa.concat_tables(combined_df)
    # print(combined_df)
    # combined_df = combined_df.combine_chunks()
    # print(pa.TableGroupBy(combined_df, ['genre']).aggregate([('genre', "count")]))

    # port = 50054
    # client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    # table_name = 'SimpleMethod_movies'
    # reader = client.do_get(flight.Ticket(table_name.encode()))
    # data = reader.read_all()
    
    # # SELECTION
    # # first level query
    # ## to string
    # print(data.select(['title']))
    # ## to int
    # print(data.select(['year']))
    # ## object from list 
    # ### low level of nulls - first element of list
    # print(data.select(['cast_0']))
    # # ### medium level of nulls
    # print(data.select(['cast_9']))
    # # ### high level of nulls - last element of list
    # print(data.select(['cast_58']))

    # # # FILTRES
    # print(pc.filter(data, pc.greater(data.column('year'), 2000)))

    # # #SORT
    # print(pc.take(data,pc.sort_indices(data, sort_keys=[("title", "ascending")])))
    # # #SORT DESC
    # print(pc.take(data,pc.sort_indices(data, sort_keys=[("title", "descending")])))

    # #GROUP BY
    # print(pa.TableGroupBy(data,'year'))
    # print(pa.TableGroupBy(data,'genres_0'))
    # print(pa.TableGroupBy(data, ['genres_0','genres_1']))
    # print(pa.TableGroupBy(data,'cast_0'))

    # #AGGREGATE FUNCTION
    # print(pa.TableGroupBy(data,'year').aggregate([("year", "count")]))
    # print(pa.TableGroupBy(data, 'genres_0').aggregate([('genres_0', "count")]))
    # print(pa.TableGroupBy(data, ['genres_0','genres_1']).aggregate([('genres_0', "count"),('genres_1', "count")]))

    # combined_df = []
    # for genre in filter(lambda x: ('genres_' in x), data.schema.names):
    #     df_genre = data.select([genre]).rename_columns(['genre'])
    #     combined_df.append(df_genre)
    
    # combined_df = pa.concat_tables(combined_df)
    # combined_df = combined_df.combine_chunks()
    # print(pa.TableGroupBy(combined_df, ['genre']).aggregate([('genre', "count")]))

    # port = 50053
    # client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    # table_name = 'TablesMethod_movies'
    # reader = client.do_get(flight.Ticket(table_name.encode()))
    # data = reader.read_all()

    port = 50052
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    table_name = 'FlattenedJSON_movies'
    reader = client.do_get(flight.Ticket(table_name.encode()))
    data = reader.read_all()

    # # SELECTION
    # # first level query
    # ## to string
    # print(data.select(['title']))
    # ## to int
    # print(data.select(['year']))
    # # object from list 
    # ### low level of nulls - first element of list
    # cast_column=data['cast']
    # cast_list = cast_column.to_pylist()
    # first_elements = []
    # for row in cast_list:
    #     if row:  # Check if the list is not empty
    #         first_elements.append(row[0])
    #     else:
    #         first_elements.append(None)
    # first_elements_column = pa.array(first_elements)
    # new_table = data.set_column(data.schema.get_field_index('cast'), 'cast', first_elements_column)

    # print(new_table.select(['cast']))
    # # # ### medium level of nulls
    # cast_column = data['cast']
    # cast_list = cast_column.to_pylist()
    # tenth_elements = []
    # for row in cast_list:
    #     if len(row) >= 10:
    #         tenth_elements.append(row[9])
    #     else:
    #         tenth_elements.append(None)
    # tenth_elements_column = pa.array(tenth_elements)
    # new_table = data.set_column(data.schema.get_field_index('cast'), 'cast', tenth_elements_column)

    # print(new_table.select(['cast']))
    # # # ### high level of nulls - last element of list
    # cast_column = data['cast']
    # cast_list = cast_column.to_pylist()
    # tenth_elements = []
    # for row in cast_list:
    #     if len(row) >= 59:
    #         tenth_elements.append(row[58])
    #     else:
    #         tenth_elements.append(None)
    # tenth_elements_column = pa.array(tenth_elements)
    # new_table = data.set_column(data.schema.get_field_index('cast'), 'cast', tenth_elements_column)

    # print(new_table.select(['cast']))

    # # # # FILTRES
    # print(pc.filter(data, pc.greater(data.column('year'), 2000)))

    # # # #SORT
    # print(pc.take(data,pc.sort_indices(data, sort_keys=[("title", "ascending")])))
    # # #SORT DESC
    # print(pc.take(data,pc.sort_indices(data, sort_keys=[("title", "descending")])))

    # #GROUP BY
    print(pa.TableGroupBy(data,'year'))

    genres_column = data['genres']
    first_genres  = [row[0] if row else None for row in genres_column]
    first_genre_table = pa.Table.from_arrays([first_genres], names=['first_genre'])
    grouped_table = first_genre_table.group_by('first_genre')
    print(grouped_table)

    genres_column = data['genres']
    genres_0 = [row[0] if row else None for row in genres_column]
    genres_1 = [row[1] if len(row) > 1 else None for row in genres_column]
    group_table = pa.Table.from_arrays([genres_0, genres_1], names=['genres_0', 'genres_1'])
    grouped_table = group_table.group_by(['genres_0', 'genres_1'])
    print(grouped_table)

    cast_column = data['cast']
    first_cast  = [row[0] if row else None for row in cast_column]
    first_cast_table = pa.Table.from_arrays([first_cast], names=['first_cast'])
    grouped_table = first_cast_table.group_by('first_cast')
    print(grouped_table)

    # #AGGREGATE FUNCTION
    print(pa.TableGroupBy(data,'year').aggregate([("year", "count")]))
    # print(pa.TableGroupBy(data, 'genres_0').aggregate([('genres_0', "count")]))
    # print(pa.TableGroupBy(data, ['genres_0','genres_1']).aggregate([('genres_0', "count"),('genres_1', "count")]))
    # genres_column = data['genres']
    # first_genres = [row[0] if row else None for row in genres_column]
    # first_genre_table = pa.Table.from_arrays([first_genres], names=['first_genre'])
    # grouped_table = first_genre_table.group_by('first_genre').aggregate(     
    #     count=pc.count,
    # )
    print(grouped_table)

    print(pa.TableGroupBy(data,'genres').aggregate([("genres", "count")]))

if __name__ == '__main__':
    client_reddit()
    # client_example()
# title: string
# year: int64
# cast: list<item: string>
#   child 0, item: string
# genres: list<item: string>
#   child 0, item: string
# href: string
# extract: string
# thumbnail: string
# thumbnail_width: double
# thumbnail_height: double