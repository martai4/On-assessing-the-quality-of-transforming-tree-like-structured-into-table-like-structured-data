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

    # port = 50052
    # client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    # table_name = 'FlattenedJSON_movies'
    # reader = client.do_get(flight.Ticket(table_name.encode()))
    # data = reader.read_all()

    # SELECTION
    # first level query
    ## to string
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
    # print(pa.TableGroupBy(data,'year'))

    # genres_column = data['genres']
    # first_genres  = [str(row[0]) if row else None for row in genres_column]
    # first_genre_table = pa.Table.from_arrays([first_genres], names=['first_genre'])
    # grouped_table = first_genre_table.group_by('first_genre')
    # print(grouped_table)
    
    
    # genres_column = data['genres']
    # genres_0 = [str(row[0]) if row else None for row in genres_column]
    # genres_1 = [str(row[1]) if len(row) > 1 else None for row in genres_column]
    # group_table = pa.Table.from_arrays([genres_0, genres_1], names=['genres_0', 'genres_1'])
    # grouped_table = group_table.group_by(['genres_0', 'genres_1'])
    # print(grouped_table)

    # cast_column = data['cast']
    # first_cast  = [str(row[0]) if row else None for row in cast_column]
    # first_cast_table = pa.Table.from_arrays([first_cast], names=['first_cast'])
    # grouped_table = first_cast_table.group_by('first_cast')
    # print(grouped_table)

    # #AGGREGATE FUNCTION
    # print(pa.TableGroupBy(data,'year').aggregate([("year", "count")]))
    # print(pa.TableGroupBy(data, 'genres_0').aggregate([('genres_0', "count")]))
    
    # genres_column = data['genres']
    # first_genres  = [str(row[0]) if row else None for row in genres_column]
    # first_genre_table = pa.Table.from_arrays([first_genres], names=['first_genre'])
    # grouped_table = first_genre_table.group_by('first_genre')
    # print(grouped_table.aggregate([('first_genre', 'count')]))
    
    # genres_0 = [str(row[0]) if row else None for row in genres_column]
    # genres_1 = [str(row[1]) if len(row) > 1 else None for row in genres_column]
    # group_table = pa.Table.from_arrays([genres_0, genres_1], names=['genres_0', 'genres_1'])
    # grouped_table = group_table.group_by(['genres_0', 'genres_1'])
    # print(grouped_table.aggregate([('genres_0', "count"),('genres_1', "count")]))
    
    # genres_as_string = data['genres'].combine_chunks().cast(pa.list_(pa.string())).flatten().cast(pa.string()).cast(pa.utf8())
    # data_with_strings = pa.table({'genres': genres_as_string})
    # grouped = data_with_strings.group_by('genres').aggregate([('genres',"count")])
    # print(grouped)
    
    port = 50053
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    
    main_table_name = 'TablesMethod_movies'
    cast_table_name = 'TablesMethod_movies_cast'
    genres_table_name = 'TablesMethod_movies_genres'
    
    reader = client.do_get(flight.Ticket(main_table_name.encode()))
    main_data = reader.read_all()
    
    reader = client.do_get(flight.Ticket(cast_table_name.encode()))
    cast_data = reader.read_all()
    
    reader = client.do_get(flight.Ticket(genres_table_name.encode()))
    genres_data = reader.read_all()
    
    # print(main_data)
    # print(cast_data)
    # print(genres_data)
    
    # SELECTION
    # first level query
    ## to string
    # print(main_data.select(['title']))
    # ## to int
    # print(main_data.select(['year']))
    # # ## object from list 
    # # ### low level of nulls - first element of list
    # print(cast_data)
    
    # grouped_table = cast_data.group_by('row_number')

    # cast_row_number=cast_data['row_number']
    # split_values = [v.split(', ') if v and v != "null" else [] for v in cast_data['value'].to_pylist()]
    # aggregated_values = {}
    # for row_number, value_list in zip(cast_row_number.to_pylist(), split_values):
    #     if row_number not in aggregated_values:
    #         aggregated_values[row_number] = value_list
    #     else:
    #         aggregated_values[row_number].extend(value_list)

    # first_values = [value_list[0] if value_list else None for value_list in aggregated_values.values()]
    # first_values_array = pa.array(first_values)
    # result_table = pa.Table.from_arrays([pa.array(list(aggregated_values.keys())), first_values_array], names=['row_number', 'first_value'])
    # print(result_table)

    # # ### medium level of nulls
    # cast_row_number=cast_data['row_number']
    # split_values = [v.split(', ') if v and v != "null" else [] for v in cast_data['value'].to_pylist()]
    # aggregated_values = {}
    # for row_number, value_list in zip(cast_row_number.to_pylist(), split_values):
    #     if row_number not in aggregated_values:
    #         aggregated_values[row_number] = value_list
    #     else:
    #         aggregated_values[row_number].extend(value_list)

    # tenth_values  = [value_list[9] if len(value_list) >= 10 else None for value_list in aggregated_values.values()]
    # tenth_values_array  = pa.array(tenth_values )
    # result_table = pa.Table.from_arrays([pa.array(list(aggregated_values.keys())), tenth_values_array ], names=['row_number', 'first_value'])
    # print(result_table)
    
    # # ### high level of nulls - last element of list
    # # print(data.select(['cast[58]']))
    # cast_row_number=cast_data['row_number']
    # split_values = [v.split(', ') if v and v != "null" else [] for v in cast_data['value'].to_pylist()]
    # aggregated_values = {}
    # for row_number, value_list in zip(cast_row_number.to_pylist(), split_values):
    #     if row_number not in aggregated_values:
    #         aggregated_values[row_number] = value_list
    #     else:
    #         aggregated_values[row_number].extend(value_list)

    # tenth_values  = [value_list[58] if len(value_list) > 58 else None for value_list in aggregated_values.values()]
    # tenth_values_array  = pa.array(tenth_values )
    # result_table = pa.Table.from_arrays([pa.array(list(aggregated_values.keys())), tenth_values_array ], names=['row_number', 'first_value'])
    # print(result_table)
    

    # # # FILTRES
    # print(pc.filter(main_data, pc.greater(main_data.column('year'), 2000)))

    # # #SORT
    # print(pc.take(main_data,pc.sort_indices(main_data, sort_keys=[("title", "ascending")])))
    # # #SORT DESC
    # print(pc.take(main_data,pc.sort_indices(main_data, sort_keys=[("title", "descending")])))

    # # #GROUP BY
    # print(pa.TableGroupBy(main_data,'year'))

    genres_row_number=genres_data['row_number']
    split_values = [v.split(', ') if v and v != "null" else [] for v in genres_data['value'].to_pylist()]
    aggregated_values = {}
    for row_number, value_list in zip(genres_row_number.to_pylist(), split_values):
        if row_number not in aggregated_values:
            aggregated_values[row_number] = value_list
        else:
            aggregated_values[row_number].extend(value_list)

    first_values = [value_list[0] if value_list else None for value_list in aggregated_values.values()]
    first_values_array = pa.array(first_values)
    result_table = pa.Table.from_arrays([pa.array(list(aggregated_values.keys())), first_values_array], names=['row_number', 'first_value'])
    print(result_table.group_by('first_value'))

    genres_row_number = genres_data['row_number']
    split_values = [v.split(', ') if v and v != "null" else [] for v in genres_data['value'].to_pylist()]
    aggregated_values = {}
    for row_number, value_list in zip(genres_row_number.to_pylist(), split_values):
        if row_number not in aggregated_values:
            aggregated_values[row_number] = value_list
        else:
            aggregated_values[row_number].extend(value_list)
    first_values = [value_list[0] if value_list else None for value_list in aggregated_values.values()]
    second_values = [value_list[1] if len(value_list) > 1 else None for value_list in aggregated_values.values()]
    first_values_array = pa.array(first_values)
    second_values_array = pa.array(second_values)
    result_table = pa.Table.from_arrays([pa.array(list(aggregated_values.keys())), first_values_array, second_values_array], names=['row_number', 'first_value', 'second_value'])
    grouped_table = result_table.group_by(['first_value', 'second_value'])
    print(grouped_table)

    cast_row_number=cast_data['row_number']
    split_values = [v.split(', ') if v and v != "null" else [] for v in cast_data['value'].to_pylist()]
    aggregated_values = {}
    for row_number, value_list in zip(cast_row_number.to_pylist(), split_values):
        if row_number not in aggregated_values:
            aggregated_values[row_number] = value_list
        else:
            aggregated_values[row_number].extend(value_list)

    first_values = [value_list[0] if value_list else None for value_list in aggregated_values.values()]
    first_values_array = pa.array(first_values)
    result_table = pa.Table.from_arrays([pa.array(list(aggregated_values.keys())), first_values_array], names=['row_number', 'first_value'])
    print(result_table.group_by('first_value'))

    # #AGGREGATE FUNCTION
    print(pa.TableGroupBy(main_data,'year').aggregate([("year", "count")]))
    
    genres_row_number=genres_data['row_number']
    split_values = [v.split(', ') if v and v != "null" else [] for v in genres_data['value'].to_pylist()]
    aggregated_values = {}
    for row_number, value_list in zip(genres_row_number.to_pylist(), split_values):
        if row_number not in aggregated_values:
            aggregated_values[row_number] = value_list
        else:
            aggregated_values[row_number].extend(value_list)

    first_values = [value_list[0] if value_list else None for value_list in aggregated_values.values()]
    first_values_array = pa.array(first_values)
    result_table = pa.Table.from_arrays([pa.array(list(aggregated_values.keys())), first_values_array], names=['row_number', 'first_value'])
    print(result_table.group_by('first_value').aggregate([('first_value', "count")]))
    
    genres_row_number = genres_data['row_number']
    split_values = [v.split(', ') if v and v != "null" else [] for v in genres_data['value'].to_pylist()]
    aggregated_values = {}
    for row_number, value_list in zip(genres_row_number.to_pylist(), split_values):
        if row_number not in aggregated_values:
            aggregated_values[row_number] = value_list
        else:
            aggregated_values[row_number].extend(value_list)
    first_values = [value_list[0] if value_list else None for value_list in aggregated_values.values()]
    second_values = [value_list[1] if len(value_list) > 1 else None for value_list in aggregated_values.values()]
    first_values_array = pa.array(first_values)
    second_values_array = pa.array(second_values)
    result_table = pa.Table.from_arrays([pa.array(list(aggregated_values.keys())), first_values_array, second_values_array], names=['row_number', 'first_value', 'second_value'])
    grouped_table = result_table.group_by(['first_value', 'second_value'])
    print(grouped_table.aggregate([('first_value', "count"),('second_value', "count")]))


    unique_row_numbers = pc.unique(genres_data['row_number'])
    grouped_data = {'row_number': [], 'value': []}
    for row_num in unique_row_numbers:
        mask = pc.equal(genres_data['row_number'], row_num)
        filtered_values = genres_data.filter(mask)['value'].to_pylist()
        grouped_data['row_number'].append(row_num.as_py())
        grouped_data['value'].append([val for val in filtered_values if val is not None])
    grouped_table = pa.table(grouped_data)
    
    genres_as_string = grouped_table['value'].combine_chunks().cast(pa.list_(pa.string())).flatten().cast(pa.string()).cast(pa.utf8())
    data_with_strings = pa.table({'genres': genres_as_string})
    grouped = data_with_strings.group_by('genres').aggregate([('genres',"count")])
    print(grouped)

if __name__ == '__main__':
    client_reddit()
    # client_example()