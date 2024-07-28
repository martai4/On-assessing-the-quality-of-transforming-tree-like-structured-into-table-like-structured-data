import pyarrow.flight as flight
import pyarrow.compute as pc
import pyarrow as pa
import pandas as pd
import random

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


def client():
    # port = 50051
    # client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    # table_name = 'JSONPath_nasa'
    # reader = client.do_get(flight.Ticket(table_name.encode()))
    # data = reader.read_all()

    # # # SELECTION
    # # # first level query
    # # ## to string
    # print(data.select(['nametype']))
    # # ## to int
    # print("No data")
    # # ## object from list 
    # # ### low level of nulls - first element of list
    # print(data.select(['geolocation.coordinates[0]']))
    # # ### medium level of nulls
    # print(data.select(['geolocation.coordinates[1]']))
    # # ### high level of nulls - last element of list
    # print("No data")

    # # # # FILTRES
    # print(pc.filter(data, pc.greater(data.column('year'), "2000")))

    # # # #SORT
    # print(pc.take(data,pc.sort_indices(data, sort_keys=[("nametype", "ascending")])))
    # # # #SORT DESC
    # print(pc.take(data,pc.sort_indices(data, sort_keys=[("nametype", "descending")])))

    # # #GROUP BY
    # print(pa.TableGroupBy(data,'year'))
    # print(pa.TableGroupBy(data,'geolocation.coordinates[0]'))
    # print(pa.TableGroupBy(data, ['geolocation.coordinates[0]','geolocation.coordinates[1]']))
    # print("No data")

    # # #AGGREGATE FUNCTION
    # print(pa.TableGroupBy(data,'year').aggregate([("year", "count")]))
    # print(pa.TableGroupBy(data, 'geolocation.coordinates[0]').aggregate([('geolocation.coordinates[0]', "count")]))
    # print(pa.TableGroupBy(data, ['geolocation.coordinates[0]','geolocation.coordinates[1]']).aggregate([('geolocation.coordinates[0]', "count"),('geolocation.coordinates[1]', "count")]))


    # print("No data")

    # # ##CLOUD PAK

    # #CONVERT TYPE
    # print(data['geolocation.coordinates[0]'].cast(pa.string()))
    # print(pa.Table.from_arrays([data.column('geolocation.coordinates[0]').cast(pa.string())], names=['geolocation.coordinates[0]']))

    # #CONCATENATE COLUMNS
    # title_column = data.column('nametype').cast('string')
    # year_column = data.column('year').cast('string')
    # concatenated = pc.binary_join_element_wise(title_column, year_column, '-')
    # concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(''), concatenated)
    # print(data.append_column('title-year', concatenated).select(['title-year']))

    # # #SPLIT COLUMN
    # titles = data['nametype']
    # title_split1 = []
    # title_split2 = []
    # for title in titles:
    #     split_title = title.as_py().split(' ', 1)
    #     title_split1.append(split_title[0])
    #     title_split2.append(split_title[1] if len(split_title) > 1 else '')
    # title_split1_array = pa.array(title_split1)
    # title_split2_array = pa.array(title_split2)
    # split_data = pa.table([title_split1_array, title_split2_array], names=['title_split1', 'title_split2'])

    # print(split_data)

    # #REPLACE MISSING VALUES
    # data1 = data
    # genres1 = data1['mass']
    # replaced_genres1 = pc.fill_null(genres1, pa.scalar('No mass'))
    # new_table = data1.set_column(data1.schema.get_field_index('mass'), 'mass', replaced_genres1)
    # print(new_table)

    # # #SELF JOIN
    # data_left = data
    # data_right = data

    # filter_mask = pc.is_in(data_left['nametype'], data_right['nametype'])
    # filtered_data_left = data_left.filter(filter_mask)
    # joined_data = pa.concat_tables([filtered_data_left, data_right])
    # print(joined_data)

    # # #UNION TABLES
    # data_left = data
    # data_right = data
    # unioned_data = pa.concat_tables([data_right, data_left])
    # print(unioned_data)

    # #SAMPLE TABLE

    # num_rows = data.num_rows
    # n = int(num_rows * (10 / 100.0))
    # sample_indices = random.sample(range(num_rows), n)
    # sampled_table = data.take(sample_indices)
    # print(sampled_table)

    # # REMOVE COLUMN
    # column_names = data.column_names
    # column_names.remove('nametype')
    # print(data.select(column_names))


    # port = 50054
    # client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    # table_name = 'SimpleMethod_nasa'
    # reader = client.do_get(flight.Ticket(table_name.encode()))
    # data = reader.read_all()

    # # # SELECTION
    # # # first level query
    # # ## to string
    # print(data.select(['nametype']))
    # # ## to int
    # print("No data")
    # # ## object from list 
    # # ### low level of nulls - first element of list
    # print(data.select(['geolocation_coordinates_0']))
    # # ### medium level of nulls
    # print(data.select(['geolocation_coordinates_1']))
    # # ### high level of nulls - last element of list
    # print("No data")

    # # # # FILTRES
    # print(pc.filter(data, pc.greater(data.column('year'), "2000")))

    # # # #SORT
    # print(pc.take(data,pc.sort_indices(data, sort_keys=[("nametype", "ascending")])))
    # # # #SORT DESC
    # print(pc.take(data,pc.sort_indices(data, sort_keys=[("nametype", "descending")])))

    # # #GROUP BY
    # print(pa.TableGroupBy(data,'year'))
    # print(pa.TableGroupBy(data,'geolocation_coordinates_0'))
    # print(pa.TableGroupBy(data, ['geolocation_coordinates_0','geolocation_coordinates_1']))
    # print("No data")

    # #AGGREGATE FUNCTION
    # print(pa.TableGroupBy(data,'year').aggregate([("year", "count")]))
    # print(pa.TableGroupBy(data, 'geolocation_coordinates_0').aggregate([('geolocation_coordinates_0', "count")]))
    # print(pa.TableGroupBy(data, ['geolocation_coordinates_0','geolocation_coordinates_1']).aggregate([('geolocation_coordinates_0', "count"),('geolocation_coordinates_1', "count")]))

    # print("No data")

    # # ##CLOUD PAK

    # # #CONVERT TYPE
    # print(data['geolocation_coordinates_0'].cast(pa.string()))
    # print(pa.Table.from_arrays([data.column('geolocation_coordinates_0').cast(pa.string())], names=['geolocation_coordinates_0']))

    # #CONCATENATE COLUMNS
    # title_column = data.column('nametype').cast('string')
    # year_column = data.column('year').cast('string')
    # concatenated = pc.binary_join_element_wise(title_column, year_column, '-')
    # concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(''), concatenated)
    # print(data.append_column('title-year', concatenated).select(['title-year']))

    # #SPLIT COLUMN
    # titles = data['nametype']
    # title_split1 = []
    # title_split2 = []
    # for title in titles:
    #     split_title = title.as_py().split(' ', 1)
    #     title_split1.append(split_title[0])
    #     title_split2.append(split_title[1] if len(split_title) > 1 else '')
    # title_split1_array = pa.array(title_split1)
    # title_split2_array = pa.array(title_split2)
    # split_data = pa.table([title_split1_array, title_split2_array], names=['title_split1', 'title_split2'])

    # print(split_data)

    # #REPLACE MISSING VALUES
    # data1 = data
    # genres1 = data1['mass']
    # replaced_genres1 = pc.fill_null(genres1, pa.scalar('No mass'))
    # new_table = data1.set_column(data1.schema.get_field_index('mass'), 'mass]', replaced_genres1)
    # print(new_table)

    # #SELF JOIN
    # data_left = data
    # data_right = data

    # filter_mask = pc.is_in(data_left['nametype'], data_right['nametype'])
    # filtered_data_left = data_left.filter(filter_mask)
    # joined_data = pa.concat_tables([filtered_data_left, data_right])
    # print(joined_data)

    # #UNION TABLES
    # data_left = data
    # data_right = data
    # unioned_data = pa.concat_tables([data_right, data_left])
    # print(unioned_data)

    # # #SAMPLE TABLE

    # num_rows = data.num_rows
    # n = int(num_rows * (10 / 100.0))
    # sample_indices = random.sample(range(num_rows), n)
    # sampled_table = data.take(sample_indices)
    # print(sampled_table)

    # # REMOVE COLUMN
    # column_names = data.column_names
    # column_names.remove('nametype')
    # print(data.select(column_names))

    port = 50052
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    table_name = 'FlattenedFirstJSON_nasa'
    reader = client.do_get(flight.Ticket(table_name.encode()))
    data = reader.read_all()

    print(data)

    # # # SELECTION
    # # # first level query
    # # ## to string
    print(data.select(['nametype']))
    # # ## to int
    print("No data")
    # # ## object from list 
    # # ### low level of nulls - first element of list
    cast_column=data['geolocation.coordinates']
    cast_list = cast_column.to_pylist()
    first_elements = []
    for row in cast_list:
        if row:  # Check if the list is not empty
            first_elements.append(row[0])
        else:
            first_elements.append(None)
    first_elements_column = pa.array(first_elements)
    new_table = data.set_column(data.schema.get_field_index('geolocation.coordinates'), 'geolocation.coordinates', first_elements_column)
    print(new_table.select(['geolocation.coordinates']))

    # # ### medium level of nulls
    # print(data.select(['geolocation_coordinates_1']))
    cast_column = data['geolocation.coordinates']
    cast_list = cast_column.to_pylist()
    tenth_elements = []
    for row in cast_list:
        if row and len(row) >= 2:
            tenth_elements.append(row[1])
        else:
            tenth_elements.append(None)
    tenth_elements_column = pa.array(tenth_elements)
    new_table = data.set_column(data.schema.get_field_index('geolocation.coordinates'), 'geolocation.coordinates', tenth_elements_column)
    print(new_table.select(['geolocation.coordinates']))
    # # ### high level of nulls - last element of list
    print("No data")

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


    # ##CLOUD PAK

    # #CONVERT TYPE
    # # print(data['year'].cast(pa.string()))
    # print(pa.Table.from_arrays([data.column('year').cast(pa.string())], names=['year']))

    # #CONCATENATE COLUMNS
    # title_column = data.column('title').cast('string')
    # year_column = data.column('year').cast('string')
    # concatenated = pc.binary_join_element_wise(title_column, year_column, '-')
    # concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(''), concatenated)
    # print(data.append_column('title-year', concatenated).select(['title-year']))

    # #SPLIT COLUMN
    # titles = data['title']
    # title_split1 = []
    # title_split2 = []
    # for title in titles:
    #     split_title = title.as_py().split(' ', 1)
    #     title_split1.append(split_title[0])
    #     title_split2.append(split_title[1] if len(split_title) > 1 else '')
    # title_split1_array = pa.array(title_split1)
    # title_split2_array = pa.array(title_split2)
    # split_data = pa.table([title_split1_array, title_split2_array], names=['title_split1', 'title_split2'])

    # print(split_data)

    # #REPLACE MISSING VALUES
    # data1 = data
    # genres1 = data1['genres'].to_pylist()
    # replaced_genres1 = []
    # for row in genres1:
    #     if len(row) >= 2:
    #         replaced_genres1.append(row[1])
    #     else:
    #         replaced_genres1.append('No genres1')
    # replaced_genres1 = pa.array(tenth_elements)
    # new_table = data1.set_column(data1.schema.get_field_index('genres'), 'genres_1]', replaced_genres1)
    # print(new_table)

    # #SELF JOIN
    # data_left = data
    # data_right = data

    # filter_mask = pc.is_in(data_left['title'], data_right['title'])
    # filtered_data_left = data_left.filter(filter_mask)
    # joined_data = pa.concat_tables([filtered_data_left, data_right])
    # print(joined_data)

    # #UNION TABLES
    # data_left = data
    # data_right = data
    # unioned_data = pa.concat_tables([data_right, data_left])
    # print(unioned_data)

    # #SAMPLE TABLE

    # num_rows = data.num_rows
    # n = int(num_rows * (10 / 100.0))
    # sample_indices = random.sample(range(num_rows), n)
    # sampled_table = data.take(sample_indices)
    # print(sampled_table)

    # # REMOVE COLUMN
    # column_names = data.column_names
    # column_names.remove('title')
    # print(data.select(column_names))





    # port = 50053
    # client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    # main_table_name = 'TablesMethod_movies'
    # cast_table_name = 'TablesMethod_movies_cast'
    # genres_table_name = 'TablesMethod_movies_genres'

    # reader = client.do_get(flight.Ticket(main_table_name.encode()))
    # main_data = reader.read_all()

    # reader = client.do_get(flight.Ticket(cast_table_name.encode()))
    # cast_data = reader.read_all()

    # reader = client.do_get(flight.Ticket(genres_table_name.encode()))
    # genres_data = reader.read_all()

    # # SELECTION
    # # first level query
    # # to string
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

    # genres_row_number=genres_data['row_number']
    # split_values = [v.split(', ') if v and v != "null" else [] for v in genres_data['value'].to_pylist()]
    # aggregated_values = {}
    # for row_number, value_list in zip(genres_row_number.to_pylist(), split_values):
    #     if row_number not in aggregated_values:
    #         aggregated_values[row_number] = value_list
    #     else:
    #         aggregated_values[row_number].extend(value_list)

    # first_values = [value_list[0] if value_list else None for value_list in aggregated_values.values()]
    # first_values_array = pa.array(first_values)
    # result_table = pa.Table.from_arrays([pa.array(list(aggregated_values.keys())), first_values_array], names=['row_number', 'first_value'])
    # print(result_table.group_by('first_value'))

    # genres_row_number = genres_data['row_number']
    # split_values = [v.split(', ') if v and v != "null" else [] for v in genres_data['value'].to_pylist()]
    # aggregated_values = {}
    # for row_number, value_list in zip(genres_row_number.to_pylist(), split_values):
    #     if row_number not in aggregated_values:
    #         aggregated_values[row_number] = value_list
    #     else:
    #         aggregated_values[row_number].extend(value_list)
    # first_values = [value_list[0] if value_list else None for value_list in aggregated_values.values()]
    # second_values = [value_list[1] if len(value_list) > 1 else None for value_list in aggregated_values.values()]
    # first_values_array = pa.array(first_values)
    # second_values_array = pa.array(second_values)
    # result_table = pa.Table.from_arrays([pa.array(list(aggregated_values.keys())), first_values_array, second_values_array], names=['row_number', 'first_value', 'second_value'])
    # grouped_table = result_table.group_by(['first_value', 'second_value'])
    # print(grouped_table)

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
    # print(result_table.group_by('first_value'))

    # # #AGGREGATE FUNCTION
    # print(pa.TableGroupBy(main_data,'year').aggregate([("year", "count")]))

    # genres_row_number=genres_data['row_number']
    # split_values = [v.split(', ') if v and v != "null" else [] for v in genres_data['value'].to_pylist()]
    # aggregated_values = {}
    # for row_number, value_list in zip(genres_row_number.to_pylist(), split_values):
    #     if row_number not in aggregated_values:
    #         aggregated_values[row_number] = value_list
    #     else:
    #         aggregated_values[row_number].extend(value_list)

    # first_values = [value_list[0] if value_list else None for value_list in aggregated_values.values()]
    # first_values_array = pa.array(first_values)
    # result_table = pa.Table.from_arrays([pa.array(list(aggregated_values.keys())), first_values_array], names=['row_number', 'first_value'])
    # print(result_table.group_by('first_value').aggregate([('first_value', "count")]))

    # genres_row_number = genres_data['row_number']
    # split_values = [v.split(', ') if v and v != "null" else [] for v in genres_data['value'].to_pylist()]
    # aggregated_values = {}
    # for row_number, value_list in zip(genres_row_number.to_pylist(), split_values):
    #     if row_number not in aggregated_values:
    #         aggregated_values[row_number] = value_list
    #     else:
    #         aggregated_values[row_number].extend(value_list)
    # first_values = [value_list[0] if value_list else None for value_list in aggregated_values.values()]
    # second_values = [value_list[1] if len(value_list) > 1 else None for value_list in aggregated_values.values()]
    # first_values_array = pa.array(first_values)
    # second_values_array = pa.array(second_values)
    # result_table = pa.Table.from_arrays([pa.array(list(aggregated_values.keys())), first_values_array, second_values_array], names=['row_number', 'first_value', 'second_value'])
    # grouped_table = result_table.group_by(['first_value', 'second_value'])
    # print(grouped_table.aggregate([('first_value', "count"),('second_value', "count")]))


    # unique_row_numbers = pc.unique(genres_data['row_number'])
    # grouped_data = {'row_number': [], 'value': []}
    # for row_num in unique_row_numbers:
    #     mask = pc.equal(genres_data['row_number'], row_num)
    #     filtered_values = genres_data.filter(mask)['value'].to_pylist()
    #     grouped_data['row_number'].append(row_num.as_py())
    #     grouped_data['value'].append([val for val in filtered_values if val is not None])
    # grouped_table = pa.table(grouped_data)

    # genres_as_string = grouped_table['value'].combine_chunks().cast(pa.list_(pa.string())).flatten().cast(pa.string()).cast(pa.utf8())
    # data_with_strings = pa.table({'genres': genres_as_string})
    # grouped = data_with_strings.group_by('genres').aggregate([('genres',"count")])
    # print(grouped)

    #     ##CLOUD PAK

    # #CONVERT TYPE
    # # print(data['year'].cast(pa.string()))
    # print(pa.Table.from_arrays([data.column('year').cast(pa.string())], names=['year']))

    # #CONCATENATE COLUMNS
    # title_column = data.column('title').cast('string')
    # year_column = data.column('year').cast('string')
    # concatenated = pc.binary_join_element_wise(title_column, year_column, '-')
    # concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(''), concatenated)
    # print(data.append_column('title-year', concatenated).select(['title-year']))

    # #SPLIT COLUMN
    # titles = data['title']
    # title_split1 = []
    # title_split2 = []
    # for title in titles:
    #     split_title = title.as_py().split(' ', 1)
    #     title_split1.append(split_title[0])
    #     title_split2.append(split_title[1] if len(split_title) > 1 else '')
    # title_split1_array = pa.array(title_split1)
    # title_split2_array = pa.array(title_split2)
    # split_data = pa.table([title_split1_array, title_split2_array], names=['title_split1', 'title_split2'])

    # print(split_data)

    # #REPLACE MISSING VALUES

    # genres_data_row_number=genres_data['row_number']
    # split_values = [v.split(', ') if v and v != "null" else [] for v in genres_data['value'].to_pylist()]
    # aggregated_values = {}
    # for row_number, value_list in zip(genres_data_row_number.to_pylist(), split_values):
    #     if row_number not in aggregated_values:
    #         aggregated_values[row_number] = value_list
    #     else:
    #         aggregated_values[row_number].extend(value_list)

    # second_values  = [value_list[1] if len(value_list) >= 2 else None for value_list in aggregated_values.values()]
    # second_values  = ['No genres1' if pc.is_null(value_list) else value_list for value_list in second_values]
    # second_values_array  = pa.array(second_values)
    # result_table = pa.Table.from_arrays([pa.array(list(aggregated_values.keys())), second_values_array ], names=['row_number', 'first_value'])
    # print(result_table)

    # #SELF JOIN
    # data_left = data
    # data_with_index = data_left.append_column('index', pa.array(range(len(data))))
    # data_right = cast_data

    # filter_mask = pc.is_in(data_with_index['index'], data_right['row_number'])
    # filtered_data_left = data_with_index.filter(filter_mask)
    # data_right = data_right.rename_columns(['index', 'value'])
    # joined_data = pa.concat_tables([filtered_data_left, data_right], promote=True)
    # print(joined_data)

    # #UNION TABLES
    # data_left = data
    # data_right = data
    # unioned_data = pa.concat_tables([data_right, data_left])
    # print(unioned_data)

    # #SAMPLE TABLE

    # num_rows = data.num_rows
    # n = int(num_rows * (10 / 100.0))
    # sample_indices = random.sample(range(num_rows), n)
    # sampled_table = data.take(sample_indices)
    # print(sampled_table)

    # # REMOVE COLUMN
    # column_names = data.column_names
    # column_names.remove('title')
    # print(data.select(column_names))

if __name__ == '__main__':
    random.seed(23)
    client()
    # client_example()