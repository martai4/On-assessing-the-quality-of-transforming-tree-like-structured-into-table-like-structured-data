import pyarrow.flight as flight
import pyarrow.compute as pc
import pyarrow as pa
import pandas as pd
import random

from Statisticker import Statisticker
from constants import *

statisticker = Statisticker()
MAIN_FILE = "./tests/queries/queries"
current_method = ""

def start():
    statisticker.start_measuring_time()

def stop(query: str):
    additional_data = f"nasa;{current_method};{query};"
    statisticker.stop_measuring_time_csv(additional_data, MAIN_FILE)



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
    global current_method
    # ------------------------------------------------------------------------------------------------
    port = 50051
    current_method = "PathFlattener"
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    table_name = 'JSONPath_nasa'
    reader = client.do_get(flight.Ticket(table_name.encode()))
    data = reader.read_all()

    # # SELECTION
    # # first level query
    # ## to string
    start()
    print(data.select(['nametype']))
    stop(SELECTION_TO_STR)
    # ## to int
    print("No data")
    # ## object from list 
    # ### low level of nulls - first element of list
    start()
    print(data.select(['geolocation.coordinates[0]']))
    stop(SELECTION_FROM_LIST_LOW_LVL_OF_NULLS)
    # ### medium level of nulls
    start()
    print(data.select(['geolocation.coordinates[1]']))
    stop(SELECTION_FROM_LIST_MEDIUM_LVL_OF_NULLS)
    # ### high level of nulls - last element of list
    print("No data")

    # # # FILTRES
    start()
    print(pc.filter(data, pc.greater(data.column('year'), "2000")))
    stop(FILTRES)

    # # #SORT
    start()
    print(pc.take(data,pc.sort_indices(data, sort_keys=[("nametype", "ascending")])))
    stop(SORT)
    # # #SORT DESC
    start()
    print(pc.take(data,pc.sort_indices(data, sort_keys=[("nametype", "descending")])))
    stop(SORT_DESC)

    # #GROUP BY
    start()
    print(pa.TableGroupBy(data,'year'))
    stop(GROUP_BY_SIMPLE_ATTRIBUTE)
    start()
    print(pa.TableGroupBy(data,'geolocation.coordinates[0]'))
    stop(GROUP_BY_ONE_ATTRIBUTE_FROM_LIST_SIMPLE)
    start()
    print(pa.TableGroupBy(data, ['geolocation.coordinates[0]','geolocation.coordinates[1]']))
    stop(GROUP_BY_TWO_ATTRIBUTES_FROM_LIST)
    print("No data")

    # #AGGREGATE FUNCTION
    start()
    print(pa.TableGroupBy(data,'year').aggregate([("year", "count")]))
    stop(AGGREGATE_FUNCTION_SIMPLE_ATTRIBUTE)
    start()
    print(pa.TableGroupBy(data, 'geolocation.coordinates[0]').aggregate([('geolocation.coordinates[0]', "count")]))
    stop(AGGREGATE_FUNCTION_ONE_FROM_LIST)
    start()
    print(pa.TableGroupBy(data, ['geolocation.coordinates[0]','geolocation.coordinates[1]']).aggregate([('geolocation.coordinates[0]', "count"),('geolocation.coordinates[1]', "count")]))
    stop(AGGREGATE_FUNCTION_TWO_FROM_LIST)

    print("No data")

    # ##CLOUD PAK

    #CONVERT TYPE
    start()
    print(data['geolocation.coordinates[0]'].cast(pa.string()))
    print(pa.Table.from_arrays([data.column('geolocation.coordinates[0]').cast(pa.string())], names=['geolocation.coordinates[0]']))
    stop(CONVERT_TYPE)

    #CONCATENATE COLUMNS
    start()
    title_column = data.column('nametype').cast('string')
    year_column = data.column('year').cast('string')
    concatenated = pc.binary_join_element_wise(title_column, year_column, '-')
    concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(''), concatenated)
    print(data.append_column('title-year', concatenated).select(['title-year']))
    stop(CONCATENATE_COLUMNS)

    # #SPLIT COLUMN
    start()
    titles = data['nametype']
    title_split1 = []
    title_split2 = []
    for title in titles:
        split_title = title.as_py().split(' ', 1)
        title_split1.append(split_title[0])
        title_split2.append(split_title[1] if len(split_title) > 1 else '')
    title_split1_array = pa.array(title_split1)
    title_split2_array = pa.array(title_split2)
    split_data = pa.table([title_split1_array, title_split2_array], names=['title_split1', 'title_split2'])

    print(split_data)
    stop(SPLIT_COLUMN)

    #REPLACE MISSING VALUES
    start()
    data1 = data
    genres1 = data1['mass']
    replaced_genres1 = pc.fill_null(genres1, pa.scalar('No mass'))
    new_table = data1.set_column(data1.schema.get_field_index('mass'), 'mass', replaced_genres1)
    print(new_table)
    stop(REPLACE_MISSING_VALUES)

    # #SELF JOIN
    start()
    data_left = data
    data_right = data

    filter_mask = pc.is_in(data_left['nametype'], data_right['nametype'])
    filtered_data_left = data_left.filter(filter_mask)
    joined_data = pa.concat_tables([filtered_data_left, data_right])
    print(joined_data)
    stop(SELF_JOIN)

    # #UNION TABLES
    start()
    data_left = data
    data_right = data
    unioned_data = pa.concat_tables([data_right, data_left])
    print(unioned_data)
    stop(UNION_TABLES)

    #SAMPLE TABLE
    start()

    num_rows = data.num_rows
    n = int(num_rows * (10 / 100.0))
    sample_indices = random.sample(range(num_rows), n)
    sampled_table = data.take(sample_indices)
    print(sampled_table)
    stop(SAMPLE_TABLE)

    # REMOVE COLUMN
    start()
    column_names = data.column_names
    column_names.remove('nametype')
    print(data.select(column_names))
    stop(REMOVE_COLUMN)

    # ------------------------------------------------------------------------------------------------
    port = 50054
    current_method = "JSONFlatten"
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    table_name = 'SimpleMethod_nasa'
    reader = client.do_get(flight.Ticket(table_name.encode()))
    data = reader.read_all()

    # # SELECTION
    # # first level query
    # ## to string
    start()
    print(data.select(['nametype']))
    stop(SELECTION_TO_STR)
    # ## to int
    print("No data")
    # ## object from list 
    # ### low level of nulls - first element of list
    start()
    print(data.select(['geolocation_coordinates_0']))
    stop(SELECTION_FROM_LIST_LOW_LVL_OF_NULLS)
    # ### medium level of nulls
    start()
    print(data.select(['geolocation_coordinates_1']))
    stop(SELECTION_FROM_LIST_MEDIUM_LVL_OF_NULLS)
    # ### high level of nulls - last element of list
    print("No data")

    # # # FILTRES
    start()
    print(pc.filter(data, pc.greater(data.column('year'), "2000")))
    stop(FILTRES)

    # # #SORT
    start()
    print(pc.take(data,pc.sort_indices(data, sort_keys=[("nametype", "ascending")])))
    stop(SORT)
    # # #SORT DESC
    start()
    print(pc.take(data,pc.sort_indices(data, sort_keys=[("nametype", "descending")])))
    stop(SORT_DESC)

    # #GROUP BY
    start()
    print(pa.TableGroupBy(data,'year'))
    stop(GROUP_BY_SIMPLE_ATTRIBUTE)
    start()
    print(pa.TableGroupBy(data,'geolocation_coordinates_0'))
    stop(GROUP_BY_ONE_ATTRIBUTE_FROM_LIST_SIMPLE)
    start()
    print(pa.TableGroupBy(data, ['geolocation_coordinates_0','geolocation_coordinates_1']))
    stop(GROUP_BY_TWO_ATTRIBUTES_FROM_LIST)
    print("No data")

    #AGGREGATE FUNCTION
    start()
    print(pa.TableGroupBy(data,'year').aggregate([("year", "count")]))
    stop(AGGREGATE_FUNCTION_SIMPLE_ATTRIBUTE)
    start()
    print(pa.TableGroupBy(data, 'geolocation_coordinates_0').aggregate([('geolocation_coordinates_0', "count")]))
    stop(AGGREGATE_FUNCTION_ONE_FROM_LIST)
    start()
    print(pa.TableGroupBy(data, ['geolocation_coordinates_0','geolocation_coordinates_1']).aggregate([('geolocation_coordinates_0', "count"),('geolocation_coordinates_1', "count")]))
    stop(AGGREGATE_FUNCTION_TWO_FROM_LIST)
    print("No data")

    # ##CLOUD PAK

    # #CONVERT TYPE
    start()
    print(data['geolocation_coordinates_0'].cast(pa.string()))
    print(pa.Table.from_arrays([data.column('geolocation_coordinates_0').cast(pa.string())], names=['geolocation_coordinates_0']))
    stop(CONVERT_TYPE)

    #CONCATENATE COLUMNS
    start()
    title_column = data.column('nametype').cast('string')
    year_column = data.column('year').cast('string')
    concatenated = pc.binary_join_element_wise(title_column, year_column, '-')
    concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(''), concatenated)
    print(data.append_column('title-year', concatenated).select(['title-year']))
    stop(CONCATENATE_COLUMNS)

    #SPLIT COLUMN
    start()
    titles = data['nametype']
    title_split1 = []
    title_split2 = []
    for title in titles:
        split_title = title.as_py().split(' ', 1)
        title_split1.append(split_title[0])
        title_split2.append(split_title[1] if len(split_title) > 1 else '')
    title_split1_array = pa.array(title_split1)
    title_split2_array = pa.array(title_split2)
    split_data = pa.table([title_split1_array, title_split2_array], names=['title_split1', 'title_split2'])

    print(split_data)
    stop(SPLIT_COLUMN)

    #REPLACE MISSING VALUES
    start()
    data1 = data
    genres1 = data1['mass']
    replaced_genres1 = pc.fill_null(genres1, pa.scalar('No mass'))
    new_table = data1.set_column(data1.schema.get_field_index('mass'), 'mass]', replaced_genres1)
    print(new_table)
    stop(REPLACE_MISSING_VALUES)

    #SELF JOIN
    start()
    data_left = data
    data_right = data

    filter_mask = pc.is_in(data_left['nametype'], data_right['nametype'])
    filtered_data_left = data_left.filter(filter_mask)
    joined_data = pa.concat_tables([filtered_data_left, data_right])
    print(joined_data)
    stop(SELF_JOIN)

    #UNION TABLES
    start()
    data_left = data
    data_right = data
    unioned_data = pa.concat_tables([data_right, data_left])
    print(unioned_data)
    stop(UNION_TABLES)

    # #SAMPLE TABLE
    start()

    num_rows = data.num_rows
    n = int(num_rows * (10 / 100.0))
    sample_indices = random.sample(range(num_rows), n)
    sampled_table = data.take(sample_indices)
    print(sampled_table)
    stop(SAMPLE_TABLE)

    # REMOVE COLUMN
    start()
    column_names = data.column_names
    column_names.remove('nametype')
    print(data.select(column_names))
    stop(REMOVE_COLUMN)

    # ------------------------------------------------------------------------------------------------
    port = 50052
    current_method = "FirstListFlattener"
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    table_name = 'FlattenedFirstJSON_nasa'
    reader = client.do_get(flight.Ticket(table_name.encode()))
    data = reader.read_all()

    print(data)

    # # # SELECTION
    # # # first level query
    # # ## to string
    start()
    print(data.select(['nametype']))
    stop(SELECTION_TO_STR)
    # # ## to int
    print("No data")
    # # ## object from list 
    # # ### low level of nulls - first element of list
    start()
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
    stop(SELECTION_FROM_LIST_LOW_LVL_OF_NULLS)

    # # ### medium level of nulls
    # print(data.select(['geolocation_coordinates_1']))
    start()
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
    stop(SELECTION_FROM_LIST_MEDIUM_LVL_OF_NULLS)
    # # ### high level of nulls - last element of list
    print("No data")

    # # FILTRES
    start()
    print(pc.filter(data, pc.greater(data.column('year'), "2000")))
    stop(FILTRES)

    # # #SORT
    start()
    print(pc.take(data,pc.sort_indices(data, sort_keys=[("nametype", "ascending")])))
    stop(SORT)
    # # #SORT DESC
    start()
    print(pc.take(data,pc.sort_indices(data, sort_keys=[("nametype", "descending")])))
    stop(SORT_DESC)

    # #GROUP BY
    start()
    print(pa.TableGroupBy(data,'year'))
    stop(GROUP_BY_SIMPLE_ATTRIBUTE)

    start()
    genres_column = data['geolocation.coordinates']
    genres_column = genres_column.to_pylist()
    first_elements = []
    for row in genres_column:
        if row: 
            first_elements.append(row[0])
        else:
            first_elements.append(None)
    first_genre_table = pa.Table.from_arrays([first_elements], names=['geolocation.coordinates0'])
    grouped_table = first_genre_table.group_by('geolocation.coordinates0')
    print(grouped_table)
    stop(GROUP_BY_ONE_ATTRIBUTE_FROM_LIST_SIMPLE)


    start()
    genres_column = data['geolocation.coordinates']
    genres_column = genres_column.to_pylist()
    genres_0 = []
    for row in genres_column:
        if row: 
            genres_0.append(row[0])
        else:
            genres_0.append(None)
    genres_1=[]
    for row in genres_column:
        if row and len(row) > 1: 
            genres_1.append(row[0])
        else:
            genres_1.append(None)
    group_table = pa.Table.from_arrays([genres_0, genres_1], names=['geolocation.coordinates0', 'geolocation.coordinates1'])
    grouped_table = group_table.group_by(['geolocation.coordinates0', 'geolocation.coordinates1'])
    print(grouped_table)
    stop(GROUP_BY_TWO_ATTRIBUTES_FROM_LIST)

    print("No data")

    #AGGREGATE FUNCTION
    start()
    print(pa.TableGroupBy(data,'year').aggregate([("year", "count")]))
    stop(AGGREGATE_FUNCTION_SIMPLE_ATTRIBUTE)

    start()
    genres_column = data['geolocation.coordinates']
    genres_column = genres_column.to_pylist()
    first_genres = []
    for row in genres_column:
        if row: 
            first_genres.append(row[0])
        else:
            first_genres.append(None)
    first_genre_table = pa.Table.from_arrays([first_genres], names=['geolocation.coordinates0'])
    grouped_table = first_genre_table.group_by('geolocation.coordinates0')
    print(grouped_table.aggregate([('geolocation.coordinates0', 'count')]))
    stop(AGGREGATE_FUNCTION_ONE_FROM_LIST)

    start()
    genres_column = data['geolocation.coordinates']
    genres_column = genres_column.to_pylist()
    genres_0 = []
    for row in genres_column:
        if row: 
            genres_0.append(row[0])
        else:
            genres_0.append(None)
    genres_1=[]
    for row in genres_column:
        if row and len(row) > 1: 
            genres_1.append(row[0])
        else:
            genres_1.append(None)
    group_table = pa.Table.from_arrays([genres_0, genres_1], names=['geolocation.coordinates0', 'geolocation.coordinates1'])
    grouped_table = group_table.group_by(['geolocation.coordinates0', 'geolocation.coordinates1'])
    print(grouped_table.aggregate([('geolocation.coordinates0', "count"),('geolocation.coordinates1', "count")]))
    stop(AGGREGATE_FUNCTION_TWO_FROM_LIST)

    print("No data")

    # # ##CLOUD PAK

    # #CONVERT TYPE
    start()
    print(first_genre_table['geolocation.coordinates0'].cast(pa.string()))
    print(pa.Table.from_arrays([first_genre_table.column('geolocation.coordinates0').cast(pa.string())], names=['geolocation.coordinates0']))
    stop(CONVERT_TYPE)

    #CONCATENATE COLUMNS
    start()
    title_column = data.column('nametype').cast('string')
    year_column = data.column('year').cast('string')
    concatenated = pc.binary_join_element_wise(title_column, year_column, '-')
    concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(''), concatenated)
    print(data.append_column('title-year', concatenated).select(['title-year']))
    stop(CONCATENATE_COLUMNS)

    #SPLIT COLUMN
    start()
    titles = data['nametype']
    title_split1 = []
    title_split2 = []
    for title in titles:
        split_title = title.as_py().split(' ', 1)
        title_split1.append(split_title[0])
        title_split2.append(split_title[1] if len(split_title) > 1 else '')
    title_split1_array = pa.array(title_split1)
    title_split2_array = pa.array(title_split2)
    split_data = pa.table([title_split1_array, title_split2_array], names=['title_split1', 'title_split2'])

    print(split_data)
    stop(SPLIT_COLUMN)

    #REPLACE MISSING VALUES
    start()
    data1 = data
    genres1 = data1['mass']
    replaced_genres1 = pc.fill_null(genres1, pa.scalar('No mass'))
    new_table = data1.set_column(data1.schema.get_field_index('mass'), 'mass]', replaced_genres1)
    print(new_table)
    stop(REPLACE_MISSING_VALUES)

    #SELF JOIN
    start()
    data_left = data
    data_right = data

    filter_mask = pc.is_in(data_left['nametype'], data_right['nametype'])
    filtered_data_left = data_left.filter(filter_mask)
    joined_data = pa.concat_tables([filtered_data_left, data_right])
    print(joined_data)
    stop(SELF_JOIN)

    #UNION TABLES
    start()
    data_left = data
    data_right = data
    unioned_data = pa.concat_tables([data_right, data_left])
    print(unioned_data)
    stop(UNION_TABLES)

    # #SAMPLE TABLE
    start()

    num_rows = data.num_rows
    n = int(num_rows * (10 / 100.0))
    sample_indices = random.sample(range(num_rows), n)
    sampled_table = data.take(sample_indices)
    print(sampled_table)
    stop(SAMPLE_TABLE)

    # REMOVE COLUMN
    start()
    column_names = data.column_names
    column_names.remove('nametype')
    print(data.select(column_names))
    stop(REMOVE_COLUMN)

    # ------------------------------------------------------------------------------------------------
    port = 50053
    current_method = "ListToTableConverter"
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    main_table_name = 'TablesMethod_nasa'
    coordinates_table_name = 'TablesMethod_nasa_geolocation.coordinates'

    reader = client.do_get(flight.Ticket(main_table_name.encode()))
    main_data = reader.read_all()

    reader = client.do_get(flight.Ticket(coordinates_table_name.encode()))
    coordinates_data = reader.read_all()

    # SELECTION
    # first level query
    # to string
    start()
    print(main_data.select(['nametype']))
    stop(SELECTION_TO_STR)
    # ## to int
    print("No data")
    # # ## object from list 
    # ### low level of nulls - first element of list
    start()

    cast_row_number = coordinates_data["row_number"]
    split_values = [
        v.split(", ") if isinstance(v, str) and v and v != "null" else []
        for v in coordinates_data["value"].to_pylist()
    ]

    aggregated_values = {}
    for row_number, value_list in zip(cast_row_number.to_pylist(), split_values):
        if row_number not in aggregated_values:
            aggregated_values[row_number] = value_list
        else:
            aggregated_values[row_number].extend(value_list)

    first_values = [
        value_list[0] if value_list else None
        for value_list in aggregated_values.values()
    ]

    first_values_array = pa.array(first_values)
    result_table = pa.Table.from_arrays(
        [pa.array(list(aggregated_values.keys())), first_values_array],
        names=["row_number", "first_value"],
    )
    print(result_table)
    stop(SELECTION_FROM_LIST_LOW_LVL_OF_NULLS)

    # # ### medium level of nulls
    start()
    cast_row_number = coordinates_data["row_number"]
    split_values = [
        v.split(", ") if isinstance(v, str) and v and v != "null" else []
        for v in coordinates_data["value"].to_pylist()
    ]
    aggregated_values = {}
    for row_number, value_list in zip(cast_row_number.to_pylist(), split_values):
        if row_number not in aggregated_values:
            aggregated_values[row_number] = value_list
        else:
            aggregated_values[row_number].extend(value_list)

    tenth_values = [
        value_list[1] if len(value_list) >= 1 else None
        for value_list in aggregated_values.values()
    ]
    tenth_values_array = pa.array(tenth_values)
    result_table = pa.Table.from_arrays(
        [pa.array(list(aggregated_values.keys())), tenth_values_array],
        names=["row_number", "first_value"],
    )
    print(result_table)
    stop(SELECTION_FROM_LIST_MEDIUM_LVL_OF_NULLS)
    # ### high level of nulls - last element of list
    print("No data")

    # # # FILTRES
    start()
    print(pc.filter(main_data, pc.greater(main_data.column('year'), "2000")))
    stop(FILTRES)

    # #SORT
    start()
    print(pc.take(main_data,pc.sort_indices(main_data, sort_keys=[("nametype", "ascending")])))
    stop(SORT)
    # #SORT DESC
    start()
    print(pc.take(main_data,pc.sort_indices(main_data, sort_keys=[("nametype", "descending")])))
    stop(SORT_DESC)

    # #GROUP BY
    start()
    print(pa.TableGroupBy(main_data,'year'))
    stop(GROUP_BY_SIMPLE_ATTRIBUTE)

    start()
    genres_row_number = coordinates_data['row_number']
    split_values = [
        str(v).split(', ') if v and v != "null" else [] 
        for v in coordinates_data['value'].to_pylist()
    ]

    aggregated_values = {}
    for row_number, value_list in zip(genres_row_number.to_pylist(), split_values):
        if row_number not in aggregated_values:
            aggregated_values[row_number] = value_list
        else:
            aggregated_values[row_number].extend(value_list)

    first_values = [value_list[0] if value_list else None for value_list in aggregated_values.values()]
    first_values_array = pa.array(first_values)
    result_table = pa.Table.from_arrays([pa.array(list(aggregated_values.keys())), first_values_array], names=['row_number', 'first_value'])

    grouped_table = result_table.group_by('first_value')
    print(grouped_table)
    stop(GROUP_BY_ONE_ATTRIBUTE_FROM_LIST_SIMPLE)

    start()
    genres_row_number = coordinates_data['row_number']
    split_values = [str(v).split(', ') if v and v != "null" else [] 
        for v in coordinates_data['value'].to_pylist()]
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
    stop(GROUP_BY_TWO_ATTRIBUTES_FROM_LIST)

    print("No data")

    # # #AGGREGATE FUNCTION
    start()
    print(pa.TableGroupBy(main_data,'year').aggregate([("year", "count")]))
    stop(AGGREGATE_FUNCTION_SIMPLE_ATTRIBUTE)

    start()
    genres_row_number=coordinates_data['row_number']
    split_values = [str(v).split(', ') if v and v != "null" else [] 
        for v in coordinates_data['value'].to_pylist()]
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
    stop(AGGREGATE_FUNCTION_ONE_FROM_LIST)

    start()
    genres_row_number = coordinates_data['row_number']
    split_values = [str(v).split(', ') if v and v != "null" else [] 
        for v in coordinates_data['value'].to_pylist()]
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
    stop(AGGREGATE_FUNCTION_TWO_FROM_LIST)

    start()
    unique_row_numbers = pc.unique(coordinates_data['row_number'])
    grouped_data = {'row_number': [], 'value': []}
    for row_num in unique_row_numbers:
        mask = pc.equal(coordinates_data['row_number'], row_num)
        filtered_values = coordinates_data.filter(mask)['value'].to_pylist()
        grouped_data['row_number'].append(row_num.as_py())
        grouped_data['value'].append([val for val in filtered_values if val is not None])
    grouped_table = pa.table(grouped_data)

    genres_as_string = grouped_table['value'].combine_chunks().cast(pa.list_(pa.string())).flatten().cast(pa.string()).cast(pa.utf8())
    data_with_strings = pa.table({'genres': genres_as_string})
    grouped = data_with_strings.group_by('genres').aggregate([('genres',"count")])
    print(grouped)
    stop(AGGREGATE_FUNCTION_ALL_ATTRIBUTES_FROM_LIST)

    ##CLOUD PAK

    # #CONVERT TYPE
    start()
    print(coordinates_data['value'].cast(pa.string()))
    print(pa.Table.from_arrays([coordinates_data.column('value').cast(pa.string())], names=['value']))
    stop(CONVERT_TYPE)

    #CONCATENATE COLUMNS
    start()
    title_column = main_data.column('nametype').cast('string')
    year_column = main_data.column('year').cast('string')
    concatenated = pc.binary_join_element_wise(title_column, year_column, '-')
    concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(''), concatenated)
    print(main_data.append_column('title-year', concatenated).select(['title-year']))
    stop(CONCATENATE_COLUMNS)

    #SPLIT COLUMN
    start()
    titles = main_data['nametype']
    title_split1 = []
    title_split2 = []
    for title in titles:
        split_title = title.as_py().split(' ', 1)
        title_split1.append(split_title[0])
        title_split2.append(split_title[1] if len(split_title) > 1 else '')
    title_split1_array = pa.array(title_split1)
    title_split2_array = pa.array(title_split2)
    split_data = pa.table([title_split1_array, title_split2_array], names=['title_split1', 'title_split2'])

    print(split_data)
    stop(SPLIT_COLUMN)

    #REPLACE MISSING VALUES
    start()

    data1 = main_data
    genres1 = data1['mass']
    replaced_genres1 = pc.fill_null(genres1, pa.scalar('No mass'))
    new_table = data1.set_column(data1.schema.get_field_index('mass'), 'mass', replaced_genres1)
    print(new_table)
    stop(REPLACE_MISSING_VALUES)

    # #SELF JOIN
    start()
    data_left = main_data
    data_right = main_data

    filter_mask = pc.is_in(data_left['nametype'], data_right['nametype'])
    filtered_data_left = data_left.filter(filter_mask)
    joined_data = pa.concat_tables([filtered_data_left, data_right])
    print(joined_data)
    stop(SELF_JOIN)

    # #UNION TABLES
    start()
    data_left = main_data
    data_right = main_data
    unioned_data = pa.concat_tables([data_right, data_left])
    print(unioned_data)
    stop(UNION_TABLES)

    #SAMPLE TABLE
    start()

    num_rows = main_data.num_rows
    n = int(num_rows * (10 / 100.0))
    sample_indices = random.sample(range(num_rows), n)
    sampled_table = main_data.take(sample_indices)
    print(sampled_table)
    stop(SAMPLE_TABLE)

    # REMOVE COLUMN
    start()
    column_names = main_data.column_names
    column_names.remove('nametype')
    print(main_data.select(column_names))
    stop(REMOVE_COLUMN)

if __name__ == '__main__':
    random.seed(23)
    client()
    # client_example()