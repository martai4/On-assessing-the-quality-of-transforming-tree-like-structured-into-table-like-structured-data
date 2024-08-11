import pyarrow.flight as flight
import pyarrow.compute as pc
import pyarrow as pa
import pandas as pd
import random
from typing import Callable

from Statisticker import Statisticker
from constants import *

statisticker = Statisticker()
MAIN_FILE = "./tests/queries/queries"
# MAIN_PATH = "./tests/queries/movies"
current_method = ""


def start():
    statisticker.start_measuring_time()


def stop(query: str):
    additional_data = f"movies;{current_method};{query};"
    statisticker.stop_measuring_time_csv(additional_data, MAIN_FILE)

def client_example():
    ports = [50050]
    for port in ports:
        client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
        reader = client.do_get(flight.Ticket("get_table_names".encode()))
        table_names = reader.read_all().to_pandas()["table_name"].tolist()

        for table_name in table_names:
            reader = client.do_get(flight.Ticket(table_name.encode()))
            data = reader.read_all()
            print(f"Data from table {port}:: '{table_name}':")
            # print(data)


def client_movies():
    port = 50050
    current_method = "Hierarchical"
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    table_name = "HierarchicalJSON_movies"
    reader = client.do_get(flight.Ticket(table_name.encode()))
    data = reader.read_all()

    # SELECTION
    # first level query
    # to string
    start()
    print(data.select(["title"]))
    stop(SELECTION_TO_STR)
    ## to int
    start()
    print(data.select(["year"]))
    stop(SELECTION_TO_INT)

    # object from list
    ### low level of nulls - first element of list
    start()
    cast_column = data["cast"]
    cast_list = cast_column.to_pylist()
    first_elements = []
    for row in cast_list:
        if row:  # Check if the list is not empty
            first_elements.append(row[0])
        else:
            first_elements.append(None)
    first_elements_column = pa.array(first_elements)
    new_table = data.set_column(
        data.schema.get_field_index("cast"), "cast", first_elements_column
    )
    print(new_table.select(["cast"]))
    stop(SELECTION_FROM_LIST_LOW_LVL_OF_NULLS)

    # # ### medium level of nulls
    start()
    cast_column = data["cast"]
    cast_list = cast_column.to_pylist()
    tenth_elements = []
    for row in cast_list:
        if len(row) >= 10:
            tenth_elements.append(row[9])
        else:
            tenth_elements.append(None)
    tenth_elements_column = pa.array(tenth_elements)
    new_table = data.set_column(
        data.schema.get_field_index("cast"), "cast", tenth_elements_column
    )
    print(new_table.select(["cast"]))
    stop(SELECTION_FROM_LIST_MEDIUM_LVL_OF_NULLS)

    # # ### high level of nulls - last element of list
    start()
    cast_column = data["cast"]
    cast_list = cast_column.to_pylist()
    tenth_elements = []
    for row in cast_list:
        if len(row) >= 59:
            tenth_elements.append(row[58])
        else:
            tenth_elements.append(None)
    tenth_elements_column = pa.array(tenth_elements)
    new_table = data.set_column(
        data.schema.get_field_index("cast"), "cast", tenth_elements_column
    )
    print(new_table.select(["cast"]))
    stop(SELECTION_FROM_LIST_HIGH_LVL_OF_NULLS)

    # # # FILTRES
    start()
    print(pc.filter(data, pc.greater(data.column("year"), 2000)))
    stop(FILTRES)

    # # #SORT
    start()
    print(pc.take(data, pc.sort_indices(data, sort_keys=[("title", "ascending")])))
    stop(SORT)
    # #SORT DESC
    start()
    print(pc.take(data, pc.sort_indices(data, sort_keys=[("title", "descending")])))
    stop(SORT_DESC)

    # GROUP BY
    start()
    print(pa.TableGroupBy(data, "year"))
    stop(GROUP_BY_SIMPLE_ATTRIBUTE)

    start()
    genres_column = data["genres"]
    first_genres = [str(row[0]) if row else None for row in genres_column]
    first_genre_table = pa.Table.from_arrays([first_genres], names=["first_genre"])
    grouped_table = first_genre_table.group_by("first_genre")
    print(grouped_table)
    stop(GROUP_BY_ONE_ATTRIBUTE_FROM_LIST_SIMPLE)

    start()
    genres_column = data["genres"]
    genres_0 = [str(row[0]) if row else None for row in genres_column]
    genres_1 = [str(row[1]) if len(row) > 1 else None for row in genres_column]
    group_table = pa.Table.from_arrays(
        [genres_0, genres_1], names=["genres_0", "genres_1"]
    )
    grouped_table = group_table.group_by(["genres_0", "genres_1"])
    print(grouped_table)
    stop(GROUP_BY_TWO_ATTRIBUTES_FROM_LIST)

    start()
    cast_column = data["cast"]
    first_cast = [str(row[0]) if row else None for row in cast_column]
    first_cast_table = pa.Table.from_arrays([first_cast], names=["first_cast"])
    grouped_table = first_cast_table.group_by("first_cast")
    print(grouped_table)
    stop(GROUP_BY_ONE_ATTRIBUTE_FROM_LIST_COMPLEX)

    # AGGREGATE FUNCTION
    start()
    print(pa.TableGroupBy(data, "year").aggregate([("year", "count")]))
    stop(AGGREGATE_FUNCTION_SIMPLE_ATTRIBUTE)

    start()
    genres_column = data["genres"]
    first_genres = [str(row[0]) if row else None for row in genres_column]
    first_genre_table = pa.Table.from_arrays([first_genres], names=["first_genre"])
    grouped_table = first_genre_table.group_by("first_genre")
    print(grouped_table.aggregate([("first_genre", "count")]))
    stop(AGGREGATE_FUNCTION_ONE_FROM_LIST)

    start()
    genres_0 = [str(row[0]) if row else None for row in genres_column]
    genres_1 = [str(row[1]) if len(row) > 1 else None for row in genres_column]
    group_table = pa.Table.from_arrays(
        [genres_0, genres_1], names=["genres_0", "genres_1"]
    )
    grouped_table = group_table.group_by(["genres_0", "genres_1"])
    print(grouped_table.aggregate([("genres_0", "count"), ("genres_1", "count")]))
    stop(AGGREGATE_FUNCTION_TWO_FROM_LIST)

    start()
    genres_as_string = (
        data["genres"]
        .combine_chunks()
        .cast(pa.list_(pa.string()))
        .flatten()
        .cast(pa.string())
        .cast(pa.utf8())
    )
    data_with_strings = pa.table({"genres": genres_as_string})
    grouped = data_with_strings.group_by("genres").aggregate([("genres", "count")])
    print(grouped)
    stop(AGGREGATE_FUNCTION_ALL_ATTRIBUTES_FROM_LIST)

    ##CLOUD PAK

    # CONVERT TYPE
    # print(data['year'].cast(pa.string()))
    start()
    print(pa.Table.from_arrays([data.column("year").cast(pa.string())], names=["year"]))
    stop(CONVERT_TYPE)

    # CONCATENATE COLUMNS
    start()
    title_column = data.column("title").cast("string")
    year_column = data.column("year").cast("string")
    concatenated = pc.binary_join_element_wise(title_column, year_column, "-")
    concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(""), concatenated)
    print(data.append_column("title-year", concatenated).select(["title-year"]))
    stop(CONCATENATE_COLUMNS)

    # SPLIT COLUMN
    start()
    titles = data["title"]
    title_split1 = []
    title_split2 = []
    for title in titles:
        split_title = title.as_py().split(" ", 1)
        title_split1.append(split_title[0])
        title_split2.append(split_title[1] if len(split_title) > 1 else "")
    title_split1_array = pa.array(title_split1)
    title_split2_array = pa.array(title_split2)
    split_data = pa.table(
        [title_split1_array, title_split2_array], names=["title_split1", "title_split2"]
    )

    print(split_data)
    stop(SPLIT_COLUMN)

    # REPLACE MISSING VALUES
    start()
    data1 = data
    genres1 = data1["genres"].to_pylist()
    replaced_genres1 = []
    for row in genres1:
        if len(row) >= 2:
            replaced_genres1.append(row[1])
        else:
            replaced_genres1.append("No genres1")
    replaced_genres1 = pa.array(tenth_elements)
    new_table = data1.set_column(
        data1.schema.get_field_index("genres"), "genres_1]", replaced_genres1
    )
    print(new_table)
    stop(REPLACE_MISSING_VALUES)

    # SELF JOIN
    start()
    data_left = data
    data_right = data

    filter_mask = pc.is_in(data_left["title"], data_right["title"])
    filtered_data_left = data_left.filter(filter_mask)
    joined_data = pa.concat_tables([filtered_data_left, data_right])
    print(joined_data)
    stop(SELF_JOIN)

    # UNION TABLES
    start()
    data_left = data
    data_right = data
    unioned_data = pa.concat_tables([data_right, data_left])
    print(unioned_data)
    stop(UNION_TABLES)

    # SAMPLE TABLE
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
    column_names.remove("title")
    print(data.select(column_names))
    stop(REMOVE_COLUMN)

def client_airlines():
    port = 50050
    current_method = "Hierarchical"
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    table_name = "HierarchicalJSON_airlines"
    reader = client.do_get(flight.Ticket(table_name.encode()))
    data = reader.read_all()

    name_field = pc.struct_field(data['Airport'], 'Name')
    name_table = pa.table({'Airport.Name': name_field})
    print(name_table)

    # # ## to int
    year_field = pc.struct_field(data['Time'], 'Year')
    year_table = pa.table({'Time.Year': year_field})
    print(year_table)

    # ## object from list 
    # ### low level of nulls - first element of list
    print("No data")
    # ### medium level of nulls
    print("No data")
    # ### high level of nulls - last element of list
    print("No data")

    # # # # FILTRES
    year_field = pc.struct_field(data['Time'], 'Year')
    filter_mask = pc.greater(year_field, 2000)
    filtered_data = pc.filter(data, filter_mask)
    print(filtered_data)

    # # #SORT
    airport_name = pc.struct_field(data['Airport'], 'Name')
    sort_indices = pc.sort_indices(airport_name, sort_keys=[("Name", "ascending")])
    sorted_data = pc.take(data, sort_indices)
    print(sorted_data)

    # # #SORT DESC
    airport_name = pc.struct_field(data['Airport'], 'Name')
    sort_indices = pc.sort_indices(airport_name, sort_keys=[("Name", "descending")])
    sorted_data = pc.take(data, sort_indices)
    print(sorted_data)

    # #GROUP BY
    print("NOT POSSIBLE WITHOUT FLATTENING pa.TableGroupBy(data, 'Time.Year')")

    print("No data")
    print("No data")
    print("No data")

    # # #AGGREGATE FUNCTION
    print('NOT POSSIBLE WITHOUT FLATTENING pa.TableGroupBy(data, "Time.Year").aggregate([("Time.Year", "count")])')

    print("No data")
    print("No data")
    print("No data")

    # # # ##CLOUD PAK

    # # #CONVERT TYPE
    year_field = pc.struct_field(data['Time'], 'Year')
    year_field_as_string = pc.cast(year_field, pa.string())
    new_table = pa.table({'Time.Year': year_field_as_string})
    print(new_table)

    #CONCATENATE COLUMNS
    title_column = pc.struct_field(data['Airport'], 'Name').cast(pa.string())
    year_column = pc.struct_field(data['Time'], 'Year').cast(pa.string())
    concatenated = pc.binary_join_element_wise(title_column, year_column, '-')
    concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(''), concatenated)
    result_table = data.append_column('title-year', concatenated).select(['title-year'])
    print(result_table)

    # #SPLIT COLUMN

    titles = pc.struct_field(data['Airport'], 'Name')
    title_split1 = []
    title_split2 = []
    for title in titles:
        split_title = title.as_py().split(' ', 1)
        title_split1.append(split_title[0])
        title_split2.append(split_title[1] if len(split_title) > 1 else '')
    title_split1_array = pa.array(title_split1, type=pa.string())
    title_split2_array = pa.array(title_split2, type=pa.string())
    split_data = pa.table({
    'title_split1': title_split1_array,
    'title_split2': title_split2_array
    })
    print(split_data)


    # # #REPLACE MISSING VALUES
    print("No data")

    # # #SELF JOIN
    data_left = data
    data_right = data

    name_left = pc.struct_field(data_left['Airport'], 'Name')
    name_right = pc.struct_field(data_right['Airport'], 'Name')
    filter_mask = pc.is_in(name_left, name_right)
    filtered_data_left = data_left.filter(filter_mask)
    joined_data = pa.concat_tables([filtered_data_left, data_right], promote=True)

    print(joined_data)

    # # #UNION TABLES
    data_left = data
    data_right = data
    unioned_data = pa.concat_tables([data_right, data_left])
    print(unioned_data)

    # # #SAMPLE TABLE
    num_rows = data.num_rows
    n = int(num_rows * (10 / 100.0))
    sample_indices = random.sample(range(num_rows), n)
    sampled_table = data.take(sample_indices)
    print(sampled_table)

    # # # REMOVE COLUMN
    print("NOT POSSIBLE WITHOUT CHANGING STRUCTURE column_names = data.column_names column_names.remove('Airport.Name') print(data.select(column_names))")

def client_nasa():
    port = 50050
    current_method = "Hierarchical"
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    table_name = "HierarchicalJSON_nasa"
    reader = client.do_get(flight.Ticket(table_name.encode()))
    data = reader.read_all()

    # # # SELECTION
    # # # first level query
    # # ## to string
    print(data.select(['nametype']))
    # # ## to int
    print("No data")
    # # ## object from list 
    # # ### low level of nulls - first element of list

    year_field = pc.struct_field(data['geolocation'], 'coordinates')
    year_table = pa.table({'geolocation.coordinates': year_field})
    cast_column = year_table['geolocation.coordinates']
    cast_list = cast_column.to_pylist()
    first_elements = []
    for row in cast_list:
        if row:
            first_elements.append(row[0])
        else:
            first_elements.append(None)
    first_elements_column = pa.array(first_elements)
    new_table = year_table.set_column(year_table.schema.get_field_index('geolocation.coordinates'), 'geolocation.coordinates', first_elements_column)
    print(new_table.select(['geolocation.coordinates']))

    # # # ### medium level of nulls
    year_field = pc.struct_field(data['geolocation'], 'coordinates')
    year_table = pa.table({'geolocation.coordinates': year_field})
    cast_column = year_table['geolocation.coordinates']
    cast_list = cast_column.to_pylist()
    tenth_elements = []
    for row in cast_list:
        if row and len(row) >= 2:
            tenth_elements.append(row[1])
        else:
            tenth_elements.append(None)
    tenth_elements_column = pa.array(tenth_elements)
    new_table = year_table.set_column(year_table.schema.get_field_index('geolocation.coordinates'), 'geolocation.coordinates', tenth_elements_column)
    print(new_table.select(['geolocation.coordinates']))

    # # # ### high level of nulls - last element of list
    print("No data")

    # # # FILTRES
    print(pc.filter(data, pc.greater(data.column('year'), "2000")))

    # # # #SORT
    print(pc.take(data, pc.sort_indices(data, sort_keys=[("nametype", "ascending")])))
    # # # #SORT DESC
    print(pc.take(data, pc.sort_indices(data, sort_keys=[("nametype", "descending")])))
        
    # # #GROUP BY
    print(pa.TableGroupBy(data, 'year'))

    year_field = pc.struct_field(data['geolocation'], 'coordinates')
    year_table = pa.table({'geolocation.coordinates': year_field})
    genres_column = year_table['geolocation.coordinates']
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

    year_field = pc.struct_field(data['geolocation'], 'coordinates')
    year_table = pa.table({'geolocation.coordinates': year_field})
    genres_column = year_table['geolocation.coordinates']
    genres_column = genres_column.to_pylist()
    genres_0 = []
    for row in genres_column:
        if row: 
            genres_0.append(row[0])
        else:
            genres_0.append(None)
    genres_1 = []
    for row in genres_column:
        if row and len(row) > 1: 
            genres_1.append(row[1])
        else:
            genres_1.append(None)
    group_table = pa.Table.from_arrays([genres_0, genres_1], names=['geolocation.coordinates0', 'geolocation.coordinates1'])
    grouped_table = group_table.group_by(['geolocation.coordinates0', 'geolocation.coordinates1'])
    print(grouped_table)

    print("No data")

    # #AGGREGATE FUNCTION
    print(pa.TableGroupBy(data, 'year').aggregate([("year", "count")]))

    year_field = pc.struct_field(data['geolocation'], 'coordinates')
    year_table = pa.table({'geolocation.coordinates': year_field})
    genres_column = year_table['geolocation.coordinates']
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

    year_field = pc.struct_field(data['geolocation'], 'coordinates')
    year_table = pa.table({'geolocation.coordinates': year_field})
    genres_column = year_table['geolocation.coordinates']
    genres_column = genres_column.to_pylist()
    genres_0 = []
    for row in genres_column:
        if row: 
            genres_0.append(row[0])
        else:
            genres_0.append(None)
    genres_1 = []
    for row in genres_column:
        if row and len(row) > 1: 
            genres_1.append(row[1])
        else:
            genres_1.append(None)
    group_table = pa.Table.from_arrays([genres_0, genres_1], names=['geolocation.coordinates0', 'geolocation.coordinates1'])
    grouped_table = group_table.group_by(['geolocation.coordinates0', 'geolocation.coordinates1'])
    print(grouped_table.aggregate([('geolocation.coordinates0', "count"), ('geolocation.coordinates1', "count")]))

    print("No data")

    # # # ##CLOUD PAK

    # # #CONVERT TYPE
    print(first_genre_table['geolocation.coordinates0'].cast(pa.string()))
    print(pa.Table.from_arrays([first_genre_table.column('geolocation.coordinates0').cast(pa.string())], names=['geolocation.coordinates0']))

    #CONCATENATE COLUMNS
    title_column = data.column('nametype').cast('string')
    year_column = data.column('year').cast('string')
    concatenated = pc.binary_join_element_wise(title_column, year_column, '-')
    concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(''), concatenated)
    print(data.append_column('title-year', concatenated).select(['title-year']))

    #SPLIT COLUMN
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

    #REPLACE MISSING VALUES
    data1 = data
    genres1 = data1['mass']
    replaced_genres1 = pc.fill_null(genres1, pa.scalar('No mass'))
    new_table = data1.set_column(data1.schema.get_field_index('mass'), 'mass', replaced_genres1)
    print(new_table)

    #SELF JOIN
    data_left = data
    data_right = data

    filter_mask = pc.is_in(data_left['nametype'], data_right['nametype'])
    filtered_data_left = data_left.filter(filter_mask)
    joined_data = pa.concat_tables([filtered_data_left, data_right])
    print(joined_data)

    #UNION TABLES
    data_left = data
    data_right = data
    unioned_data = pa.concat_tables([data_right, data_left])
    print(unioned_data)

    # #SAMPLE TABLE
    num_rows = data.num_rows
    n = int(num_rows * (10 / 100.0))
    sample_indices = random.sample(range(num_rows), n)
    sampled_table = data.take(sample_indices)
    print(sampled_table)

    # REMOVE COLUMN
    column_names = data.column_names
    column_names.remove('nametype')
    print(data.select(column_names))

def client_gists():
    port = 50050
    current_method = "Hierarchical"
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    table_name = "HierarchicalJSON_gists"
    reader = client.do_get(flight.Ticket(table_name.encode()))
    data = reader.read_all()

    # SELECTION
    # first level query
    ## to string
    name_field = pc.struct_field(data['owner'], 'login')
    name_table = pa.table({'owner.login': name_field})
    print(name_table)

    # # ## to int
    name_field = pc.struct_field(data['owner'], 'id')
    name_table = pa.table({'owner.id': name_field})
    print(name_table)

    # ## object from list 
    # ### low level of nulls - first element of list
    print('No data')

    # ### medium level of nulls
    print('No data')

    # ### high level of nulls - last element of list
    print('No data')

    # # # # FILTRES
    year_field = pc.struct_field(data['owner'], 'id')
    filter_mask = pc.greater(year_field, 2000)
    filtered_data = pc.filter(data, filter_mask)
    print(filtered_data)

    # # #SORT
    airport_name = pc.struct_field(data['owner'], 'login')
    sort_indices = pc.sort_indices(airport_name, sort_keys=[("login", "ascending")])
    sorted_data = pc.take(data, sort_indices)
    print(sorted_data)

    # # #SORT DESC
    airport_name = pc.struct_field(data['owner'], 'login')
    sort_indices = pc.sort_indices(airport_name, sort_keys=[("login", "descending")])
    sorted_data = pc.take(data, sort_indices)
    print(sorted_data)
        
    # #GROUP BY
    print("NOT POSSIBLE WITHOUT FLATTENING pa.TableGroupBy(data, 'owner.id')")
    print('No data')
    print('No data')
    print('No data')

    # # #AGGREGATE FUNCTION
    print("NOT POSSIBLE WITHOUT FLATTENING pa.TableGroupBy(data, 'owner.id').aggregate([('owner.id', 'count')]))")
    print('No data')
    print('No data')
    print('No data')

    # ##CLOUD PAK

    # #CONVERT TYPE
    year_field = pc.struct_field(data['owner'], 'id')
    year_field_as_string = pc.cast(year_field, pa.string())
    new_table = pa.table({'owner.id': year_field_as_string})
    print(new_table)

    # #CONCATENATE COLUMNS

    title_column = pc.struct_field(data['owner'], 'login').cast(pa.string())
    year_column = pc.struct_field(data['owner'], 'id').cast(pa.string())
    concatenated = pc.binary_join_element_wise(title_column, year_column, '-')
    concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(''), concatenated)
    result_table = data.append_column('title-year', concatenated).select(['title-year'])
    print(result_table)   

    # #SPLIT COLUMN

    titles = pc.struct_field(data['owner'], 'login')
    title_split1 = []
    title_split2 = []
    for title in titles:
        split_title = title.as_py().split(' ', 1)
        title_split1.append(split_title[0])
        title_split2.append(split_title[1] if len(split_title) > 1 else '')
    title_split1_array = pa.array(title_split1, type=pa.string())
    title_split2_array = pa.array(title_split2, type=pa.string())
    split_data = pa.table({
    'title_split1': title_split1_array,
    'title_split2': title_split2_array
    })
    print(split_data)

    # #REPLACE MISSING VALUES
    data1 = data
    genres1 = data1['description']
    replaced_genres1 = pc.fill_null(genres1, pa.scalar('No description'))
    new_table = data1.set_column(data1.schema.get_field_index('description'), 'description', replaced_genres1)
    print(new_table)

    # #SELF JOIN
    data_left = data
    data_right = data

    name_left = pc.struct_field(data_left['owner'], 'login')
    name_right = pc.struct_field(data_right['owner'], 'login')
    filter_mask = pc.is_in(name_left, name_right)
    filtered_data_left = data_left.filter(filter_mask)
    joined_data = pa.concat_tables([filtered_data_left, data_right], promote=True)

    print(joined_data)

    #UNION TABLES
    data_left = data
    data_right = data
    unioned_data = pa.concat_tables([data_right, data_left])
    print(unioned_data)

    #SAMPLE TABLE
    num_rows = data.num_rows
    n = int(num_rows * (10 / 100.0))
    sample_indices = random.sample(range(num_rows), n)
    sampled_table = data.take(sample_indices)
    print(sampled_table)

    # REMOVE COLUMN
    print("NOT POSSIBLE WITHOUT CHANGING STRUCTURE column_names = data.column_names column_names.remove('owner.login') print(data.select(column_names))")

def client_reddit():
    port = 50050
    current_method = "Hierarchical"
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    table_name = "HierarchicalJSON_reddit"
    reader = client.do_get(flight.Ticket(table_name.encode()))
    data = reader.read_all()

    # SELECTION
    # first level query
    # to string
    # print(data.select(['data.after']))
    name_field = pc.struct_field(data['data'], 'after')
    name_table = pa.table({'data.after': name_field})
    print(name_table)

    # to int
    name_field = pc.struct_field(data['data'], 'dist')
    name_table = pa.table({'data.dist': name_field})
    print(name_table)

    # object from list 
    # low level of nulls - first element of list
    name_field = pc.struct_field(data['data'], 'children')
    name_table = pa.table({'data.children': name_field})
    children_column = name_table['data.children']
    subreddits = []
    for row in children_column:
        list_array = row.as_py()
        if list_array:
            first_child = list_array[0]
            if 'data' in first_child and 'subreddit' in first_child['data']:
                subreddit = first_child['data']['subreddit']
                subreddits.append(subreddit)
    print(subreddits)

    # # medium level of nulls
    name_field = pc.struct_field(data['data'], 'children')
    name_table = pa.table({'data.children': name_field})
    children_column = name_table['data.children']
    subreddits = []
    for row in children_column:
        list_array = row.as_py()
        if list_array:
            first_child = list_array[9]
            if 'data' in first_child and 'subreddit' in first_child['data']:
                subreddit = first_child['data']['subreddit']
                subreddits.append(subreddit)
    print(subreddits)

    # high level of nulls - last element of list
    name_field = pc.struct_field(data['data'], 'children')
    name_table = pa.table({'data.children': name_field})
    children_column = name_table['data.children']
    subreddits = []
    for row in children_column:
        list_array = row.as_py()
        if list_array:
            first_child = list_array[24]
            if 'data' in first_child and 'subreddit' in first_child['data']:
                subreddit = first_child['data']['subreddit']
                subreddits.append(subreddit)
    print(subreddits)

    # FILTRES
    name_field = pc.struct_field(data['data'], 'children')
    name_table = pa.table({'data.children': name_field})
    children_column = name_table['data.children']
    subreddits = []
    for row in children_column:
        list_array = row.as_py()
        if list_array:
            first_child = list_array[0]
            if 'data' in first_child and 'created' in first_child['data']:
                subreddit = first_child['data']['created']
                if subreddit > 2000:
                    subreddits.append(subreddit)
    print(subreddits)

    # # SORT
    airport_name = pc.struct_field(data['data'], 'after')
    sort_indices = pc.sort_indices(airport_name, sort_keys=[("after", "ascending")])
    sorted_data = pc.take(data, sort_indices)
    print(sorted_data)
    
    # # SORT DESC
    airport_name = pc.struct_field(data['data'], 'after')
    sort_indices = pc.sort_indices(airport_name, sort_keys=[("after", "descending")])
    sorted_data = pc.take(data, sort_indices)
    print(sorted_data)
        
    # # GROUP BY
    print("NOT POSSIBLE WITHOUT FLATTENING pa.TableGroupBy(data, 'data.after')")
    print("NOT POSSIBLE WITHOUT FLATTENING")
    print("NOT POSSIBLE WITHOUT FLATTENING")
    print("NOT POSSIBLE WITHOUT FLATTENING")

    # # AGGREGATE FUNCTION
    print("NOT POSSIBLE WITHOUT FLATTENING pa.TableGroupBy(data, 'data.after').aggregate([('data.after', 'count')])")
    print("NOT POSSIBLE WITHOUT FLATTENING")
    print("NOT POSSIBLE WITHOUT FLATTENING")
    print("NOT POSSIBLE WITHOUT FLATTENING")

    # ##CLOUD PAK

    # # CONVERT TYPE
    year_field = pc.struct_field(data['data'], 'dist')
    year_field_as_string = pc.cast(year_field, pa.string())
    new_table = pa.table({'data.dist': year_field_as_string})
    print(new_table)

    # # CONCATENATE COLUMNS

    title_column = pc.struct_field(data['data'], 'after').cast(pa.string())
    year_column = pc.struct_field(data['data'], 'dist').cast(pa.string())
    concatenated = pc.binary_join_element_wise(title_column, year_column, '-')
    concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(''), concatenated)
    result_table = data.append_column('title-year', concatenated).select(['title-year'])
    print(result_table)

    # # SPLIT COLUMN
    titles = pc.struct_field(data['data'], 'after')
    title_split1 = []
    title_split2 = []
    for title in titles:
        split_title = title.as_py().split('_', 1)
        title_split1.append(split_title[0])
        title_split2.append(split_title[1] if len(split_title) > 1 else '')
    title_split1_array = pa.array(title_split1, type=pa.string())
    title_split2_array = pa.array(title_split2, type=pa.string())
    split_data = pa.table({
    'title_split1': title_split1_array,
    'title_split2': title_split2_array
    })
    print(split_data)

    # REPLACE MISSING VALUES
    name_field = pc.struct_field(data['data'], 'children')
    name_table = pa.table({'data.children': name_field})
    children_column = name_table['data.children']
    subreddits = []
    for row in children_column:
        list_array = row.as_py()
        if list_array:
            first_child = list_array[0]
            if 'data' in first_child and 'subreddit' in first_child['data']:
                subreddit = first_child['data']['subreddit']
                subreddits.append(subreddit)
            else:
                subreddits.append(None)
        else:
            subreddits.append(None)
    new_table = pa.Table.from_arrays([subreddits], names=['subreddit'])
    genres1 = new_table['subreddit']
    replaced_genres1 = pc.fill_null(genres1, pa.scalar('No subreddit'))
    new_table = new_table.set_column(new_table.schema.get_field_index('subreddit'), 'subreddit', replaced_genres1)
    print(new_table)

    # #SELF JOIN
    data_left = data
    data_right = data

    name_left = pc.struct_field(data_left['data'], 'after')
    name_right = pc.struct_field(data_right['data'], 'after')
    filter_mask = pc.is_in(name_left, name_right)
    filtered_data_left = data_left.filter(filter_mask)
    joined_data = pa.concat_tables([filtered_data_left, data_right], promote=True)

    print(joined_data)

    # UNION TABLES
    data_left = data
    data_right = data
    unioned_data = pa.concat_tables([data_right, data_left])
    print(unioned_data)

    # SAMPLE TABLE
    print('No data')

    # # REMOVE COLUMN
    print("NOT POSSIBLE WITHOUT CHANGING STRUCTURE column_names = data.column_names column_names.remove('data.after') print(data.select(column_names))")



if __name__ == "__main__":
    random.seed(23)
    # client_movies()
    # client_airlines()
    # client_nasa()
    # client_gists()
    client_reddit()

    # client_example()