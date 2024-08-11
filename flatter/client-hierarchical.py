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

    # print(data.select(['Airport.Name']))
    name_field = pc.struct_field(data['Airport'], 'Name')
    name_table = pa.table({'Airport.Name': name_field})
    print(name_table)

    # # ## to int
    # print(data.select(['Time.Year']))
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

def client_gists():
    port = 50050
    current_method = "Hierarchical"
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    table_name = "HierarchicalJSON_gists"
    reader = client.do_get(flight.Ticket(table_name.encode()))
    data = reader.read_all()

def client_reddit():
    port = 50050
    current_method = "Hierarchical"
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    table_name = "HierarchicalJSON_reddit"
    reader = client.do_get(flight.Ticket(table_name.encode()))
    data = reader.read_all()

if __name__ == "__main__":
    random.seed(23)
    # client_movies()
    client_airlines()
    # client_nasa()
    # client_gists()
    # client_reddit()
    # client_example()