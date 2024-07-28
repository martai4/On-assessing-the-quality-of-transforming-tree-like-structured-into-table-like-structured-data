import pyarrow.flight as flight
import pyarrow.compute as pc
import pyarrow as pa
import pandas as pd
import random
from typing import Callable

from Statisticker import Statisticker
from constants import *

statisticker = Statisticker()
MAIN_PATH = "./tests/queries/movies"
current_method = ""


def start():
    statisticker.start_measuring_time()


def stop(query: str):
    statisticker.stop_measuring_time(f"{MAIN_PATH}-{query}-{current_method}.txt")


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


def client_movies():
    global current_method
    # ------------------------------------------------------------------------------------------------
    port = 50051
    current_method = "PathFlattener"
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    table_name = "JSONPath_movies"
    reader = client.do_get(flight.Ticket(table_name.encode()))
    data = reader.read_all()

    # SELECTION
    # first level query
    ## to string
    start()
    print(data.select(["title"]))
    stop(SELECTION_TO_STR)
    ## to int
    start()
    print(data.select(["year"]))
    stop(SELECTION_TO_INT)
    ## object from list
    ### low level of nulls - first element of list
    start()
    print(data.select(["cast[0]"]))
    stop(SELECTION_FROM_LIST_LOW_LVL_OF_NULLS)
    ### medium level of nulls
    start()
    print(data.select(["cast[9]"]))
    stop(SELECTION_FROM_LIST_MEDIUM_LVL_OF_NULLS)
    ### high level of nulls - last element of list
    start()
    print(data.select(["cast[58]"]))
    stop(SELECTION_FROM_LIST_HIGH_LVL_OF_NULLS)

    # # FILTRES
    start()
    print(pc.filter(data, pc.greater(data.column("year"), 2000)))
    stop(FILTRES)

    # #SORT
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
    print(pa.TableGroupBy(data, "genres[0]"))
    stop(GROUP_BY_ONE_ATTRIBUTE_FROM_LIST_SIMPLE)
    start()
    print(pa.TableGroupBy(data, ["genres[0]", "genres[1]"]))
    stop(GROUP_BY_TWO_ATTRIBUTES_FROM_LIST)
    start()
    print(pa.TableGroupBy(data, "cast[0]"))
    stop(GROUP_BY_ONE_ATTRIBUTE_FROM_LIST_COMPLEX)

    # AGGREGATE FUNCTION
    start()
    print(pa.TableGroupBy(data, "year").aggregate([("year", "count")]))
    stop(AGGREGATE_FUNCTION_SIMPLE_ATTRIBUTE)
    start()
    print(pa.TableGroupBy(data, "genres[0]").aggregate([("genres[0]", "count")]))
    stop(AGGREGATE_FUNCTION_ONE_FROM_LIST)
    start()
    print(
        pa.TableGroupBy(data, ["genres[0]", "genres[1]"]).aggregate(
            [("genres[0]", "count"), ("genres[1]", "count")]
        )
    )
    stop(AGGREGATE_FUNCTION_TWO_FROM_LIST)

    combined_df = []
    for genre in filter(lambda x: ("genre" in x), data.schema.names):
        print(genre)
        df_genre = data.select([genre]).rename_columns(["genre"])
        combined_df.append(df_genre)

    combined_df = pa.concat_tables(combined_df)
    combined_df = combined_df.combine_chunks()
    print(pa.TableGroupBy(combined_df, ["genre"]).aggregate([("genre", "count")]))
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
    genres1 = data1["genres[1]"]
    replaced_genres1 = pc.fill_null(genres1, pa.scalar("No genres1"))
    new_table = data1.set_column(
        data1.schema.get_field_index("genres[1]"), "genres[1]", replaced_genres1
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

    # ------------------------------------------------------------------------------------------------
    port = 50054
    current_method = "JSONFlatten"
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    table_name = "SimpleMethod_movies"
    reader = client.do_get(flight.Ticket(table_name.encode()))
    data = reader.read_all()

    # SELECTION
    # first level query
    ## to string
    start()
    print(data.select(["title"]))
    stop(SELECTION_TO_STR)
    ## to int
    start()
    print(data.select(["year"]))
    stop(SELECTION_TO_INT)
    ## object from list
    ### low level of nulls - first element of list
    start()
    print(data.select(["cast_0"]))
    stop(SELECTION_FROM_LIST_LOW_LVL_OF_NULLS)
    # ### medium level of nulls
    start()
    print(data.select(["cast_9"]))
    stop(SELECTION_FROM_LIST_MEDIUM_LVL_OF_NULLS)
    # ### high level of nulls - last element of list
    start()
    print(data.select(["cast_58"]))
    stop(SELECTION_FROM_LIST_HIGH_LVL_OF_NULLS)

    # # FILTRES
    start()
    print(pc.filter(data, pc.greater(data.column("year"), 2000)))
    stop(FILTRES)

    # #SORT
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
    print(pa.TableGroupBy(data, "genres_0"))
    stop(GROUP_BY_ONE_ATTRIBUTE_FROM_LIST_SIMPLE)
    start()
    print(pa.TableGroupBy(data, ["genres_0", "genres_1"]))
    stop(GROUP_BY_TWO_ATTRIBUTES_FROM_LIST)
    start()
    print(pa.TableGroupBy(data, "cast_0"))
    stop(GROUP_BY_ONE_ATTRIBUTE_FROM_LIST_COMPLEX)

    # AGGREGATE FUNCTION
    start()
    print(pa.TableGroupBy(data, "year").aggregate([("year", "count")]))
    stop(AGGREGATE_FUNCTION_SIMPLE_ATTRIBUTE)
    start()
    print(pa.TableGroupBy(data, "genres_0").aggregate([("genres_0", "count")]))
    stop(AGGREGATE_FUNCTION_ONE_FROM_LIST)
    start()
    print(
        pa.TableGroupBy(data, ["genres_0", "genres_1"]).aggregate(
            [("genres_0", "count"), ("genres_1", "count")]
        )
    )
    stop(AGGREGATE_FUNCTION_TWO_FROM_LIST)

    start()
    combined_df = []
    for genre in filter(lambda x: ("genres_" in x), data.schema.names):
        df_genre = data.select([genre]).rename_columns(["genre"])
        combined_df.append(df_genre)

    combined_df = pa.concat_tables(combined_df)
    combined_df = combined_df.combine_chunks()
    print(pa.TableGroupBy(combined_df, ["genre"]).aggregate([("genre", "count")]))
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
    genres1 = data1["genres_1"]
    replaced_genres1 = pc.fill_null(genres1, pa.scalar("No genres1"))
    new_table = data1.set_column(
        data1.schema.get_field_index("genres_1"), "genres_1]", replaced_genres1
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

    # ------------------------------------------------------------------------------------------------
    port = 50052
    current_method = "FirstListFlattener"
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    table_name = "FlattenedFirstJSON_movies"
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

    # ------------------------------------------------------------------------------------------------
    port = 50053
    current_method = "ListToTableConverter"
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    main_table_name = "TablesMethod_movies"
    cast_table_name = "TablesMethod_movies_cast"
    genres_table_name = "TablesMethod_movies_genres"

    reader = client.do_get(flight.Ticket(main_table_name.encode()))
    main_data = reader.read_all()

    reader = client.do_get(flight.Ticket(cast_table_name.encode()))
    cast_data = reader.read_all()

    reader = client.do_get(flight.Ticket(genres_table_name.encode()))
    genres_data = reader.read_all()

    # SELECTION
    # first level query
    # to string
    start()
    print(main_data.select(["title"]))
    stop(SELECTION_TO_STR)
    ## to int
    start()
    print(main_data.select(["year"]))
    stop(SELECTION_TO_INT)
    # ## object from list
    # ### low level of nulls - first element of list
    start()
    grouped_table = cast_data.group_by("row_number")

    cast_row_number = cast_data["row_number"]
    split_values = [
        v.split(", ") if v and v != "null" else []
        for v in cast_data["value"].to_pylist()
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

    # ### medium level of nulls
    start()
    cast_row_number = cast_data["row_number"]
    split_values = [
        v.split(", ") if v and v != "null" else []
        for v in cast_data["value"].to_pylist()
    ]
    aggregated_values = {}
    for row_number, value_list in zip(cast_row_number.to_pylist(), split_values):
        if row_number not in aggregated_values:
            aggregated_values[row_number] = value_list
        else:
            aggregated_values[row_number].extend(value_list)

    tenth_values = [
        value_list[9] if len(value_list) >= 10 else None
        for value_list in aggregated_values.values()
    ]
    tenth_values_array = pa.array(tenth_values)
    result_table = pa.Table.from_arrays(
        [pa.array(list(aggregated_values.keys())), tenth_values_array],
        names=["row_number", "first_value"],
    )
    print(result_table)
    start(SELECTION_FROM_LIST_MEDIUM_LVL_OF_NULLS)

    # ### high level of nulls - last element of list
    # print(data.select(['cast[58]']))
    start()
    cast_row_number = cast_data["row_number"]
    split_values = [
        v.split(", ") if v and v != "null" else []
        for v in cast_data["value"].to_pylist()
    ]
    aggregated_values = {}
    for row_number, value_list in zip(cast_row_number.to_pylist(), split_values):
        if row_number not in aggregated_values:
            aggregated_values[row_number] = value_list
        else:
            aggregated_values[row_number].extend(value_list)

    tenth_values = [
        value_list[58] if len(value_list) > 58 else None
        for value_list in aggregated_values.values()
    ]
    tenth_values_array = pa.array(tenth_values)
    result_table = pa.Table.from_arrays(
        [pa.array(list(aggregated_values.keys())), tenth_values_array],
        names=["row_number", "first_value"],
    )
    print(result_table)
    stop(SELECTION_FROM_LIST_HIGH_LVL_OF_NULLS)

    # # FILTRES
    start()
    print(pc.filter(main_data, pc.greater(main_data.column("year"), 2000)))
    stop(FILTRES)

    # #SORT
    start()
    print(
        pc.take(
            main_data, pc.sort_indices(main_data, sort_keys=[("title", "ascending")])
        )
    )
    stop(SORT)
    # #SORT DESC
    start()
    print(
        pc.take(
            main_data, pc.sort_indices(main_data, sort_keys=[("title", "descending")])
        )
    )
    stop(SORT_DESC)

    # #GROUP BY
    start()
    print(pa.TableGroupBy(main_data, "year"))
    stop(GROUP_BY_SIMPLE_ATTRIBUTE)

    start()
    genres_row_number = genres_data["row_number"]
    split_values = [
        v.split(", ") if v and v != "null" else []
        for v in genres_data["value"].to_pylist()
    ]
    aggregated_values = {}
    for row_number, value_list in zip(genres_row_number.to_pylist(), split_values):
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
    print(result_table.group_by("first_value"))
    stop(GROUP_BY_ONE_ATTRIBUTE_FROM_LIST_SIMPLE)

    start()
    genres_row_number = genres_data["row_number"]
    split_values = [
        v.split(", ") if v and v != "null" else []
        for v in genres_data["value"].to_pylist()
    ]
    aggregated_values = {}
    for row_number, value_list in zip(genres_row_number.to_pylist(), split_values):
        if row_number not in aggregated_values:
            aggregated_values[row_number] = value_list
        else:
            aggregated_values[row_number].extend(value_list)
    first_values = [
        value_list[0] if value_list else None
        for value_list in aggregated_values.values()
    ]
    second_values = [
        value_list[1] if len(value_list) > 1 else None
        for value_list in aggregated_values.values()
    ]
    first_values_array = pa.array(first_values)
    second_values_array = pa.array(second_values)
    result_table = pa.Table.from_arrays(
        [
            pa.array(list(aggregated_values.keys())),
            first_values_array,
            second_values_array,
        ],
        names=["row_number", "first_value", "second_value"],
    )
    grouped_table = result_table.group_by(["first_value", "second_value"])
    print(grouped_table)
    stop(GROUP_BY_TWO_ATTRIBUTES_FROM_LIST)

    start()
    cast_row_number = cast_data["row_number"]
    split_values = [
        v.split(", ") if v and v != "null" else []
        for v in cast_data["value"].to_pylist()
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
    print(result_table.group_by("first_value"))
    stop(GROUP_BY_ONE_ATTRIBUTE_FROM_LIST_COMPLEX)

    # #AGGREGATE FUNCTION
    start()
    print(pa.TableGroupBy(main_data, "year").aggregate([("year", "count")]))
    stop(AGGREGATE_FUNCTION_SIMPLE_ATTRIBUTE)

    start()
    genres_row_number = genres_data["row_number"]
    split_values = [
        v.split(", ") if v and v != "null" else []
        for v in genres_data["value"].to_pylist()
    ]
    aggregated_values = {}
    for row_number, value_list in zip(genres_row_number.to_pylist(), split_values):
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
    print(result_table.group_by("first_value").aggregate([("first_value", "count")]))
    stop(AGGREGATE_FUNCTION_ONE_FROM_LIST)

    start()
    genres_row_number = genres_data["row_number"]
    split_values = [
        v.split(", ") if v and v != "null" else []
        for v in genres_data["value"].to_pylist()
    ]
    aggregated_values = {}
    for row_number, value_list in zip(genres_row_number.to_pylist(), split_values):
        if row_number not in aggregated_values:
            aggregated_values[row_number] = value_list
        else:
            aggregated_values[row_number].extend(value_list)
    first_values = [
        value_list[0] if value_list else None
        for value_list in aggregated_values.values()
    ]
    second_values = [
        value_list[1] if len(value_list) > 1 else None
        for value_list in aggregated_values.values()
    ]
    first_values_array = pa.array(first_values)
    second_values_array = pa.array(second_values)
    result_table = pa.Table.from_arrays(
        [
            pa.array(list(aggregated_values.keys())),
            first_values_array,
            second_values_array,
        ],
        names=["row_number", "first_value", "second_value"],
    )
    grouped_table = result_table.group_by(["first_value", "second_value"])
    print(
        grouped_table.aggregate([("first_value", "count"), ("second_value", "count")])
    )
    stop(AGGREGATE_FUNCTION_TWO_FROM_LIST)

    start()
    unique_row_numbers = pc.unique(genres_data["row_number"])
    grouped_data = {"row_number": [], "value": []}
    for row_num in unique_row_numbers:
        mask = pc.equal(genres_data["row_number"], row_num)
        filtered_values = genres_data.filter(mask)["value"].to_pylist()
        grouped_data["row_number"].append(row_num.as_py())
        grouped_data["value"].append(
            [val for val in filtered_values if val is not None]
        )
    grouped_table = pa.table(grouped_data)

    genres_as_string = (
        grouped_table["value"]
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
    genres_data_row_number = genres_data["row_number"]
    split_values = [
        v.split(", ") if v and v != "null" else []
        for v in genres_data["value"].to_pylist()
    ]
    aggregated_values = {}
    for row_number, value_list in zip(genres_data_row_number.to_pylist(), split_values):
        if row_number not in aggregated_values:
            aggregated_values[row_number] = value_list
        else:
            aggregated_values[row_number].extend(value_list)

    second_values = [
        value_list[1] if len(value_list) >= 2 else None
        for value_list in aggregated_values.values()
    ]
    second_values = [
        "No genres1" if pc.is_null(value_list) else value_list
        for value_list in second_values
    ]
    second_values_array = pa.array(second_values)
    result_table = pa.Table.from_arrays(
        [pa.array(list(aggregated_values.keys())), second_values_array],
        names=["row_number", "first_value"],
    )
    print(result_table)
    stop(REPLACE_MISSING_VALUES)

    # SELF JOIN
    start()
    data_left = data
    data_with_index = data_left.append_column("index", pa.array(range(len(data))))
    data_right = cast_data

    filter_mask = pc.is_in(data_with_index["index"], data_right["row_number"])
    filtered_data_left = data_with_index.filter(filter_mask)
    data_right = data_right.rename_columns(["index", "value"])
    joined_data = pa.concat_tables([filtered_data_left, data_right], promote=True)
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


if __name__ == "__main__":
    random.seed(23)
    client_movies()
    # client_example()
