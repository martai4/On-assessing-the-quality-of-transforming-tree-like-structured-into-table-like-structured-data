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
    additional_data = f"gists;{current_method};{query};"
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

def client_gists():
    global current_method
    # ------------------------------------------------------------------------------------------------
    port = 50051
    current_method = "PathFlattener"
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    table_name = 'JSONPath_gists'
    reader = client.do_get(flight.Ticket(table_name.encode()))
    data = reader.read_all()

    # SELECTION
    # first level query
    ## to string
    start()
    print(data.select(['owner.login']))
    stop(SELECTION_TO_STR)
    # ## to int
    start()
    print(data.select(['owner.id']))
    stop(SELECTION_TO_INT)
    # ## object from list 
    # ### low level of nulls - first element of list
    print('No data')
    # ### medium level of nulls
    print('No data')
    # ### high level of nulls - last element of list
    print('No data')

    # # # FILTRES
    start()
    print(pc.filter(data, pc.greater(data.column('owner.id'), 2000)))
    stop(FILTRES)

    # #SORT
    start()
    print(pc.take(data,pc.sort_indices(data, sort_keys=[("owner.login", "ascending")])))
    stop(SORT)
    # #SORT DESC
    start()
    print(pc.take(data,pc.sort_indices(data, sort_keys=[("owner.login", "descending")])))
    stop(SORT_DESC)

    #GROUP BY
    start()
    print(pa.TableGroupBy(data,'owner.id'))
    stop(GROUP_BY_SIMPLE_ATTRIBUTE)
    print('No data')
    print('No data')
    print('No data')

    # #AGGREGATE FUNCTION
    start()
    print(pa.TableGroupBy(data,'owner.id').aggregate([("owner.id", "count")]))
    stop(AGGREGATE_FUNCTION_SIMPLE_ATTRIBUTE)
    print('No data')
    print('No data')
    print('No data')

    ##CLOUD PAK

    #CONVERT TYPE
    start()
    print(data['owner.id'].cast(pa.string()))
    print(pa.Table.from_arrays([data.column('owner.id').cast(pa.string())], names=['owner.id']))
    stop(CONVERT_TYPE)

    #CONCATENATE COLUMNS
    start()
    title_column = data.column('owner.login').cast('string')
    year_column = data.column('owner.id').cast('string')
    concatenated = pc.binary_join_element_wise(title_column, year_column, '-')
    concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(''), concatenated)
    print(data.append_column('title-year', concatenated).select(['title-year']))
    stop(CONCATENATE_COLUMNS)

    #SPLIT COLUMN
    start()
    titles = data['owner.login']
    title_split1 = []
    title_split2 = []
    for title in titles:
        split_title = title.as_py().split('-', 1)
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
    genres1 = data1['description']
    replaced_genres1 = pc.fill_null(genres1, pa.scalar('No description'))
    new_table = data1.set_column(data1.schema.get_field_index('description'), 'description', replaced_genres1)
    print(new_table)
    stop(REPLACE_MISSING_VALUES)

    #SELF JOIN
    start()
    data_left = data
    data_right = data

    filter_mask = pc.is_in(data_left['owner.login'], data_right['owner.login'])
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
    column_names.remove('owner.login')
    print(data.select(column_names))
    stop(REMOVE_COLUMN)

    # ------------------------------------------------------------------------------------------------
    port = 50054
    current_method = "JSONFlatten"
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    table_name = 'SimpleMethod_gists'
    reader = client.do_get(flight.Ticket(table_name.encode()))
    data = reader.read_all()

    # SELECTION
    # first level query
    ## to string
    start()
    print(data.select(['owner_login']))
    stop(SELECTION_TO_STR)
    # # ## to int
    start()
    print(data.select(['owner_id']))
    stop(SELECTION_TO_INT)
    # ## object from list 
    # ### low level of nulls - first element of list
    print('No data')
    # ### medium level of nulls
    print('No data')
    # ### high level of nulls - last element of list
    print('No data')

    # # # FILTRES
    start()
    print(pc.filter(data, pc.greater(data.column('owner_id'), 2000)))
    stop(FILTRES)

    # #SORT
    start()
    print(pc.take(data,pc.sort_indices(data, sort_keys=[("owner_login", "ascending")])))
    stop(SORT)
    # #SORT DESC
    start()
    print(pc.take(data,pc.sort_indices(data, sort_keys=[("owner_login", "descending")])))
    stop(SORT_DESC)

    #GROUP BY
    start()
    print(pa.TableGroupBy(data,'owner_id'))
    stop(GROUP_BY_SIMPLE_ATTRIBUTE)
    print('No data')
    print('No data')
    print('No data')

    # #AGGREGATE FUNCTION
    start()
    print(pa.TableGroupBy(data,'owner_id').aggregate([("owner_id", "count")]))
    stop(AGGREGATE_FUNCTION_SIMPLE_ATTRIBUTE)
    print('No data')
    print('No data')
    print('No data')


    ##CLOUD PAK

    #CONVERT TYPE
    start()
    print(data['owner_id'].cast(pa.string()))
    print(pa.Table.from_arrays([data.column('owner_id').cast(pa.string())], names=['owner_id']))
    stop(CONVERT_TYPE)

    #CONCATENATE COLUMNS
    start()
    title_column = data.column('owner_login').cast('string')
    year_column = data.column('owner_id').cast('string')
    concatenated = pc.binary_join_element_wise(title_column, year_column, '-')
    concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(''), concatenated)
    print(data.append_column('title-year', concatenated).select(['title-year']))
    stop(CONCATENATE_COLUMNS)

    #SPLIT COLUMN
    start()
    titles = data['owner_login']
    title_split1 = []
    title_split2 = []
    for title in titles:
        split_title = title.as_py().split('-', 1)
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
    genres1 = data1['description']
    replaced_genres1 = pc.fill_null(genres1, pa.scalar('No description'))
    new_table = data1.set_column(data1.schema.get_field_index('description'), 'description', replaced_genres1)
    print(new_table)
    stop(REPLACE_MISSING_VALUES)

    #SELF JOIN
    start()
    data_left = data
    data_right = data

    filter_mask = pc.is_in(data_left['owner_login'], data_right['owner_login'])
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
    column_names.remove('owner_login')
    print(data.select(column_names))
    stop(REMOVE_COLUMN)

    # ------------------------------------------------------------------------------------------------
    port = 50052
    current_method = "FirstListFlattener"
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    table_name = 'FlattenedFirstJSON_gists'
    reader = client.do_get(flight.Ticket(table_name.encode()))
    data = reader.read_all()

    # SELECTION
    # first level query
    ## to string
    start()
    print(data.select(['owner.login']))
    stop(SELECTION_TO_STR)
    # ## to int
    start()
    print(data.select(['owner.id']))
    stop(SELECTION_TO_INT)
    # ## object from list 
    # ### low level of nulls - first element of list
    print('No data')
    # ### medium level of nulls
    print('No data')
    # ### high level of nulls - last element of list
    print('No data')


    # # # FILTRES
    start()
    print(pc.filter(data, pc.greater(data.column('owner.id'), 2000)))
    stop(FILTRES)

    # #SORT
    start()
    print(pc.take(data,pc.sort_indices(data, sort_keys=[("owner.login", "ascending")])))
    stop(SORT)
    # #SORT DESC
    start()
    print(pc.take(data,pc.sort_indices(data, sort_keys=[("owner.login", "descending")])))
    stop(SORT_DESC)

    #GROUP BY
    print(pa.TableGroupBy(data,'owner.id'))
    stop(GROUP_BY_SIMPLE_ATTRIBUTE)
    print('No data')
    print('No data')
    print('No data')

    # #AGGREGATE FUNCTION
    print(pa.TableGroupBy(data,'owner.id').aggregate([("owner.id", "count")]))
    stop(AGGREGATE_FUNCTION_SIMPLE_ATTRIBUTE)
    print('No data')
    print('No data')
    print('No data')



    ##CLOUD PAK

    #CONVERT TYPE
    start()
    print(data['owner.id'].cast(pa.string()))
    print(pa.Table.from_arrays([data.column('owner.id').cast(pa.string())], names=['owner.id']))
    stop(CONVERT_TYPE)

    #CONCATENATE COLUMNS
    start()
    title_column = data.column('owner.login').cast('string')
    year_column = data.column('owner.id').cast('string')
    concatenated = pc.binary_join_element_wise(title_column, year_column, '-')
    concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(''), concatenated)
    print(data.append_column('title-year', concatenated).select(['title-year']))
    stop(CONCATENATE_COLUMNS)

    #SPLIT COLUMN
    start()
    titles = data['owner.login']
    title_split1 = []
    title_split2 = []
    for title in titles:
        split_title = title.as_py().split('-', 1)
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
    genres1 = data1['description']
    replaced_genres1 = pc.fill_null(genres1, pa.scalar('No description'))
    new_table = data1.set_column(data1.schema.get_field_index('description'), 'description', replaced_genres1)
    print(new_table)
    stop(REPLACE_MISSING_VALUES)

    #SELF JOIN
    start()
    data_left = data
    data_right = data

    filter_mask = pc.is_in(data_left['owner.login'], data_right['owner.login'])
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
    column_names.remove('owner.login')
    print(data.select(column_names))
    stop(REMOVE_COLUMN)

    # ------------------------------------------------------------------------------------------------
    port = 50053
    current_method = "ListToTableConverter"
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    main_table_name = 'TablesMethod_gists'

    reader = client.do_get(flight.Ticket(main_table_name.encode()))
    data = reader.read_all()

    # SELECTION
    # first level query
    ## to string
    start()
    print(data.select(['owner.login']))
    stop(SELECTION_TO_STR)
    # ## to int
    start()
    print(data.select(['owner.id']))
    stop(SELECTION_TO_INT)
    # ## object from list 
    # ### low level of nulls - first element of list
    print('No data')
    # ### medium level of nulls
    print('No data')
    # ### high level of nulls - last element of list
    print('No data')

    # # # FILTRES
    start()
    print(pc.filter(data, pc.greater(data.column('owner.id'), 2000)))
    stop(FILTRES)

    # #SORT
    start()
    print(pc.take(data,pc.sort_indices(data, sort_keys=[("owner.login", "ascending")])))
    stop(SORT)
    # #SORT DESC
    start()
    print(pc.take(data,pc.sort_indices(data, sort_keys=[("owner.login", "descending")])))
    stop(SORT_DESC)

    #GROUP BY
    start()
    print(pa.TableGroupBy(data,'owner.id'))
    stop(GROUP_BY_SIMPLE_ATTRIBUTE)
    print('No data')
    print('No data')
    print('No data')

    # #AGGREGATE FUNCTION
    start()
    print(pa.TableGroupBy(data,'owner.id').aggregate([("owner.id", "count")]))
    stop(AGGREGATE_FUNCTION_SIMPLE_ATTRIBUTE)
    print('No data')
    print('No data')
    print('No data')

    ##CLOUD PAK

    #CONVERT TYPE
    start()
    print(data['owner.id'].cast(pa.string()))
    print(pa.Table.from_arrays([data.column('owner.id').cast(pa.string())], names=['owner.id']))
    stop(CONVERT_TYPE)

    #CONCATENATE COLUMNS
    start()
    title_column = data.column('owner.login').cast('string')
    year_column = data.column('owner.id').cast('string')
    concatenated = pc.binary_join_element_wise(title_column, year_column, '-')
    concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(''), concatenated)
    print(data.append_column('title-year', concatenated).select(['title-year']))
    stop(CONCATENATE_COLUMNS)

    #SPLIT COLUMN
    start()
    titles = data['owner.login']
    title_split1 = []
    title_split2 = []
    for title in titles:
        split_title = title.as_py().split('-', 1)
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
    genres1 = data1['description']
    replaced_genres1 = pc.fill_null(genres1, pa.scalar('No description'))
    new_table = data1.set_column(data1.schema.get_field_index('description'), 'description', replaced_genres1)
    print(new_table)
    stop(REPLACE_MISSING_VALUES)

    #SELF JOIN
    start()
    data_left = data
    data_right = data

    filter_mask = pc.is_in(data_left['owner.login'], data_right['owner.login'])
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
    column_names.remove('owner.login')
    print(data.select(column_names))
    stop(REMOVE_COLUMN)

if __name__ == '__main__':
    random.seed(23)
    [ client_gists() for _ in range(12)]
    # client_example()
