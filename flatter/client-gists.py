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

def client_gists():
    # port = 50051
    # client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    # table_name = 'JSONPath_gists'
    # reader = client.do_get(flight.Ticket(table_name.encode()))
    # data = reader.read_all()

    # # SELECTION
    # # first level query
    # ## to string
    # print(data.select(['owner.login']))
    # # ## to int
    # print(data.select(['owner.id']))
    # # ## object from list 
    # # ### low level of nulls - first element of list
    # print('No data')
    # # ### medium level of nulls
    # print('No data')
    # # ### high level of nulls - last element of list
    # print('No data')

    # # # # FILTRES
    # print(pc.filter(data, pc.greater(data.column('owner.id'), 2000)))

    # # #SORT
    # print(pc.take(data,pc.sort_indices(data, sort_keys=[("owner.login", "ascending")])))
    # # #SORT DESC
    # print(pc.take(data,pc.sort_indices(data, sort_keys=[("owner.login", "descending")])))

    # #GROUP BY
    # print(pa.TableGroupBy(data,'owner.id'))
    # print('No data')
    # print('No data')
    # print('No data')

    # # #AGGREGATE FUNCTION
    # print(pa.TableGroupBy(data,'owner.id').aggregate([("owner.id", "count")]))
    # print('No data')
    # print('No data')
    # print('No data')

    # ##CLOUD PAK

    # #CONVERT TYPE
    # print(data['owner.id'].cast(pa.string()))
    # print(pa.Table.from_arrays([data.column('owner.id').cast(pa.string())], names=['owner.id']))

    # #CONCATENATE COLUMNS
    # title_column = data.column('owner.login').cast('string')
    # year_column = data.column('owner.id').cast('string')
    # concatenated = pc.binary_join_element_wise(title_column, year_column, '-')
    # concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(''), concatenated)
    # print(data.append_column('title-year', concatenated).select(['title-year']))

    # #SPLIT COLUMN
    # titles = data['owner.login']
    # title_split1 = []
    # title_split2 = []
    # for title in titles:
    #     split_title = title.as_py().split('-', 1)
    #     title_split1.append(split_title[0])
    #     title_split2.append(split_title[1] if len(split_title) > 1 else '')
    # title_split1_array = pa.array(title_split1)
    # title_split2_array = pa.array(title_split2)
    # split_data = pa.table([title_split1_array, title_split2_array], names=['title_split1', 'title_split2'])

    # print(split_data)

    # #REPLACE MISSING VALUES
    # data1 = data
    # genres1 = data1['description']
    # replaced_genres1 = pc.fill_null(genres1, pa.scalar('No description'))
    # new_table = data1.set_column(data1.schema.get_field_index('description'), 'description', replaced_genres1)
    # print(new_table)

    # #SELF JOIN
    # data_left = data
    # data_right = data

    # filter_mask = pc.is_in(data_left['owner.login'], data_right['owner.login'])
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
    # column_names.remove('owner.login')
    # print(data.select(column_names))


    # port = 50054
    # client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    # table_name = 'SimpleMethod_gists'
    # reader = client.do_get(flight.Ticket(table_name.encode()))
    # data = reader.read_all()

    # # SELECTION
    # # first level query
    # ## to string
    # print(data.select(['owner_login']))
    # # # ## to int
    # print(data.select(['owner_id']))
    # # ## object from list 
    # # ### low level of nulls - first element of list
    # print('No data')
    # # ### medium level of nulls
    # print('No data')
    # # ### high level of nulls - last element of list
    # print('No data')

    # # # # FILTRES
    # print(pc.filter(data, pc.greater(data.column('owner_id'), 2000)))

    # # #SORT
    # print(pc.take(data,pc.sort_indices(data, sort_keys=[("owner_login", "ascending")])))
    # # #SORT DESC
    # print(pc.take(data,pc.sort_indices(data, sort_keys=[("owner_login", "descending")])))

    # #GROUP BY
    # print(pa.TableGroupBy(data,'owner_id'))
    # print('No data')
    # print('No data')
    # print('No data')

    # # #AGGREGATE FUNCTION
    # print(pa.TableGroupBy(data,'owner_id').aggregate([("owner_id", "count")]))
    # print('No data')
    # print('No data')
    # print('No data')


    # ##CLOUD PAK

    # #CONVERT TYPE
    # print(data['owner_id'].cast(pa.string()))
    # print(pa.Table.from_arrays([data.column('owner_id').cast(pa.string())], names=['owner_id']))

    # #CONCATENATE COLUMNS
    # title_column = data.column('owner_login').cast('string')
    # year_column = data.column('owner_id').cast('string')
    # concatenated = pc.binary_join_element_wise(title_column, year_column, '-')
    # concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(''), concatenated)
    # print(data.append_column('title-year', concatenated).select(['title-year']))

    # #SPLIT COLUMN
    # titles = data['owner_login']
    # title_split1 = []
    # title_split2 = []
    # for title in titles:
    #     split_title = title.as_py().split('-', 1)
    #     title_split1.append(split_title[0])
    #     title_split2.append(split_title[1] if len(split_title) > 1 else '')
    # title_split1_array = pa.array(title_split1)
    # title_split2_array = pa.array(title_split2)
    # split_data = pa.table([title_split1_array, title_split2_array], names=['title_split1', 'title_split2'])

    # print(split_data)

    # #REPLACE MISSING VALUES
    # data1 = data
    # genres1 = data1['description']
    # replaced_genres1 = pc.fill_null(genres1, pa.scalar('No description'))
    # new_table = data1.set_column(data1.schema.get_field_index('description'), 'description', replaced_genres1)
    # print(new_table)

    # #SELF JOIN
    # data_left = data
    # data_right = data

    # filter_mask = pc.is_in(data_left['owner_login'], data_right['owner_login'])
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
    # column_names.remove('owner_login')
    # print(data.select(column_names))

    # port = 50052
    # client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    # table_name = 'FlattenedFirstJSON_gists'
    # reader = client.do_get(flight.Ticket(table_name.encode()))
    # data = reader.read_all()

    # # SELECTION
    # # first level query
    # ## to string
    # print(data.select(['owner.login']))
    # # ## to int
    # print(data.select(['owner.id']))
    # # ## object from list 
    # # ### low level of nulls - first element of list
    # print('No data')
    # # ### medium level of nulls
    # print('No data')
    # # ### high level of nulls - last element of list
    # print('No data')


    # # # # FILTRES
    # print(pc.filter(data, pc.greater(data.column('owner.id'), 2000)))

    # # #SORT
    # print(pc.take(data,pc.sort_indices(data, sort_keys=[("owner.login", "ascending")])))
    # # #SORT DESC
    # print(pc.take(data,pc.sort_indices(data, sort_keys=[("owner.login", "descending")])))

    # #GROUP BY
    # print(pa.TableGroupBy(data,'owner.id'))
    # print('No data')
    # print('No data')
    # print('No data')

    # # #AGGREGATE FUNCTION
    # print(pa.TableGroupBy(data,'owner.id').aggregate([("owner.id", "count")]))
    # print('No data')
    # print('No data')
    # print('No data')



    # ##CLOUD PAK

    # #CONVERT TYPE
    # print(data['owner.id'].cast(pa.string()))
    # print(pa.Table.from_arrays([data.column('owner.id').cast(pa.string())], names=['owner.id']))

    # #CONCATENATE COLUMNS
    # title_column = data.column('owner.login').cast('string')
    # year_column = data.column('owner.id').cast('string')
    # concatenated = pc.binary_join_element_wise(title_column, year_column, '-')
    # concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(''), concatenated)
    # print(data.append_column('title-year', concatenated).select(['title-year']))

    # #SPLIT COLUMN
    # titles = data['owner.login']
    # title_split1 = []
    # title_split2 = []
    # for title in titles:
    #     split_title = title.as_py().split('-', 1)
    #     title_split1.append(split_title[0])
    #     title_split2.append(split_title[1] if len(split_title) > 1 else '')
    # title_split1_array = pa.array(title_split1)
    # title_split2_array = pa.array(title_split2)
    # split_data = pa.table([title_split1_array, title_split2_array], names=['title_split1', 'title_split2'])

    # print(split_data)

    # #REPLACE MISSING VALUES
    # data1 = data
    # genres1 = data1['description']
    # replaced_genres1 = pc.fill_null(genres1, pa.scalar('No description'))
    # new_table = data1.set_column(data1.schema.get_field_index('description'), 'description', replaced_genres1)
    # print(new_table)

    # #SELF JOIN
    # data_left = data
    # data_right = data

    # filter_mask = pc.is_in(data_left['owner.login'], data_right['owner.login'])
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
    # column_names.remove('owner.login')
    # print(data.select(column_names))

    port = 50053
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    main_table_name = 'TablesMethod_gists'

    reader = client.do_get(flight.Ticket(main_table_name.encode()))
    data = reader.read_all()

    # SELECTION
    # first level query
    ## to string
    print(data.select(['owner.login']))
    # ## to int
    print(data.select(['owner.id']))
    # ## object from list 
    # ### low level of nulls - first element of list
    print('No data')
    # ### medium level of nulls
    print('No data')
    # ### high level of nulls - last element of list
    print('No data')

    # # # FILTRES
    print(pc.filter(data, pc.greater(data.column('owner.id'), 2000)))

    # #SORT
    print(pc.take(data,pc.sort_indices(data, sort_keys=[("owner.login", "ascending")])))
    # #SORT DESC
    print(pc.take(data,pc.sort_indices(data, sort_keys=[("owner.login", "descending")])))

    #GROUP BY
    print(pa.TableGroupBy(data,'owner.id'))
    print('No data')
    print('No data')
    print('No data')

    # #AGGREGATE FUNCTION
    print(pa.TableGroupBy(data,'owner.id').aggregate([("owner.id", "count")]))
    print('No data')
    print('No data')
    print('No data')

    ##CLOUD PAK

    #CONVERT TYPE
    print(data['owner.id'].cast(pa.string()))
    print(pa.Table.from_arrays([data.column('owner.id').cast(pa.string())], names=['owner.id']))

    #CONCATENATE COLUMNS
    title_column = data.column('owner.login').cast('string')
    year_column = data.column('owner.id').cast('string')
    concatenated = pc.binary_join_element_wise(title_column, year_column, '-')
    concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(''), concatenated)
    print(data.append_column('title-year', concatenated).select(['title-year']))

    #SPLIT COLUMN
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

    #REPLACE MISSING VALUES
    data1 = data
    genres1 = data1['description']
    replaced_genres1 = pc.fill_null(genres1, pa.scalar('No description'))
    new_table = data1.set_column(data1.schema.get_field_index('description'), 'description', replaced_genres1)
    print(new_table)

    #SELF JOIN
    data_left = data
    data_right = data

    filter_mask = pc.is_in(data_left['owner.login'], data_right['owner.login'])
    filtered_data_left = data_left.filter(filter_mask)
    joined_data = pa.concat_tables([filtered_data_left, data_right])
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
    column_names = data.column_names
    column_names.remove('owner.login')
    print(data.select(column_names))

if __name__ == '__main__':
    random.seed(23)
    client_gists()
    # client_example()