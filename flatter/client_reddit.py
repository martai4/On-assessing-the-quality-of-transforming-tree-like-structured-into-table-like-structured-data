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
    additional_data = f"reddit;{current_method};{query};"
    statisticker.stop_measuring_time_csv(additional_data, MAIN_FILE)


def client_reddit():
    global current_method
    # ------------------------------------------------------------------------------------------------
    port = 50051
    current_method = "PathFlattener"
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    table_name = 'JSONPath_reddit'
    reader = client.do_get(flight.Ticket(table_name.encode()))
    data = reader.read_all()

    # # SELECTION
    # # first level query
    # ## to string
    start()
    print(data.select(['data.after']))
    stop(SELECTION_TO_STR)
    # ## to int
    start()
    print(data.select(['data.dist']))
    stop(SELECTION_TO_INT)
    # ## object from list 
    # ### low level of nulls - first element of list
    start()
    print(data.select(['data.children[0].data.subreddit']))
    stop(SELECTION_FROM_LIST_LOW_LVL_OF_NULLS)
    # ### medium level of nulls
    start()
    print(data.select(['data.children[9].data.subreddit']))
    stop(SELECTION_FROM_LIST_MEDIUM_LVL_OF_NULLS)
    # ### high level of nulls - last element of list
    start()
    print(data.select(['data.children[24].data.subreddit']))
    stop(SELECTION_FROM_LIST_HIGH_LVL_OF_NULLS)

    # # # FILTRES
    start()
    print(pc.filter(data, pc.greater(data.column('data.children[0].data.created'), 2000)))
    stop(FILTRES)

    # #SORT
    start()
    print(pc.take(data,pc.sort_indices(data, sort_keys=[("data.after", "ascending")])))
    stop(SORT)
    # #SORT DESC
    start()
    print(pc.take(data,pc.sort_indices(data, sort_keys=[("data.after", "descending")])))
    stop(SORT_DESC)

    #GROUP BY
    start()
    print(pa.TableGroupBy(data,'data.after'))
    stop(GROUP_BY_SIMPLE_ATTRIBUTE)
    start()
    print(pa.TableGroupBy(data,'data.children[0].data.subreddit'))
    stop(GROUP_BY_ONE_ATTRIBUTE_FROM_LIST_SIMPLE)
    start()
    print(pa.TableGroupBy(data, ['data.children[0].data.subreddit','data.children[1].data.subreddit']))
    stop(GROUP_BY_TWO_ATTRIBUTES_FROM_LIST)
    print('No data')

    #AGGREGATE FUNCTION
    start()
    print(pa.TableGroupBy(data,'data.after').aggregate([("data.after", "count")]))
    stop(AGGREGATE_FUNCTION_SIMPLE_ATTRIBUTE)
    
    start()
    print(pa.TableGroupBy(data, 'data.children[0].data.subreddit').aggregate([('data.children[0].data.subreddit', "count")]))
    stop(AGGREGATE_FUNCTION_ONE_FROM_LIST)
    
    start()
    print(pa.TableGroupBy(data, ['data.children[0].data.subreddit','data.children[1].data.subreddit']).aggregate([('data.children[0].data.subreddit', "count"),('data.children[1].data.subreddit', "count")]))
    stop(AGGREGATE_FUNCTION_TWO_FROM_LIST)
    print('No data')

    # ##CLOUD PAK

    #CONVERT TYPE
    start()
    print(data['data.dist'].cast(pa.string()))
    print(pa.Table.from_arrays([data.column('data.dist').cast(pa.string())], names=['data.dist']))
    stop(CONVERT_TYPE)

    #CONCATENATE COLUMNS
    start()
    title_column = data.column('data.after').cast('string')
    year_column = data.column('data.dist').cast('string')
    concatenated = pc.binary_join_element_wise(title_column, year_column, '-')
    concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(''), concatenated)
    print(data.append_column('title-year', concatenated).select(['title-year']))
    stop(CONCATENATE_COLUMNS)

    #SPLIT COLUMN
    start()
    titles = data['data.after']
    title_split1 = []
    title_split2 = []
    for title in titles:
        split_title = title.as_py().split('_', 1)
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
    genres1 = data1['data.children[0].data.subreddit']
    replaced_genres1 = pc.fill_null(genres1, pa.scalar('No subreddit'))
    new_table = data1.set_column(data1.schema.get_field_index('data.children[0].data.subreddit'), 'data.children[0].data.subreddit', replaced_genres1)
    print(new_table)
    stop(REPLACE_MISSING_VALUES)

    #SELF JOIN
    start()
    data_left = data
    data_right = data

    filter_mask = pc.is_in(data_left['data.after'], data_right['data.after'])
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

    print('No data')

    # REMOVE COLUMN
    start()
    column_names = data.column_names
    column_names.remove('data.after')
    print(data.select(column_names))
    stop(REMOVE_COLUMN)

    # ------------------------------------------------------------------------------------------------
    port = 50054
    current_method = "JSONFlatten"
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    table_name = 'SimpleMethod_reddit'
    reader = client.do_get(flight.Ticket(table_name.encode()))
    data = reader.read_all()

    # # SELECTION
    # # first level query
    # ## to string
    start()
    print(data.select(['data_after']))
    stop(SELECTION_TO_STR)
    # ## to int
    start()
    print(data.select(['data_dist']))
    stop(SELECTION_TO_INT)
    # ## object from list 
    # ### low level of nulls - first element of list
    start()
    print(data.select(['data_children_0_data_subreddit']))
    stop(SELECTION_FROM_LIST_LOW_LVL_OF_NULLS)
    # # ### medium level of nulls
    start()
    print(data.select(['data_children_9_data_subreddit']))
    stop(SELECTION_FROM_LIST_MEDIUM_LVL_OF_NULLS)
    # # ### high level of nulls - last element of list
    start()
    print(data.select(['data_children_24_data_subreddit']))
    stop(SELECTION_FROM_LIST_HIGH_LVL_OF_NULLS)

    # # # FILTRES
    start()
    print(pc.filter(data, pc.greater(data.column('data_children_0_data_created'), 2000)))
    stop(FILTRES)

    # # #SORT
    start()
    print(pc.take(data,pc.sort_indices(data, sort_keys=[("data_after", "ascending")])))
    stop(SORT)
    # # #SORT DESC
    start()
    print(pc.take(data,pc.sort_indices(data, sort_keys=[("data_after", "descending")])))
    stop(SORT_DESC)

    # #GROUP BY
    start()
    print(pa.TableGroupBy(data,'data_after'))
    stop(GROUP_BY_SIMPLE_ATTRIBUTE)
    start()
    print(pa.TableGroupBy(data,'data_children_0_data_subreddit'))
    stop(GROUP_BY_ONE_ATTRIBUTE_FROM_LIST_SIMPLE)
    start()
    print(pa.TableGroupBy(data, ['data_children_0_data_subreddit','data_children_1_data_subreddit']))
    stop(GROUP_BY_TWO_ATTRIBUTES_FROM_LIST)
    print('No data')

    # # #AGGREGATE FUNCTION
    start()
    print(pa.TableGroupBy(data,'data_after').aggregate([("data_after", "count")]))
    stop(AGGREGATE_FUNCTION_SIMPLE_ATTRIBUTE)

    start()
    print(pa.TableGroupBy(data, 'data_children_0_data_subreddit').aggregate([('data_children_0_data_subreddit', "count")]))
    stop(AGGREGATE_FUNCTION_ONE_FROM_LIST)

    start()
    print(pa.TableGroupBy(data, ['data_children_0_data_subreddit','data_children_1_data_subreddit']).aggregate([('data_children_0_data_subreddit', "count"),('data_children_1_data_subreddit', "count")]))
    stop(AGGREGATE_FUNCTION_TWO_FROM_LIST)
    print('No data')

    # ##CLOUD PAK

    #CONVERT TYPE
    start()
    print(data['data_dist'].cast(pa.string()))
    print(pa.Table.from_arrays([data.column('data_dist').cast(pa.string())], names=['data_dist']))
    stop(CONVERT_TYPE)

    #CONCATENATE COLUMNS
    start()
    title_column = data.column('data_after').cast('string')
    year_column = data.column('data_dist').cast('string')
    concatenated = pc.binary_join_element_wise(title_column, year_column, '-')
    concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(''), concatenated)
    print(data.append_column('title-year', concatenated).select(['title-year']))
    stop(CONCATENATE_COLUMNS)

    #SPLIT COLUMN
    start()
    titles = data['data_after']
    title_split1 = []
    title_split2 = []
    for title in titles:
        split_title = title.as_py().split('_', 1)
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
    genres1 = data1['data_children_0_data_subreddit']
    replaced_genres1 = pc.fill_null(genres1, pa.scalar('No subreddit'))
    new_table = data1.set_column(data1.schema.get_field_index('data_children_0_data_subreddit'), 'data_children_0_data_subreddit', replaced_genres1)
    print(new_table)
    stop(REPLACE_MISSING_VALUES)

    #SELF JOIN
    start()
    data_left = data
    data_right = data

    filter_mask = pc.is_in(data_left['data_after'], data_right['data_after'])
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

    print('No data')

    # REMOVE COLUMN
    start()
    column_names = data.column_names
    column_names.remove('data_after')
    print(data.select(column_names))
    stop(REMOVE_COLUMN)

    # ------------------------------------------------------------------------------------------------
    port = 50052
    current_method = "FirstListFlattener"
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    table_name = 'FlattenedFirstJSON_reddit'
    reader = client.do_get(flight.Ticket(table_name.encode()))
    data = reader.read_all()

    # # SELECTION
    # # first level query
    # # to string
    start()
    print(data.select(['data.after']))
    stop(SELECTION_TO_STR)
    # # ## to int
    start()
    print(data.select(['data.dist']))
    stop(SELECTION_TO_INT)

    # object from list 
    ### low level of nulls - first element of list
    start()
    children_column = data['data.children']
    subreddits = []
    for row in children_column:
        list_array = row.as_py()
        if list_array:
            first_child = list_array[0]
            if 'data' in first_child and 'subreddit' in first_child['data']:
                subreddit = first_child['data']['subreddit']
                subreddits.append(subreddit)
    print(subreddits)
    stop(SELECTION_FROM_LIST_LOW_LVL_OF_NULLS)

    # # # ### medium level of nulls
    start()
    children_column = data['data.children']
    subreddits = []
    for row in children_column:
        list_array = row.as_py()
        if list_array:
            first_child = list_array[9]
            if 'data' in first_child and 'subreddit' in first_child['data']:
                subreddit = first_child['data']['subreddit']
                subreddits.append(subreddit)
    print(subreddits)
    stop(SELECTION_FROM_LIST_MEDIUM_LVL_OF_NULLS)

    # ### high level of nulls - last element of list
    start()
    children_column = data['data.children']
    subreddits = []
    for row in children_column:
        list_array = row.as_py()
        if list_array:
            first_child = list_array[24]
            if 'data' in first_child and 'subreddit' in first_child['data']:
                subreddit = first_child['data']['subreddit']
                subreddits.append(subreddit)
    print(subreddits)
    stop(SELECTION_FROM_LIST_HIGH_LVL_OF_NULLS)

    # # # # FILTRES
    start()
    children_column = data['data.children']
    subreddits = []
    for row in children_column:
        list_array = row.as_py()
        if list_array:
            first_child = list_array[0]
            if 'data' in first_child and 'created' in first_child['data']:
                subreddit = first_child['data']['created']
                if subreddit>2000:
                    subreddits.append(subreddit)
    print(subreddits)
    stop(FILTRES)

    # #SORT
    start()
    print(pc.take(data,pc.sort_indices(data, sort_keys=[("data.after", "ascending")])))
    stop(SORT)
    # #SORT DESC
    start()
    print(pc.take(data,pc.sort_indices(data, sort_keys=[("data.after", "descending")])))
    stop(SORT_DESC)

    #GROUP BY
    start()
    print(pa.TableGroupBy(data,'data.after'))
    stop(GROUP_BY_SIMPLE_ATTRIBUTE)

    
    start()
    children_column = data['data.children']
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
    print(pa.TableGroupBy(data,'subreddit'))
    stop(GROUP_BY_ONE_ATTRIBUTE_FROM_LIST_SIMPLE)

    start()
    children_column = data['data.children']
    subreddits0 = []
    for row in children_column:
        list_array = row.as_py()
        if list_array:
            first_child = list_array[0]
            if 'data' in first_child and 'subreddit' in first_child['data']:
                subreddit = first_child['data']['subreddit']
                subreddits0.append(subreddit)
            else:
                subreddits0.append(None)
        else:
            subreddits0.append(None)

    subreddits1 = []
    for row in children_column:
        list_array = row.as_py()
        if list_array:
            first_child = list_array[1]
            if 'data' in first_child and 'subreddit' in first_child['data']:
                subreddit = first_child['data']['subreddit']
                subreddits1.append(subreddit)
            else:
                subreddits1.append(None)
        else:
            subreddits1.append(None)
    new_table = pa.Table.from_arrays([subreddits0, subreddits1], names=['subreddit_0', 'subreddit_1'])
    grouped_table = new_table.group_by(['subreddit_0', 'subreddit_1'])
    print(grouped_table)
    stop(GROUP_BY_TWO_ATTRIBUTES_FROM_LIST)
    print('No data')
    
    # #AGGREGATE FUNCTION
    start()
    print(pa.TableGroupBy(data,'data.after').aggregate([("data.after", "count")]))
    stop(AGGREGATE_FUNCTION_SIMPLE_ATTRIBUTE)

    start()
    children_column = data['data.children']
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
    print(pa.TableGroupBy(new_table,'subreddit').aggregate([('subreddit', "count")]))
    stop(AGGREGATE_FUNCTION_ONE_FROM_LIST)

    start()
    children_column = data['data.children']
    subreddits0 = []
    for row in children_column:
        list_array = row.as_py()
        if list_array:
            first_child = list_array[0]
            if 'data' in first_child and 'subreddit' in first_child['data']:
                subreddit = first_child['data']['subreddit']
                subreddits0.append(subreddit)
            else:
                subreddits0.append(None)
        else:
            subreddits0.append(None)

    subreddits1 = []
    for row in children_column:
        list_array = row.as_py()
        if list_array:
            first_child = list_array[1]
            if 'data' in first_child and 'subreddit' in first_child['data']:
                subreddit = first_child['data']['subreddit']
                subreddits1.append(subreddit)
            else:
                subreddits1.append(None)
        else:
            subreddits1.append(None)
    new_table = pa.Table.from_arrays([subreddits0, subreddits1], names=['subreddit_0', 'subreddit_1'])
    grouped_table = new_table.group_by(['subreddit_0', 'subreddit_1'])
    print(grouped_table.aggregate([('subreddit_0', "count"),('subreddit_1', "count")]))
    stop(AGGREGATE_FUNCTION_TWO_FROM_LIST)
    print('No data')

    ##CLOUD PAK

    # CONVERT TYPE
    start()
    print(data['data.dist'].cast(pa.string()))
    print(pa.Table.from_arrays([data.column('data.dist').cast(pa.string())], names=['data.dist']))
    stop(CONVERT_TYPE)

    #CONCATENATE COLUMNS
    start()
    title_column = data.column('data.after').cast('string')
    year_column = data.column('data.dist').cast('string')
    concatenated = pc.binary_join_element_wise(title_column, year_column, '-')
    concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(''), concatenated)
    print(data.append_column('title-year', concatenated).select(['title-year']))
    stop(CONCATENATE_COLUMNS)

    #SPLIT COLUMN
    start()
    titles = data['data.after']
    title_split1 = []
    title_split2 = []
    for title in titles:
        split_title = title.as_py().split('_', 1)
        title_split1.append(split_title[0])
        title_split2.append(split_title[1] if len(split_title) > 1 else '')
    title_split1_array = pa.array(title_split1)
    title_split2_array = pa.array(title_split2)
    split_data = pa.table([title_split1_array, title_split2_array], names=['title_split1', 'title_split2'])

    print(split_data)
    stop(SPLIT_COLUMN)

    #REPLACE MISSING VALUES
    start()
    # data1 = data
    # genres1 = data1['genres'].to_pylist()
    # replaced_genres1 = []
    # for row in genres1:
    #     if len(row) >= 2:
    #         replaced_genres1.append(row[1])
    #     else:
    #         replaced_genres1.append('No genres1')
    children_column = data['data.children']
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
    stop(REPLACE_MISSING_VALUES)

    #SELF JOIN
    start()
    data_left = data
    data_right = data

    filter_mask = pc.is_in(data_left['data.after'], data_right['data.after'])
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

    print('No data')

    # REMOVE COLUMN
    start()
    column_names = data.column_names
    column_names.remove('data.after')
    print(data.select(column_names))
    stop(REMOVE_COLUMN)

    # ------------------------------------------------------------------------------------------------
    port = 50053
    current_method = "ListToTableConverter"
    client = flight.FlightClient(f"grpc+tcp://localhost:{port}")
    main_table_name = 'TablesMethod_reddit'
    children_table_name = 'TablesMethod_reddit_data.children'
    link_flair_richtext_table_name = 'TablesMethod_reddit_value.data.link_flair_richtext'
    user_reports_table_name = 'TablesMethod_reddit_value.data.user_reports'
    author_flair_richtext_table_name = 'TablesMethod_reddit_value.data.author_flair_richtext'
    content_categories_table_name = 'TablesMethod_reddit_value.data.content_categories'
    preview_images_table_name = 'TablesMethod_reddit_value.data.preview.images'
    resolutions_table_name = 'TablesMethod_reddit_value.resolutions'
    obfuscated_resolutions_table_name = 'TablesMethod_reddit_value.variants.obfuscated.resolutions'
    nsfw_resolutions_table_name = 'TablesMethod_reddit_value.variants.nsfw.resolutions'
    all_awardings_table_name = 'TablesMethod_reddit_value.data.all_awardings'
    awarders_table_name = 'TablesMethod_reddit_value.data.awarders'
    treatment_tags_table_name = 'TablesMethod_reddit_value.data.treatment_tags'
    mod_reports_table_name = 'TablesMethod_reddit_value.data.mod_reports'
    media_metadata_i9maqgeklzpb1_p_table_name = 'TablesMethod_reddit_value.data.media_metadata.i9maqgeklzpb1.p'
    media_metadata_kp02ageklzpb1_p_table_name = 'TablesMethod_reddit_value.data.media_metadata.kp02ageklzpb1.p'
    media_metadata_gbnbhheklzpb1_p_table_name = 'TablesMethod_reddit_value.data.media_metadata.gbnbhheklzpb1.p'
    media_metadata_sir4jfeklzpb1_p_table_name = 'TablesMethod_reddit_value.data.media_metadata.sir4jfeklzpb1.p'
    media_metadata_4otai5fklzpb1_p_table_name = 'TablesMethod_reddit_value.data.media_metadata.4otai5fklzpb1.p'
    gallery_data_items_table_name = 'TablesMethod_reddit_value.data.gallery_data.items'


    reader = client.do_get(flight.Ticket(main_table_name.encode()))
    main_data = reader.read_all()

    reader_children = client.do_get(flight.Ticket(children_table_name.encode()))
    children_data = reader_children.read_all()

    reader_link_flair = client.do_get(flight.Ticket(link_flair_richtext_table_name.encode()))
    link_flair_data = reader_link_flair.read_all()

    reader_user_reports = client.do_get(flight.Ticket(user_reports_table_name.encode()))
    user_reports_data = reader_user_reports.read_all()

    reader_author_flair = client.do_get(flight.Ticket(author_flair_richtext_table_name.encode()))
    author_flair_data = reader_author_flair.read_all()

    reader_content_categories = client.do_get(flight.Ticket(content_categories_table_name.encode()))
    content_categories_data = reader_content_categories.read_all()

    reader_preview_images = client.do_get(flight.Ticket(preview_images_table_name.encode()))
    preview_images_data = reader_preview_images.read_all()

    reader_resolutions = client.do_get(flight.Ticket(resolutions_table_name.encode()))
    resolutions_data = reader_resolutions.read_all()

    reader_obfuscated_resolutions = client.do_get(flight.Ticket(obfuscated_resolutions_table_name.encode()))
    obfuscated_resolutions_data = reader_obfuscated_resolutions.read_all()

    reader_nsfw_resolutions = client.do_get(flight.Ticket(nsfw_resolutions_table_name.encode()))
    nsfw_resolutions_data = reader_nsfw_resolutions.read_all()

    reader_all_awardings = client.do_get(flight.Ticket(all_awardings_table_name.encode()))
    all_awardings_data = reader_all_awardings.read_all()

    reader_awarders = client.do_get(flight.Ticket(awarders_table_name.encode()))
    awarders_data = reader_awarders.read_all()

    reader_treatment_tags = client.do_get(flight.Ticket(treatment_tags_table_name.encode()))
    treatment_tags_data = reader_treatment_tags.read_all()

    reader_mod_reports = client.do_get(flight.Ticket(mod_reports_table_name.encode()))
    mod_reports_data = reader_mod_reports.read_all()

    reader_media_i9maqgeklzpb1_p = client.do_get(flight.Ticket(media_metadata_i9maqgeklzpb1_p_table_name.encode()))
    media_i9maqgeklzpb1_p_data = reader_media_i9maqgeklzpb1_p.read_all()

    reader_media_kp02ageklzpb1_p = client.do_get(flight.Ticket(media_metadata_kp02ageklzpb1_p_table_name.encode()))
    media_kp02ageklzpb1_p_data = reader_media_kp02ageklzpb1_p.read_all()

    reader_media_gbnbhheklzpb1_p = client.do_get(flight.Ticket(media_metadata_gbnbhheklzpb1_p_table_name.encode()))
    media_gbnbhheklzpb1_p_data = reader_media_gbnbhheklzpb1_p.read_all()

    reader_media_sir4jfeklzpb1_p = client.do_get(flight.Ticket(media_metadata_sir4jfeklzpb1_p_table_name.encode()))
    media_sir4jfeklzpb1_p_data = reader_media_sir4jfeklzpb1_p.read_all()

    reader_media_4otai5fklzpb1_p = client.do_get(flight.Ticket(media_metadata_4otai5fklzpb1_p_table_name.encode()))
    media_4otai5fklzpb1_p_data = reader_media_4otai5fklzpb1_p.read_all()

    reader_gallery_items = client.do_get(flight.Ticket(gallery_data_items_table_name.encode()))
    gallery_items_data = reader_gallery_items.read_all()

    # SELECTION
    # first level query
    # to string
    start()
    print(main_data.select(['data.after']))
    stop(SELECTION_TO_STR)
    # ## to int
    start()
    print(main_data.select(['data.dist']))
    stop(SELECTION_TO_INT)
    # # ## object from list 
    # # ### low level of nulls - first element of list
    # print(children_data)
    start()
    print(pc.filter(children_data, pc.equal(children_data.column('row_number'), 1)).select(['value.data.subreddit']))
    stop(SELECTION_FROM_LIST_LOW_LVL_OF_NULLS)
    # # ### medium level of nulls
    start()
    print(pc.filter(children_data, pc.equal(children_data.column('row_number'), 9)).select(['value.data.subreddit']))
    stop(SELECTION_FROM_LIST_MEDIUM_LVL_OF_NULLS)
    # # ### high level of nulls - last element of list
    start()
    print(pc.filter(children_data, pc.equal(children_data.column('row_number'), 24)).select(['value.data.subreddit']))
    stop(SELECTION_FROM_LIST_HIGH_LVL_OF_NULLS)

    # # FILTRES
    start()
    children0=pc.filter(children_data, pc.equal(children_data.column('row_number'), 1))
    print(pc.filter(children0, pc.greater(children0.column('value.data.created'), 2000)))
    stop(FILTRES)

    # #SORT
    start()
    print(pc.take(main_data,pc.sort_indices(main_data, sort_keys=[("data.after", "ascending")])))
    stop(SORT)
    # #SORT DESC
    start()
    print(pc.take(main_data,pc.sort_indices(main_data, sort_keys=[("data.after", "descending")])))
    stop(SORT_DESC)

    # #GROUP BY
    start()
    print(pa.TableGroupBy(main_data,'data.after'))
    stop(GROUP_BY_SIMPLE_ATTRIBUTE)

    start()
    print(pa.TableGroupBy(pc.filter(children_data, pc.equal(children_data.column('row_number'), 1)),'value.data.subreddit'))
    stop(GROUP_BY_ONE_ATTRIBUTE_FROM_LIST_SIMPLE)
    
    start()
    value_set = pa.array([1, 2])
    print(pa.TableGroupBy(pc.filter(children_data, pc.is_in(children_data.column('row_number'), value_set=value_set)),'value.data.subreddit'))
    stop(GROUP_BY_TWO_ATTRIBUTES_FROM_LIST)
    print('No data')

    # # #AGGREGATE FUNCTION
    start()
    print(pa.TableGroupBy(main_data,'data.after').aggregate([("data.after", "count")]))
    stop(AGGREGATE_FUNCTION_SIMPLE_ATTRIBUTE)

    start()
    print(pa.TableGroupBy(pc.filter(children_data, pc.equal(children_data.column('row_number'), 1)),'value.data.subreddit').aggregate([('value.data.subreddit', "count")]))
    stop(AGGREGATE_FUNCTION_ONE_FROM_LIST)
    
    start()
    value_set = pa.array([1, 2])
    print(pa.TableGroupBy(pc.filter(children_data, pc.is_in(children_data.column('row_number'), value_set=value_set)),'value.data.subreddit').aggregate([('value.data.subreddit', "count")]))
    stop(AGGREGATE_FUNCTION_TWO_FROM_LIST)

    print('No data')

    #CLOUD PAK

    #CONVERT TYPE
    start()
    print(main_data['data.dist'].cast(pa.string()))
    print(pa.Table.from_arrays([main_data.column('data.dist').cast(pa.string())], names=['data.dist']))
    stop(CONVERT_TYPE)

    #CONCATENATE COLUMNS
    start()
    title_column = main_data.column('data.after').cast('string')
    year_column = main_data.column('data.dist').cast('string')
    concatenated = pc.binary_join_element_wise(title_column, year_column, '-')
    concatenated = pc.if_else(pc.is_null(concatenated), pa.scalar(''), concatenated)
    print(main_data.append_column('title-year', concatenated).select(['title-year']))
    stop(CONCATENATE_COLUMNS)

    #SPLIT COLUMN
    start()
    titles = main_data['data.after']
    title_split1 = []
    title_split2 = []
    for title in titles:
        split_title = title.as_py().split('_', 1)
        title_split1.append(split_title[0])
        title_split2.append(split_title[1] if len(split_title) > 1 else '')
    title_split1_array = pa.array(title_split1)
    title_split2_array = pa.array(title_split2)
    split_data = pa.table([title_split1_array, title_split2_array], names=['title_split1', 'title_split2'])

    print(split_data)
    stop(SPLIT_COLUMN)

    # #REPLACE MISSING VALUES
    start()

    data1 =pc.filter(children_data, pc.equal(children_data.column('row_number'), 1))
    genres1 = data1['value.data.subreddit']
    replaced_genres1 = pc.fill_null(genres1, pa.scalar('No subreddit'))
    new_table = data1.set_column(data1.schema.get_field_index('value.data.subreddit'), 'value.data.subreddit', replaced_genres1)
    print(new_table)
    stop(REPLACE_MISSING_VALUES)

    #SELF JOIN
    start()
    data_left = main_data
    data_right = main_data

    filter_mask = pc.is_in(data_left['data.after'], data_right['data.after'])
    filtered_data_left = data_left.filter(filter_mask)
    joined_data = pa.concat_tables([filtered_data_left, data_right])
    print(joined_data)
    stop(SELF_JOIN)

    #UNION TABLES
    start()
    data_left = main_data
    data_right = main_data
    unioned_data = pa.concat_tables([data_right, data_left])
    print(unioned_data)
    stop(UNION_TABLES)

    #SAMPLE TABLE

    print('No data')

    # REMOVE COLUMN
    start()
    column_names = main_data.column_names
    column_names.remove('data.after')
    print(main_data.select(column_names))
    stop(REMOVE_COLUMN)


if __name__ == '__main__':
    random.seed(23)
    client_reddit()
    # client_example()