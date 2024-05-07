from methods.JSONFirstListFlattener import JSONFirstListFlattener
from methods.JSONPathFlattener import JSONPathFlattener
from methods.JSONListToTableConverter import JSONListToTableConverter
from methods.JSONFlatten import JSONFlatten
from methods.JSONDummy import JSONDummy

def get_strategy(strategy: str):
    result = JSONDummy()

    if strategy == "JSON_FIRST_LIST_FLATTENER":
        result = JSONFirstListFlattener()
    elif strategy == "JSON_PATH_FLATTENER":
        result = JSONPathFlattener()
    elif strategy == "JSON_FLATTEN":
        result = JSONFlatten()
    elif strategy == "JSON_LIST_TO_TABLE_CONVERTER":
        result = JSONListToTableConverter()

    return result