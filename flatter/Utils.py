from methods.JSONFirstListFlattener import JSONFirstListFlattener
from methods.JSONPathFlattener import JSONPathFlattener
from methods.JSONListToTableConverter import JSONListToTableConverter
from methods.JSONFlatten import JSONFlatten
from methods.JSONDummy import JSONDummy

def get_strategy(strategy: str):
    return JSONFirstListFlattener()