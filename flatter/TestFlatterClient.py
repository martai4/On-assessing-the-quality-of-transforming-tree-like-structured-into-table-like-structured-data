import threading

from Statisticker import Statisticker
from methods.JSONFirstListFlattener import JSONFirstListFlattener
from methods.JSONPathFlattener import JSONPathFlattener
from methods.JSONListToTableConverter import JSONListToTableConverter
from methods.JSONFlatten import JSONFlatten
from methods.JSONDummy import JSONDummy

if __name__ == "__main__":
    knowledge_files = ['../data/airlines.json',
                       '../data/gists.json',
                    #    '../data/testFiles/movies-large-1.txt',
                       '../data/movies.json',
                       '../data/reddit.json',
                       '../data/nasa.json']

    statisticker = Statisticker()

    flatter_list = [
        (JSONPathFlattener(), 50051, "JSONPathFlattener"),
        (JSONFirstListFlattener(), 50052, "JSONFirstListFlattener"),
        (JSONListToTableConverter(), 50053, "JSONListToTableConverter"),
        (JSONFlatten(), 50054, "JSONFlatten"),
        (JSONDummy(), 50055, "JSONDummy"),
    ]

    for (flatter, port, method_name) in flatter_list:
        thread = threading.Thread(target=flatter.serve, args=(port,))
        thread.start()

        print(f"--- {method_name} ---")
        flatter.load_json_from_file(knowledge_files)
        print(f"--- ============= ---")

        # flatter.server.stop()
        # thread.join()
