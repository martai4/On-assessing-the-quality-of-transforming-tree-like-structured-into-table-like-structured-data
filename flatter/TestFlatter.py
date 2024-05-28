import threading

from Statisticker import Statisticker
from methods.JSONFirstListFlattener import JSONFirstListFlattener
from methods.JSONPathFlattener import JSONPathFlattener
from methods.JSONListToTableConverter import JSONListToTableConverter
from methods.JSONFlatten import JSONFlatten
from methods.JSONDummy import JSONDummy

if __name__ == "__main__":
    TEST_LOOPS = 12

    knowledge_files = ['../data/airlines.json',
                       '../data/gists.json',
                       '../data/movies.json',
                       '../data/reddit.json',
                       '../data/nasa.json']
    
    test_name = "AIRLINES-100000"
    file_name = "../data/testFiles/" + test_name
    files_to_check = 20

    statisticker = Statisticker()
    # monitor_thread = threading.Thread(target=statisticker.start_monitoring, args=("statistics",))
    # monitor_thread.start()

    flatter_list = [
        (JSONPathFlattener(), 50051, "JSONPathFlattener"),
        (JSONFirstListFlattener(), 50052, "JSONFirstListFlattener"),
        (JSONListToTableConverter(), 50053, "JSONListToTableConverter"),
        (JSONFlatten(), 50054, "JSONFlatten"),
        (JSONDummy(), 50055, "JSONDummy")
    ]

    for i in range(TEST_LOOPS):
        print(f"Loop nr: {i}")
        for (flatter, port, method_name) in flatter_list:
            thread = threading.Thread(target=flatter.serve, args=(port,))
            thread.start()

            print(f"--- {method_name} ---")
            statisticker.start_measuring_time()
            # flatter.load_json_from_file(knowledge_files) # Knowledge files
            flatter.load_json_from_file([f"{file_name}-{i}.txt" for i in range(files_to_check)])
            statisticker.stop_measuring_time(f"tests/time/files/{test_name}-{method_name}")

            flatter.server.stop()
            thread.join()

        statisticker.stop_monitoring()
        # monitor_thread.join()
