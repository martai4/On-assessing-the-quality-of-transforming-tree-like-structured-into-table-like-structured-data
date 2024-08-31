import threading
import itertools

from Statisticker import Statisticker
from methods.JSONFirstListFlattener import JSONFirstListFlattener
from methods.JSONPathFlattener import JSONPathFlattener
from methods.JSONListToTableConverter import JSONListToTableConverter
from methods.JSONFlatten import JSONFlatten
from methods.JSONHierarchical import JSONHierarchical
from methods.JSONDummy import JSONDummy

if __name__ == "__main__":
    TEST_LOOPS = 12

    files_to_check = [
        "airlines",
        # "gists",
        # "movies",
        # "reddit",
        # "nasa",
    ]

    statisticker = Statisticker()
    # monitor_thread = threading.Thread(target=statisticker.start_monitoring, args=("statistics",))
    # monitor_thread.start()

    flatter_list = [
        (JSONPathFlattener(), 50051, "JSONPathFlattener"),
        # (JSONFirstListFlattener(), 50052, "JSONFirstListFlattener"),
        # (JSONListToTableConverter(), 50053, "JSONListToTableConverter"),
        # (JSONFlatten(), 50054, "JSONFlatten"),
        # (JSONHierarchical(), 50055, "Hierarchical"),
    ]

    # IF script runs more than once
    how_many_already = 0

    for loop, test_file in itertools.product(range(TEST_LOOPS), files_to_check):
        print(f"Loop nr: {loop}")
        for flatter, port, method_name in flatter_list:
            # memory_monitor_thread = threading.Thread(
            #     target=statisticker.start_monitoring,
            #     args=(
            #         f"tests/memory/{test_file}-{method_name}-{loop + how_many_already}",
            #     ),
            # )
            # memory_monitor_thread.start()

            server_thread = threading.Thread(target=flatter.serve, args=(port,))
            server_thread.start()

            print(f"--- {method_name} ---")
            # statisticker.start_measuring_cpu()
            # statisticker.start_measuring_time()

            flatter.load_json_from_file([f"../data/testFiles/{test_file}.txt"])

            # statisticker.stop_measuring_time(f"tests/time/{test_file}-{method_name}")
            # statisticker.stop_measuring_cpu(f"tests/cpu/{test_file}-{method_name}")
            # statisticker.stop_monitoring()

            # memory_monitor_thread.join()
            flatter.server.stop()
            server_thread.join()
