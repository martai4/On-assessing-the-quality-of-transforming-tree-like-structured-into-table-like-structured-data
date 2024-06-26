import asyncio
import socket
import threading
import time
import json

import uvicorn
from fastapi import FastAPI
from fastapi_profiler import PyInstrumentProfilerMiddleware

import Utils
from Statisticker import Statisticker

app = FastAPI()
# app.add_middleware(PyInstrumentProfilerMiddleware)
statisticker = Statisticker()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/socket-test/")
async def socket_test(processing_strategy:str, socket_port: int, server_port: int, filename: str) -> str:
    asyncio.create_task(socket_test_task(processing_strategy, socket_port, server_port, filename))
    # processing_thread = threading.Thread(target=socket_test_task, args=(processing_strategy, socket_port, server_port, filename))
    # processing_thread.start()
    time.sleep(1)  # Wait for the socket to be set

    return "OK"


async def socket_test_task(processing_strategy: str, socket_port: int, server_port: int, filename: str):
    print(f'Processing strategy: {processing_strategy}')
    flattener = Utils.get_strategy(processing_strategy)
    PROCESSING = False
    json_list = []
    # thread_lock = threading.Lock()

    def socket_test_processing(json_list: list) -> None:
        total = 0
        while PROCESSING or json_list:
            print(f"Elements to proccess: {len(json_list)}")
            if not json_list:
                print("wating for data...")
                time.sleep(0.5)
            else:
                elems = len(json_list)
                flattener.do_put("TestDataset", json_list[:elems])
                del json_list[:elems]
                total += elems
        print(f"Processed elems: {total}")

    flattener_server_tread = threading.Thread(target=flattener.serve, args=(server_port,))
    flattener_server_tread.start()
    time.sleep(1)  # Wait for server to start

    host = '127.0.0.1'
    buffer_size = 2 ** 32
    FIRST_BREAK = 10
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, socket_port))
        s.listen(1)
        print(f'Listen on {socket_port}...')

        conn, addr = s.accept()
        print(f'Connected with {addr}')

        # memory_monitor_thread = threading.Thread(target=statisticker.start_monitoring, args=(f"tests/memory/{filename}",))
        processing_thread = threading.Thread(target=socket_test_processing, args=(json_list,))

        try:
            while True:
                if not PROCESSING and json_list:
                    PROCESSING = True
                    time.sleep(FIRST_BREAK)
                    print("Start of time measuring")
                    statisticker.start_measuring_time()
                    # memory_monitor_thread.start()
                    processing_thread.start()

                data = conn.recv(buffer_size)
                if not data:
                    break

                stringdata = data.decode()
                json_temp = list(filter(None, stringdata.split(">>>")))
                json_list.extend(json.loads(f"[ {','.join(json_temp[1:-1])} ]"))
        finally:
            print("Data capture completed")
            PROCESSING = False
            if processing_thread.is_alive():
                processing_thread.join()
            # statisticker.stop_monitoring()
            statisticker.stop_measuring_time(f"tests/time/{'-'.join(filename.split('---')[:-1])}")

            conn.close()
            flattener.server.stop()
            # if memory_monitor_thread.is_alive():
            #     memory_monitor_thread.join()
            flattener_server_tread.join()
            print("Test finished successfully")

if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True, workers=2)
