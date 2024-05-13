# python -m uvicorn api:app --reload --port 8000

import socket, time, asyncio, sys
from fastapi import FastAPI
import threading

import Utils
from Statisticker import Statisticker

app = FastAPI()
statisticker = Statisticker()

json_list = []

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.post("/prepare-test/")
async def prepare_test(socket_port: int):
    asyncio.create_task(prepare_test_task(socket_port))
    time.sleep(1)  # Wait for the socket to be set
    return "OK"

async def prepare_test_task(socket_port: int):
    host = '127.0.0.1'
    buffer_size = 2 ** 32

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, socket_port))
        s.listen(1)
        print(f'Listen on {socket_port}...')

        conn, addr = s.accept()
        print(f'Connected with {addr}')

        try:
            while True:
                data = conn.recv(buffer_size)
                if not data:
                    break

                stringdata = data.decode('utf-8', errors='ignore')
                json = list(filter(None, stringdata.split(">>>")))
                json_list.extend(json)
        finally:
            size = sys.getsizeof(json_list) / (1024 * 1024)
            print("Size of the list in MB:", round(size, 3))

            conn.close()

@app.post("/socket-test/")
async def socket_test(processing_strategy:str, server_port: int) -> str:
    asyncio.create_task(socket_test_task(processing_strategy, server_port))
    return "OK"

async def socket_test_task(processing_strategy:str, server_port: int):
    filename = "temp---100---100---x" # TODO set proper filename

    print(f'Processing strategy: {processing_strategy}')
    flattener = Utils.get_strategy(processing_strategy)
    flattener_server_tread = threading.Thread(target=flattener.serve, args=(server_port,))
    flattener_server_tread.start()
    time.sleep(1)  # Wait for server to start

    statisticker.start_measuring_time()
    monitor_thread = threading.Thread(target=statisticker.start_monitoring, args=(f"tests/cpu-memory/{filename}",))
    monitor_thread.start()

    flattener.do_put("TestDataset", json_list[1:-1])

    statisticker.stop_monitoring()
    statisticker.stop_measuring_time(f"tests/time/{'-'.join(filename.split('---')[:-1])}")
    monitor_thread.join()
    flattener.server.stop()
    flattener_server_tread.join()
    print("Test finished successfully")
