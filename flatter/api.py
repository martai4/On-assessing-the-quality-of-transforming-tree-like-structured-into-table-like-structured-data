# python -m uvicorn api:app --reload --port 8000

import socket, time, asyncio
from fastapi import FastAPI
import threading

import Utils
from Statisticker import Statisticker

app = FastAPI()
statisticker = Statisticker()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/socket-test/")
async def socket_test(processing_strategy:str, socket_port: int, server_port: int, filename: str) -> str:
    asyncio.create_task(socket_test_task(processing_strategy, socket_port, server_port, filename))
    time.sleep(1)  # Wait for the socket to be set

    return "OK"

async def socket_test_task(processing_strategy:str, socket_port: int, server_port: int, filename: str):
    print(f'Processing strategy: {processing_strategy}')
    flattener = Utils.get_strategy(processing_strategy)
    PROCCESSING = False
    json_list = []
    thread_lock = threading.Lock()

    def socket_test_processing(json_list: list) -> None:
        total = 0
        while PROCCESSING or json_list:
            print(f"Elements to proccess: {len(json_list)}")
            if not json_list:
                print("wating for data...")
                time.sleep(2)
            else:
                elems = len(json_list)
                flattener.do_put("TestDataset", json_list[:elems])
                with thread_lock:
                    del json_list[:elems]
                total += elems
        print(f"Proccessed elems: {total}")

    flattener_server_tread = threading.Thread(target=flattener.serve, args=(server_port,))
    flattener_server_tread.start()
    time.sleep(1)  # Wait for server to start

    host = '127.0.0.1'
    buffer_size = 2 ** 32
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, socket_port))
        s.listen(1)
        print(f'Listen on {socket_port}...')

        conn, addr = s.accept()
        print(f'Connected with {addr}')

        statisticker.start_measuring_time()
        monitor_thread = threading.Thread(target=statisticker.start_monitoring, args=(f"tests/cpu-memory/{filename}",))
        monitor_thread.start()

        try:
            processing_thread = threading.Thread(target=socket_test_processing, args=(json_list,))

            while True:
                if not PROCCESSING and json_list:
                    PROCCESSING = True
                    processing_thread.start()

                data = conn.recv(buffer_size)
                if not data:
                    break

                stringdata = data.decode('utf-8')
                json = list(filter(None, stringdata.split(">>>")))
                with thread_lock:
                    json_list.extend(json[1:-1])
        finally:
            print("Data capture completed")
            PROCCESSING = False
            processing_thread.join()
            statisticker.stop_monitoring()
            statisticker.stop_measuring_time(f"tests/time/{'-'.join(filename.split('---')[:-1])}")

            conn.close()
            flattener.server.stop()
            monitor_thread.join()
            flattener_server_tread.join()
            print("Test finished successfully")
