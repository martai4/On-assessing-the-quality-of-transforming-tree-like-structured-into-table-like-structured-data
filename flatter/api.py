# python -m uvicorn api:app --reload --port 8000

import socket, time, asyncio
from fastapi import FastAPI
import threading

from Statisticker import Statisticker
from methods.JSONFirstListFlattener import JSONFirstListFlattener
from methods.JSONPathFlattener import JSONPathFlattener
from methods.JSONListToTableConverter import JSONListToTableConverter
from methods.FlattenJSON import FlattenJSON

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/socket-test/{socket_port}/{server_port}")
async def socket_test(socket_port: int, server_port: int) -> str:
    asyncio.create_task(socket_test_task(socket_port, server_port))
    time.sleep(1)  # Wait for the socket to be set

    return "OK"

async def socket_test_task(socket_port: int, server_port: int):
    host = '127.0.0.1'
    buffer_size = 2 ** 32  # TODO check limits of buffer size

    flattener = JSONFirstListFlattener()  # TODO make it dynamic
    flattener_server_tread = threading.Thread(target=flattener.serve, args=(server_port,))
    flattener_server_tread.start()
    time.sleep(1)  # Wait for server to start

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, socket_port))
        s.listen(1)
        print(f'Listen on {socket_port}...')

        conn, addr = s.accept()
        print(f'Connected with {addr}')

        # thread_list = []
        try:
            while True:
                data = conn.recv(buffer_size)
                if not data:
                    break

                stringdata = data.decode('utf-8')
                json = list(filter(None, stringdata.split(">>>")))
                flattener.do_put("test", json) # TODO change dataset name

                # new_tread = threading.Thread(target=flattener.do_put, args=("test", json))
                # new_tread.start()
                # thread_list.append(new_tread)
        finally:
            conn.close()
            # [tread.join() for tread in thread_list]
            flattener.server.stop()
            flattener_server_tread.join()
            print("Test finished successfully")
