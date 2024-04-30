# python -m uvicorn api:app --reload --port 8000

import socket, time, asyncio
from json import loads
from fastapi import FastAPI
import threading

import Utils
from methods.JSONFirstListFlattener import JSONFirstListFlattener
from methods.JSONPathFlattener import JSONPathFlattener
from methods.JSONListToTableConverter import JSONListToTableConverter
from methods.FlattenJSON import FlattenJSON

app = FastAPI()

monitor_thread = threading.Thread(target=Utils.monitor_usage)
monitor_thread.start()

@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/socket-test/{socketPort}/{serverPort}")
async def socket_test(socketPort: int, serverPort: int) -> str:
    asyncio.create_task(socket_test_task(socketPort, serverPort))
    time.sleep(1)  # Wait for the socket to be set

    return "OK"


async def socket_test_task(socket_port: int, server_port: int):
    host = '127.0.0.1'
    buffer_size = 256 * 1024

    flattener = JSONPathFlattener() # TODO make it dynamic
    flattener_server_tread = threading.Thread(target=flattener.serve, args=(server_port,))
    flattener_server_tread.start()
    time.sleep(1) # Wait for server to start

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

                stringdata = data.decode('utf-8')
                json = list(filter(None, stringdata.split(">>>")))
                flattener.do_put("test", json) # TODO change name of dataset
        finally:
            conn.close()
            flattener.server.stop()
            flattener_server_tread.join()
            print("Test finished successfully")
