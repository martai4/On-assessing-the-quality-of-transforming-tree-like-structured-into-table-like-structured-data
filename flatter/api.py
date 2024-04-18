# python -m uvicorn api:app --reload --port 8000

import socket, time, asyncio
from json import loads
from fastapi import FastAPI

from methods.JSONFirstListFlattener import JSONFirstListFlattener
from methods.JSONPathFlattener import JSONPathFlattener
from methods.JSONListToTableConverter import JSONListToTableConverter
from methods.FlattenJSON import FlattenJSON

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/socket-test/{port}")
async def socket_test(port: int) -> str:
    asyncio.create_task(socket_test_task(port))
    time.sleep(1)  # Wait for the socket to be set

    return "OK"


async def socket_test_task(port: int):
    host = '127.0.0.1'
    buffer_size = 256 * 1024

    flattener = JSONPathFlattener() # todo make it dynamic

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        s.listen(1)
        print(f'Listen on {port}...')

        conn, addr = s.accept()
        print(f'Connected with {addr}')

        try:
            while True:
                data = conn.recv(buffer_size)
                if not data:
                    break

                stringdata = data.decode('utf-8')
                json_list = list(filter(None, stringdata.split(">>>")))
                flattener.do_put(json_list)

                time.sleep(2)
                print("------------------------")
        finally:
            conn.close()
            print("Test finished successfully")
