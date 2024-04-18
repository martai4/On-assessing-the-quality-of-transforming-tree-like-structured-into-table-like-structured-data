# python -m  uvicorn api:app --reload --port 8000

import socket, time, asyncio
from json import loads
from fastapi import FastAPI

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/open-port/{port}")
async def open_port(port: int) -> str:
    asyncio.create_task(test(port))
    time.sleep(1)  # Wait for the socket to be set

    return "OK"


async def test(port: int):
    print("test")
    host = '127.0.0.1'
    buffer_size = 256 * 1024

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

                for json in json_list:
                    try:
                        parsed_data = loads(json)
                        print(f'Parsed data: {parsed_data}')
                    except:
                        print('Something is wrong with json!')

                time.sleep(2)
                print("------------------------")
        finally:
            conn.close()
            print("Test finished successfully")
