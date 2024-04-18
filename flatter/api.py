# python -m  uvicorn api:app --reload --port 8000

import socket, time, asyncio
from json import loads
from fastapi import FastAPI
import threading

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
    asyncio.create_task(socket_test_temp(port))
    time.sleep(1)  # Wait for the socket to be set

    return "OK"

async def socket_test_temp(port: int):
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

@app.post("/thread-test")
async def tread_test():
    json_file_paths = ['../data/airlines.json',
                       '../data/gists.json',
                       '../data/movies.json',
                       '../data/reddit.json',
                       '../data/nasa.json']


    json_path_flattener = JSONPathFlattener()
    json_path_flattener_thread = threading.Thread(target=json_path_flattener.serve, args=(json_file_paths, 50051))
    json_path_flattener_thread.start()

    json_first_list_flattener = JSONFirstListFlattener()
    json_first_list_flattener_thread = threading.Thread(target=json_first_list_flattener.serve, args=(json_file_paths, 50052))
    json_first_list_flattener_thread.start()

    json_list_to_table_converter = JSONListToTableConverter()
    json_list_to_table_converter_thread = threading.Thread(target=json_list_to_table_converter.serve, args=(json_file_paths, 50053))
    json_list_to_table_converter_thread.start()

    flatten_json = FlattenJSON()
    flatten_json_thread = threading.Thread(target=flatten_json.serve, args=(json_file_paths, 50054))
    flatten_json_thread.start()

    json_path_flattener_thread.join()
    json_first_list_flattener_thread.join()
    json_list_to_table_converter_thread.join()
    flatten_json_thread.join()
