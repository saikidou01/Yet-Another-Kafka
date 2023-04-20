import asyncio
import json
import time
HOST="127.0.0.1"
PORT=8888

async def run_client()->None:
    reader,writer=await asyncio.open_connection(HOST,PORT)
    topic = input("write your topic name: ")  # define a variable to store input in
    value = input("write your data : ")  # define a variable to store input in

    message={
        'topic':topic,
        #'key':'1',
        'value':value
}


    writer.write(json.dumps(message,ensure_ascii=False).encode("gbk"))
    await writer.drain()
    while True:
        data=await reader.read(1024)

        if not data:
            raise Exception("soclet closed")
        print(f"received:{data.decode()!r}")

if __name__ =="__main__":
    loop=asyncio.new_event_loop()
    loop.run_until_complete(run_client())

