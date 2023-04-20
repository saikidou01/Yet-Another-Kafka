import asyncio
import json
import time
HOST="127.0.0.1"
PORT = 7779
async def run_client()->None:
    reader,writer=await asyncio.open_connection(HOST,PORT)
    topic = input("write your topic name: ")  # define a variable to store input in

    message={
        'topic':topic,
    }

    writer.write(json.dumps(message, ensure_ascii=False).encode("gbk"))
    await writer.drain()
    while True:
        data = await reader.read(1024)
        msg = data.decode()
        addr, port = writer.get_extra_info("peername")
        print(addr)
        print(port)
        a = json.loads(msg)
        # print(a)
        for i in a:
            print(rf"{i}")
        # if not data:
        #     raise Exception("socket closed")
        # print(f"received:{data.decode()!r}")


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    loop.run_until_complete(run_client())