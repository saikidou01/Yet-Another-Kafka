import asyncio
import json
import os
HOST="127.0.0.1"
PORT=7779
import glob
import os
from itertools import chain


async def handle_echo_1(reader: asyncio.StreamReader,writer: asyncio.StreamWriter)->None:
    data=None
    passed=False


    while data !=b"quit":
        data =await reader.read(1024)
        msg=data.decode()
        addr,port=writer.get_extra_info("peername")
        b=json.loads(msg)

        topic=b['topic']
        dir_paths = rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage1"
        isExist_broker = os.path.exists(dir_paths)
        if isExist_broker:
            os.chdir(rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage1\{topic}")
            my_files = glob.glob('*.txt')
            #print(my_files)
            lines=[]
            lines.append([" from broker1:"])
            # print("broker2")

            for i in my_files:
                with open(rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage1\{topic}\{i}") as f:
                    # print(list(line for line in f))
                     lines.append([line.rstrip('\n') for line in f])
            print(lines)
            flatten_list = list(chain.from_iterable(lines))
            print(flatten_list)
            writer.write(json.dumps(flatten_list, ensure_ascii=False).encode("gbk"))
    # writer.close()
    # await writer.wait_closed()

        else:
            os.chdir(rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage2\{topic}")
            my_files = glob.glob('*.txt')
            # print(my_files)
            lines = []
            lines.append([" from broker2:"])

            for i in my_files:
                with open(rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage2\{topic}\{i}") as f:
                    # print(list(line for line in f))
                    lines.append([line.rstrip('\n') for line in f])
            print(lines)
            flatten_list = list(chain.from_iterable(lines))
            print(flatten_list)
            writer.write(json.dumps(flatten_list, ensure_ascii=False).encode("gbk"))
    writer.close()
    await writer.wait_closed()




async def run_server()->None:
    server= await asyncio.start_server(handle_echo_1,HOST,PORT)
    # print("in1")
    async with server:
        await server.serve_forever()



if __name__ =="__main__":
    loop=asyncio.new_event_loop()
    loop.run_until_complete(run_server())
    # loop.run_until_complete(run_server2())
