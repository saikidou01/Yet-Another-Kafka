import asyncio
import json
import os
import signal
import time
import glob
import os
import shutil
from itertools import chain
from zookeeper import zoo


# def handler(signum, frame):
#     res = input("Ctrl-c was pressed. Do you really want to exit? y/n ")
#     if res == 'y':
#         exit(1)
#
#
# signal.signal(signal.SIGINT, handler)
#
# count = 0
# while True:
#     print(count)
#     count += 1
#     time.sleep(0.1)
HOST="127.0.0.1"
PORT=8888
flag = zoo()
if flag == False:
        shutil.rmtree(rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage1")
from broker_function import broker1,broker2
async def handle_echo(reader: asyncio.StreamReader,writer: asyncio.StreamWriter)->None:
    data=None
    passed=False

    # flag=True
    # def handler(signum, frame):
    #     flag = False
    #
    # signal.signal(signal.SIGINT, handler)
    # try:
    #     flag=True
    # except KeyboardInterrupt:
    #     flag=False
    flag1=True
    flag2=True

    while data !=b"quit":
        # try:
        #     flag = True
        # except KeyboardInterrupt:
        #     flag = False

        data =await reader.read(1024)
        msg=data.decode()
        addr,port=writer.get_extra_info("peername")
        a=json.loads(msg)
        print(a)
        dir_paths = rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage1"
        isExist_broker = os.path.exists(dir_paths)
        if isExist_broker:

            dir_path = rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage1\{a['topic']}"
            #val=len(os.listdir(dir_path))
            isExist = os.path.exists(dir_path)
            # with open(r"E:\demos\files\read_demo.txt", 'r') as fp:
            #     x = len(fp.readlines())
            if  not isExist:

                # part=partitioner(0,a['topic'])
                broker1(a['topic'], a['value'])
                passed = True

            else:
                # dir_path = rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage1\{a['topic']}"
                # val1 = len(os.listdir(dir_path))
                broker1(a['topic'], a['value'])
                passed = True
        else:
            dir_path = rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage2\{a['topic']}"
            # val=len(os.listdir(dir_path))
            isExist = os.path.exists(dir_path)
            # with open(r"E:\demos\files\read_demo.txt", 'r') as fp:
            #     x = len(fp.readlines())
            if not isExist:

                # part=partitioner(0,a['topic'])
                broker2(a['topic'], a['value'])
                passed = True

            else:
                # dir_path = rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage1\{a['topic']}"
                # val1 = len(os.listdir(dir_path))
                broker2(a['topic'], a['value'])
                passed = True

        #print(val)
        #part=partitioner(val,a['topic'])

        # if val>2:
        #     print("broker fail!!!")
        #     #make broker2 the leader
        #     broker2(a['topic'],a['key'],a['value'])
        #     passed=True
        # else:
        # broker1(a['topic'],part,a['value'])
        # passed=True

        if passed==True:
            writer.write(b"Ack FROM THE SERVER")
            await writer.drain()
        else:
            writer.write(b"there is a problem")
            await writer.drain()
        # writer.write(data)
        # await writer.drain()
    writer.close()
    await writer.wait_closed()

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
            print("broker2")

            for i in my_files:
                with open(rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage1\{topic}\{i}") as f:
                    # print(list(line for line in f))
                     lines.append([line.rstrip('\n') for line in f])
            print(lines)
            flatten_list = list(chain.from_iterable(lines))
            print(flatten_list)
            writer.write(json.dumps(flatten_list, ensure_ascii=False).encode("gbk"))

        else:
            os.chdir(rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage2\{topic}")
            my_files = glob.glob('*.txt')
            # print(my_files)
            print("broker2")
            lines = []
            lines.append([" from broker2:"])
            lines.append([" from broker1:"])

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


        await writer.drain()
        while True:
            data = await reader.read(1024)

        # writer.close()
        # await writer.wait_closed()

# async def run_server()->None:
#     try:
#         server= await asyncio.start_server(handle_echo,HOST,PORT)
#         async with server:
#             await server.serve_forever()
#     except KeyboardInterrupt:
#         print("Received exit, exiting")
# async def run_server2()->None:
#     server= await asyncio.start_server(handle_echo,HOST,9998)
#     print("in2")
#     async with server:
#         await server.serve_forever()
# def partitioner(val,topic):
#   if val==0:
#       partition =topic+f'_partition_{val+1}'
#   else:
#       partition = topic + f'_partition_{val + 1}'
#
#   return partition

# def handler(signum, frame):
#     flag=True
#
#
# signal.signal(signal.SIGINT, handler)

# def sigint_handler(flag):
#     return flag=False

async def main():


    # flag=signal.signal(signal.SIGINT, sigint_handler)


    server1 = await asyncio.start_server(
        handle_echo, '127.0.0.1', 8888)

    # addr1 = server1.sockets[0].getsockname()
    # print(f'Serving 1 on {addr1}')

    server2 = await asyncio.start_server(
        handle_echo_1, '127.0.0.1', 8889)

    # addr2 = server2.sockets[0].getsockname()
    # print(f'Serving 2 on {addr2}')

    async with server1, server2:
        await asyncio.gather(
            server1.serve_forever(), server2.serve_forever())

asyncio.run(main())

# if __name__ =="__main__":
#     loop=asyncio.new_event_loop()
#     loop.run_until_complete(run_server())
#     loop.run_until_complete(run_server2())
