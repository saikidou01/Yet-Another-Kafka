import os
import socket
import time

def check(host,port,timeout=1):
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM) #presumably
    sock.settimeout(timeout)
    try:
       sock.connect((host,port))
    except:
       return False
    else:
       sock.close()

       return True

def zoo():
    while True:

        if check('127.0.0.1',8888):
            # print("producer beat")
            return False
        else:
            return True

