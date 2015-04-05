import sys, json, argparse, copy, signal, time, os
sys.path.append('/home/mininet/simulator/')

import libTK as libTK
from libTK import out, logPrefix, timeint
from libTK.utils import tkutils as tkutil
from libTK.comm.sender import TKSenderReconnect
from libTK import *
from libTK import settings


import socket, threading, SocketServer


class TKGenData:

    def __init__(self, ip, port, time):
        self.ip = ip
        self.port = port
        self.time = time
        self.sock = None
        self.run = True


    def stop(self):
        self.run = False
       
    def send_data(self):
        # Open a socket - this will stay open because we don't need to shutdown/create new sockets         

        while (self.run):
            time.sleep(self.time)
            # Sleep then start a thread to send data
            msg_thread = threading.Thread(target=self.send_message)
            msg_thread.start()
           
 

    def send_message(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.connect((self.ip, self.port))
            # Generate a random messsage, then send to server
            message = "test1"
            out.info("Sending message to server: %s\n" % message)
            m_str = json2str(message)
            self.sock.sendall(m_str)
            response = str2json(self.sock.recv(1024))
            out.info("Client Received: %s\n" % response)
        except Exception as e:
            out.info("Error: %s\n" % e)
        # Only get here once we want to stop
        self.sock.close()

def setupArgParse():
    p = argparse.ArgumentParser(description='Daemon for ParaDrop Framework Control Configuration server')
    p.add_argument('-s', '--settings', help='Setting string to overwrite from settings.py, format is "KEY:VALUE"', action='append', type=str)
    p.add_argument('-p', '--port', help='Port to send to', type=int, default=10000)
    p.add_argument('-i', '--ipaddr', help='IP Address to send to', type=str, default='localhost')
    p.add_argument('-t', '--time', help='How often to send requests', type=int, default=5)

    return p


if (__name__ == "__main__"):

    p = setupArgParse()
    args = p.parse_args()
    

    out.info('-- %s Starting genData: port: %s ipaddr: %s time: %s\n' % (logPrefix(), args.port, args.ipaddr, args.time))

    # Set up the thread which will send data to a node
    tkgen = TKGenData(args.ipaddr, args.port, args.time)
    gen_thread = threading.Thread(target=tkgen.send_data)

    #gen_thread.daemon = True
    gen_thread.start()
   

 
    # Listen for kill signal, shutdown everything
    try:
        while (True):
            pass 
    except KeyboardInterrupt:
        tkgen.stop()
