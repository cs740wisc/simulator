import sys, json, argparse, copy, signal, time, os
sys.path.append('/home/mininet/simulator/')

from libTK import out, logPrefix, timeint
from libTK.utils import tkutils as tkutil
from libTK import *
from libTK import settings
from libTK import comm
from libTK import generator

import socket, threading, SocketServer


class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        # Receive the data, parse into json object
        data = str2json(self.request.recv(1024))

        # Pass off to coord object so it can check for violations
        self.server.generator.receivedData(self.request, data) 

        
class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):


    def __init__(self, server_address, handler_class, generator):
        """
        Keep track of generator object which will handle all calculations
        """
        self.allow_reuse_address = True
        self.generator = generator
        SocketServer.TCPServer.__init__(self, server_address, handler_class)


def setupArgParse():
    p = argparse.ArgumentParser(description='Daemon for ParaDrop Framework Control Configuration server')
    p.add_argument('-p', '--nodeport', help='Port to send to', type=int, default=10000)
    p.add_argument('-i', '--nodeip', help='IP Address to send to', type=str, default='localhost')
    p.add_argument('-t', '--time', help='How often to send requests', type=int, default=5)
    p.add_argument('-g', '--genip', help='IP Address to listen on', type=str, default='localhost')
    p.add_argument('-r', '--genport', help='Port to listen on ', type=int, default=12000)

    return p


if (__name__ == "__main__"):

    p = setupArgParse()
    args = p.parse_args()
    

    out.info('-- %s Starting genData: port: %s ipaddr: %s time: %s\n' % (logPrefix(), args.nodeport, args.nodeip, args.time))


    # Set up the thread which will send data to a node
    tkgen = generator.Generator(args.nodeip, args.nodeport, args.time)

    # Start a thread which will listen for stop requests
    server = ThreadedTCPServer((args.genip, args.genport), ThreadedTCPRequestHandler, tkgen)
    ip, port = server.server_address

    # Start a thread with the server -- that thread will then start one
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.start() 

    # Listen for kill signal, shutdown everything
    try:
        while (True):
            pass 
    except KeyboardInterrupt:
        server_thread.stop()
        tkgen.stopGen()
