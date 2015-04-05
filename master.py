import sys, json, argparse, copy, signal, time, os
sys.path.append('/home/mininet/simulator/')

import socket, threading, SocketServer
from libTK import *
from libTK import settings
from libTK import coordinator
from libTK import comm



class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        # Receive the data, parse into json object
        data = str2json(self.request.recv(1024))

        # Pass off to coord object so it can check for violations
        self.server.coord.receivedData(data) 

        
class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):


    def __init__(self, server_address, handler_class, coord):
        """
        Keep track of coord object which will handle all calculations
        """
        self.allow_reuse_address = True
        self.coord = coord
        SocketServer.TCPServer.__init__(self, server_address, handler_class)









def setupArgParse():
    p = argparse.ArgumentParser(description='Daemon for ParaDrop Framework Control Configuration server')
    p.add_argument('-p', '--port', help='Port to listen on', type=int, default=11000)
    p.add_argument('-i', '--host', help='Host to listen on', type=str, default='localhost')
    p.add_argument('-n', '--numnodes', help='Number of nodes collecting data', type=int, default=1)
    p.add_argument('-k', '--topk', help='K objects to get', type=int, default=1)
    return p


if (__name__ == "__main__"):

    p = setupArgParse()
    args = p.parse_args()
    
    out.info('-- %s Starting master node: port: %s ipaddr: %s\n' % (logPrefix(), args.port, args.host))
    # Port 0 means to select an arbitrary unused port

    # Get current top-k queries
    coord = coordinator.Coordinator(args.numnodes, args.topk)
    
    server = ThreadedTCPServer((args.host, args.port), ThreadedTCPRequestHandler, coord)
    ip, port = server.server_address




    # Start a thread with the server -- that thread will then start one
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.start() 


    # Start client if necessary

    # Listen for kill signal, shutdown everything
    try:
        while (True):
            pass 
    except KeyboardInterrupt:
        server.shutdown()

    
