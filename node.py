import sys, json, argparse, copy, signal, time, os
sys.path.append('/home/mininet/simulator/')

import socket, threading, SocketServer
from libTK import *
from libTK import settings
from libTK import nodecoord
from libTK import comm


class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        data = str2json(self.request.recv(1024))

        self.server.node_coord.receivedData(self.request, data)


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):

    def __init__(self, server_address, handler_class, node_coord, master_address):
        self.allow_reuse_address = True
        self.numRequests = 0
        self.node_coord = node_coord
        SocketServer.TCPServer.__init__(self, server_address, handler_class)
        


def setupArgParse():
    p = argparse.ArgumentParser(description='Daemon for ParaDrop Framework Control Configuration server')
    p.add_argument('-p', '--port', help='Port to listen on', type=int, default=10000)
    p.add_argument('-i', '--host', help='Host to listen on', type=str, default='localhost')
    p.add_argument('-m', '--masterip', help='IP of the master node which coordinates everything.', type=str, default='localhost')
    p.add_argument('-n', '--masterport', help='Port of the master node.', type=int, default=11000)

    return p


if (__name__ == "__main__"):

    p = setupArgParse()
    args = p.parse_args()
    
    out.info('-- %s Starting genData: port: %s ipaddr: %s\n' % (logPrefix(), args.port, args.host))
    # Port 0 means to select an arbitrary unused port


    node_coord = nodecoord.NodeCoordinator((args.masterip, args.masterport))

    server = ThreadedTCPServer((args.host, args.port), ThreadedTCPRequestHandler, node_coord, (args.masterip, args.masterport))
    ip, port = server.server_address

    # Start a thread with the server -- that thread will then start one
    # more thread for each request
    server_thread = threading.Thread(target=server.serve_forever)
    # Exit the server thread when the main thread terminates
    #server_thread.daemon = True
    server_thread.start() 


    # Start client if necessary

    # Listen for kill signal, shutdown everything
    try:
        while (True):
            pass 
    except KeyboardInterrupt:
        server.shutdown()

    
