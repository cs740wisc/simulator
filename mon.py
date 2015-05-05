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
        self.node_coord = node_coord
        SocketServer.TCPServer.__init__(self, server_address, handler_class)
        


def setupArgParse():
    p = argparse.ArgumentParser(description='Daemon for ParaDrop Framework Control Configuration server')
    p.add_argument('-p', '--nodeport', help='Port to listen on', type=int, default=10000)
    p.add_argument('-i', '--nodeip', help='Host to listen on', type=str, default='localhost')
    p.add_argument('-m', '--masterip', help='IP of the master node which coordinates everything.', type=str, default='localhost')
    p.add_argument('-n', '--masterport', help='Port of the master node.', type=int, default=11000)
    p.add_argument('-s', '--hostname', help='Hostname of this host.', type=str, default='h1')
    p.add_argument('-t', '--testname', help='Name of the test to run.', type=str, default='same')

    return p


if (__name__ == "__main__"):

    p = setupArgParse()
    args = p.parse_args()
    

    node_coord = nodecoord.NodeCoordinator((args.masterip, args.masterport), args.hostname, args.testname)

    server = ThreadedTCPServer((args.nodeip, args.nodeport), ThreadedTCPRequestHandler, node_coord, (args.masterip, args.masterport))
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
        node_coord.stopGen()
        server.shutdown()

    