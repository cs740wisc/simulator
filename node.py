import sys, json, argparse, copy, signal, time, os
sys.path.append('/home/mininet/simulator/')

import socket, threading, SocketServer
from libTK import *
from libTK import settings


class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):


    def handle(self):
        data = self.request.recv(1024)
        data = str2json(data)
        cur_thread = threading.current_thread()

        out.info("Server Received: %s\n" % data)
        response =  json2str(data)
        self.request.sendall(response)

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    allow_reuse_address = True


def setupArgParse():
    p = argparse.ArgumentParser(description='Daemon for ParaDrop Framework Control Configuration server')
    p.add_argument('-p', '--port', help='Port to listen on', type=int, default=10000)
    p.add_argument('-i', '--host', help='Host to listen on', type=str, default='localhost')

    return p


if (__name__ == "__main__"):

    p = setupArgParse()
    args = p.parse_args()
    
    out.info('-- %s Starting genData: port: %s ipaddr: %s\n' % (logPrefix(), args.port, args.host))
    # Port 0 means to select an arbitrary unused port

    server = ThreadedTCPServer((args.host, args.port), ThreadedTCPRequestHandler)
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

    
