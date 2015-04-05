
import socket, threading
from libTK import *



def send_msg(addr, msg):
    """ Creates a socket and sends message to the addr. """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.connect(addr)
        # Generate a random messsage, then send to server
        out.info("Sending message to %s port %s: %s\n" % (addr[0], addr[1], msg))
        sock.sendall(json2str(msg))
        # Only get here once we want to stop
        sock.close()
    except Exception as e:
        out.err("send_msg error: %s\n" % e)



