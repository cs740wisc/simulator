import json, yaml
from libTK import *
import threading, socket, time
from libTK import comm
from libTK import settings

class Generator():
    """
        Generators values to the nodes which will store per-object counts
    """

    def __init__(self, nodeip, nodeport, sendtime):
        """ 
            Sends requests to its node every sendtime seconds.
        """

        out.info("Instantiating generator class.\n")

        self.nodeip = nodeip
        self.nodeport = nodeport
        self.sendtime = sendtime
        
        self.run = False

    def stop(self):
        self.run = False

    def start(self):
        self.run = True

    def send_data(self):
        while (self.run):
            time.sleep(self.sendtime)
            msg_thread = threading.Thread(target=self.gen_message)
            msg_thread.start()

    def gen_message(self):
        import random
        import string

        obj = random.choice(string.ascii_lowercase)
        msg = {'msgType': settings.MSG_REQUEST_DATA, 'object': obj}


        comm.send_msg((self.nodeip, self.nodeport), msg)

    def receivedData(self, recvSock, data):

        out.info("Generator received mesage: %s\n" % data)

