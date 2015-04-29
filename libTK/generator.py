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




 
    def gen_message(self):
        import random
        import string

        obj = random.choice(string.ascii_lowercase)
        msg = {'msgType': settings.MSG_REQUEST_DATA, 'object': obj}

        comm.send_msg((self.nodeip, self.nodeport), msg)

    def receivedData(self, recvSock, data):

        out.info("Generator received mesage: %s\n" % data)

        msgType = data['msgType']
        hn = data['hn']

        if (msgType == settings.MSG_START_GEN):
            self.startGen()
        elif (msgType == settings.MSG_STOP_GEN):
            self.stopGen()
