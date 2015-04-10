from libTK import *
import threading, socket
from libTK import comm
from libTK import settings

class NodeCoordinator():
    """
        Maintains the per node values
        Receives any new messages, and calculates if any updates are necessary

    """


    def __init__(self, master_address):
        out.info("Instantiating node coordinator class.\n")
        self.vals = {"a": 1, "b": 2}

        self.master_address = master_address

        # TODO need to figure out hostname

    def receivedData(self, requestSock, data):
        """ 
            Listens for data from the coordinator or generator. Handles appropriately.
        """
        out.info("Node received message: %s\n" % data)
        msgType = data['msgType']
        hn = data['hn']
        srcIP = requestSock.getpeername()[0]


        if   (msgType == settings.MSG_REQUEST_DATA):
            # From generator, request for a specific node should increment value by 1.
            pass
        elif (msgType == settings.MSG_GET_OBJECT_COUNTS):
            # Request to get all current values at this node
            # Simply send to the coordinator
            out.info("Returning current object counts to coordinator.\n" % data)
            self.getVals(hn, srcIP)
        

    def getVals(self, hn, srcIP):

        addr = (srcIP, settings.RECV_PORT)
        msg = {'msgType': settings.MSG_GET_OBJECT_COUNTS_RESPONSE, 'data': self.vals, 'hn': hn}

        comm.send_msg(addr, msg) 




