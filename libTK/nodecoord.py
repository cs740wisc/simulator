from libTK import *
import threading, socket
from libTK import comm

class NodeCoordinator():
    """
        Maintains the per node values
        Receives any new messages, and calculates if any updates are necessary

    """


    def __init__(self, master_address):
        out.info("Instantiating node coordinator class.\n")
        self.vals = {"a": 1, "b": 2}

        self.master_address = master_address




    def receivedData(self, data):
        """ Listens to see if any violations have occurred. Begins reassignment process if necessary.
        """
        out.info("Server Received Message: %s\n" % data)
        msgType = data['msgType']

        if (data['msgType'] == 'request'):
            pass
        elif (data['msgType'] == 'getVals'):
            comm.send_msg(self.master_address, self.vals) 






