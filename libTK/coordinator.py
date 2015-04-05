
from libTK import *
import threading, socket
from libTK import comm

class Coordinator():
    """
        Maintains the global top-k set
        Receives any new messages, and calculates if any updates are necessary

    """


    def __init__(self, numNodes, k):
        """ Receives number of nodes.
            Looks up based on hostname to find all ip addresses
            Stores hostname 
            Contacts all nodes, gets all current object counts
        """

        self.k = k
        self.nodes = {}

        # Generate IPs to contact
        for i in range(1,numNodes+1):
            host = "h%s" % i
            ip = "10.0.0.%s" % 2*i
            self.nodes[host] = {"ip": ip, "vals": {}, "waiting": False}


        for hn, node in self.nodes.iteritems():
            msg = {"msgType": "getVals"}
            comm.send_msg((node['ip'], 10000), msg)

 

        out.info("Instantiating coordinator class.\n")



    def test(self, a):
        out.info("a: %s\n" % a)

    def receivedData(self, data):
        """ Listens to see if any violations have occurred. Begins reassignment process if necessary.
        """
        out.info("Server Received Message: %s\n" % data)




