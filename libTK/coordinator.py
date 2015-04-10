
from libTK import *
import threading, socket
from libTK import comm
from libTK import settings

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

        out.info("Instantiating coordinator class.\n")
       
        self.k = k
        self.nodes = {}

        # Generate IPs to contact
        for i in range(1,numNodes+1):
            host = "h%s" % i
            ip = "10.0.0.%s" % 2*i
            self.nodes[host] = {"ip": ip, "vals": {}, "waiting": True}

        # Start process to get initial top k values, this will be used to set thresholds at each node
        # Responses must be handled asynchronously
        out.info("Sending requests for all data to each node for initial top-k computation.\n")
        for hn, node in self.nodes.iteritems():
            msg = {"msgType": settings.MSG_GET_OBJECT_COUNTS, "hn": hn}
            comm.send_msg((node['ip'], 10000), msg)


    def receivedData(self, requestSock, data):
        """ 
            Listens for any response messages. They are detailed below.
            Each response contains the name of the node for each lookup.


            getValsResponse:       

 
        """
        out.info("Coordinator Received Message: %s\n" % data)

        msgType = data['msgType']
        hn = data['hn']


        if   (msgType == settings.MSG_GET_OBJECT_COUNTS_RESPONSE):
            # From generator, request for a specific node should increment value by 1.
            
            self.initObjectCountResponse(hn, data['data'])
        elif (msgType == 'getVals'):
            # Request to get all current values at this node
            # Simply send to the coordinator
            self.getVals(hn, requestSock)


    def initObjectCountResponse(self, hn, data):
        """
            Updates the initial values at node hn. 
            Check if all nodes have sent their response. 
            If so, initialize aggregation of all
        """
        
        self.nodes[hn]['vals'] = data
        self.nodes[hn]['waiting'] = False


        for hn, node in self.nodes.iteritems():
            if (node['waiting']):
                return

        # If we get here, all nodes have received their data
        self.aggregateInitTopK()


    def aggregateInitTopK(self):
        totalVals = {}


        for hn, node in self.nodes.iteritems():
            print(node)
            for key, val in node['vals'].iteritems():
                if key in totalVals:
                    totalVals[key] += val
                else:
                    totalVals[key] = val


        print totalVals
        
