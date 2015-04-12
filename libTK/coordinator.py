import json, yaml
from libTK import *
import threading, socket
from libTK import comm
from libTK import settings

class Coordinator():
    """
        Maintains the global top-k set
        Receives any new messages, and calculates if any updates are necessary

    """

    def __init__(self, k, nodeport):
        """ Receives number of nodes.
            Looks up based on hostname to find all ip addresses
            Stores hostname 
            Contacts all nodes, gets all current object counts
        """

        out.info("Instantiating coordinator class.\n")
     

        self.dataLock = threading.Lock() 

        # READ FILE to get hostnames, ips 
        self.dataLock.acquire()     
        self.ips = yaml.load(open(settings.FILE_SIMULATION_IPS, 'r'))

        self.nodes = self.ips['nodes']
        
        print self.nodes
        self.nodeport = nodeport
        self.k = k

        # TODO: change to input from program
        self.epsilon = 0
        
        # Start process to get initial top k values, this will be used to set thresholds at each node
        # Responses must be handled asynchronously
        out.info("Sending requests for all data to each node for initial top-k computation.\n")
        for hn, node in self.nodes.iteritems():
            node['vals'] = {}
            node['waiting'] = True
            
            msg = {"msgType": settings.MSG_GET_OBJECT_COUNTS, "hn": hn}
            comm.send_msg((node['ip'], self.nodeport), msg)
        
        self.dataLock.release()     

        print(self.nodes)

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
        self.dataLock.acquire()     
        self.nodes[hn]['vals'] = data
        self.nodes[hn]['waiting'] = False

        nodes = copy.deepcopy(self.nodes)
        self.dataLock.release()

        for hn, node in nodes.iteritems():
            if (node['waiting']):
                return

        # If we get here, all nodes have received their data
        aggregated = self.aggregateInitTopK(nodes)
        
        sortedVals = self.sortVals(aggregated)


    def aggregateInitTopK(self, nodes):
        totalVals = {}

        for hn, node in nodes.iteritems():
            for key, val in node['vals'].iteritems():
                if key in totalVals:
                    totalVals[key] += val
                else:
                    totalVals[key] = val

        return totalVals
    
    def sortVals(self, vals):
        """ 
            Expects a dictionary of d[key] = value
            Returns a sorted array of (key, value) tuples
        """

        sortedVals = sorted(vals.items(), key=operator.itemgetter(1), reverse=True)
