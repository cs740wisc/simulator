import json, yaml
from libTK import *
import threading, socket
from libTK import comm
from libTK import settings
import time


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
        self.ips = yaml.load(open(settings.FILE_SIMULATION_IPS, 'r'))



        self.nodes = self.ips['nodes']
        
        self.nodeport = nodeport
        self.k = k

        self.topk = []

        # TODO: change to input from program
        self.epsilon = 0


        # Contacts all nodes, performs resolution, sets up initial parameters       
        self.getInitialVals()


 
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
            
            self.setObjectStats(hn, data['data'])
        elif (msgType == 'getVals'):
            # Request to get all current values at this node
            # Simply send to the coordinator
            self.getVals(hn, requestSock)

    def getInitialVals(self)
            

        # TODO add resolution lock, only one resolution can occur at once
        # TODO separate these into functions, make it so initial top k query is the same as later queries

        # Start process to get initial top k values, this will be used to set thresholds at each node
        # Responses must be handled asynchronously
        out.info("Sending requests for all data to each node for initial top-k computation.\n")
        F_node = (1.0 - self.F_coord) / len(self.nodes) 
        for hn, node in self.nodes.iteritems():
            node['vals'] = {}
            node['params'] = {}
            node['border'] = 0
            node['F'] = F_node

            node['waiting'] = True
            
            msg = {"msgType": settings.MSG_GET_OBJECT_COUNTS, "hn": hn}
            comm.send_msg((node['ip'], self.nodeport), msg)

        
        # Will wait until all nodes have values
        self.waitForResponses()        

        # 
        self.dataLock.acquire()     
        nodes = copy.deepcopy(self.nodes)
        self.dataLock.release()


        leewayVals = calculateLeewayVals(participatingSum, borderSum)


        participatingNodes = self.nodes.keys()

        # Will assign adjustment factors to nodes, 
        performReallocation(sortedVals)
        setTopK(sortVals)
        # All nodes have values, now aggregate them
        
        # TODO add resolution release, only one resolution can occur at once
        

    def getParticipatingCountsAndBorder(self, nodes):
        participatingSum = {}
        borderSum = 0
        aggregateSum = {}


        
        for hn, node in nodes.iteritems():
            borderSum += node['border']
            for key, info in node['partials'].iteritems():
                if key in participatingSum:
                    participatingSum[key] += info['val'] + info['param']
                    aggregateSum[key] += info['val']
                else:
                    participatingSum[key] = info['val'] + info['param']
                    aggregateSum[key] = info['val']

        # TODO add the max of the adjustment params at the coordinator not in resolution set
        borderSum += 0


        ###################################################
        # SORT TO GET TOP K
        sortedVals = self.sortVals(aggregateSum)
        topk = sortedVals[0:self.k]
        participatingObjects = [a[0] for a in sortedVals]
        topKObjects = [a[0] for a in topk]

        ####################################################
        # CALCULATE LEEWAY
        leeway = {}
        
        for o in participatingObjects:
            # If the object is in the top k set, we need to include epsilon
            leeway[o] = participatingSum[o] - borderSum[o] + epsilon
            if (o in topKObjects): 
                leeway[o] += self.epsilon



        adjustFactors = {}
        #####################################################
        # ASSIGN ADJUSTMENT FACTORS
        for hn, node in nodes.iteritems():
            adjustFactors[hn] = {}
            for o in participatingObjects:
                adjustFactors[hn][o] = node['border'] - node['partials'][o]['val'] + node['F']*leeway[o]

        
        #####################################################
        # ASSIGN ADJUSTMENT FACTORS FOR COORDINATOR
        coordAdjFactors = {}
        for o in participatingObjects:
            coordAdjFactors[o] = node['border'] - node['partials'][o]['val'] + node['F']*leeway[o]
            if (o in topKObjects):
                coordAdjFactors[o] -= epsilon
                

        
        # TODO Notify all nodes of the new top k set, their adjustment factors






    def reallocate(self, topk, resSet, borderVals, partialVals, partialParams, allocParams):
    
        # Calculate leeway
        leeway = {}
        
        # Iterate over all objects
        for o in resSet:
                

        
        borderCoord = 0


    def waitForResponses(self):
        """
            waits until all nodes have responded with their partial values.

        """
        waiting = False

        while (True):
            # Iterate through nodes, make sure all have responded
            for hn, node in nodes.iteritems():
                if (node['waiting']):
                    waiting = True

            # If we are no longer waiting on any nodes return
            if (not waiting):
                return

            # Sleep a little so we don't waste cycles        
            time.sleep(0.5)                




    def setObjectStats(self, hn, data):
        """
            Updates the initial values at node hn. 
            Check if all nodes have sent their response. 
            If so, initialize aggregation of all
        """
        self.dataLock.acquire()     
        self.nodes[hn]['vals'] = data
        self.nodes[hn]['waiting'] = False
        self.dataLock.release()



    def setTopK(sortVals):

        # Set the top k value, only keep the top however if not enough objs
        if (len(sortVals) < self.k):
            self.topk = sortVals
        else:
            self.topk = sortVals[0:self.k]



    def requestCurrentVals(self, objects, nodes)
    """
        Requests the current partial values from Nj
        nodes: Nj, nodes involved in resolution set
        objects: Oi, objects in resolution
        
    """
    
    def sortVals(self, vals):
        """ 
            Expects a dictionary of d[key] = value
            Returns a sorted array of (key, value) tuples
        """

        sortedVals = sorted(vals.items(), key=operator.itemgetter(1), reverse=True)

