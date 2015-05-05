import json, yaml
from libTK import *
import threading, socket
from libTK import comm
from libTK import settings
import time
import copy
import operator


class Coordinator():
    """
        Maintains the global top-k set
        Receives any new messages, and calculates if any updates are necessary

    """

    def __init__(self, k, nodeport, testname):
        """ Receives number of nodes.
            Looks up based on hostname to find all ip addresses
            Stores hostname 
            Contacts all nodes, gets all current object counts
        """

        out.info("Instantiating coordinator class.\n")
     
        # READ FILE to get hostnames, ips 
        self.ips = yaml.load(open(settings.FILE_SIMULATION_IPS, 'r'))

        self.nodes = self.ips['nodes']
        
        self.nodeport = nodeport
        self.F_coord = 0.5
        # TODO: change to input from program
        self.epsilon = 0
        self.k = k




        self.topk = []

        self.dataLock = threading.Lock()
        self.resolveLock = threading.Lock()

        # Original thread needs to stay open to listen as server
        # Contacts all nodes, performs resolution, sets up initial parameters       
        performInit_thread = threading.Thread(target=self.sendStartCmd)
        performInit_thread.start()
 
        
	# Wait for 10 seconds for enough data to be generated
        time.sleep(3)

        # Perform initial resolution
        initial_resolve_thread = threading.Thread(target=self.performInitialResolution)
        initial_resolve_thread.start()

    def resolve(self, hn, data):
        """
            Loop through all objects it has received
        """
   
        return
        """ 
        # Get a lock so only one resolution
        self.resolveLock.acquire()

        # Check if topk is valid, if not don't resolve
        
        violated_objects = data['violations']
        topk = data['topk']
        partials_at_node = data['partials']

        for top_obj in topk:
            # a, b, c
            for obj in violated_objects:
                partial_val = partials_at_node[obj]
               
                if (partial_val['val'] + partial_val['adj'] + self.coord 
                    if(partialCopy[top_obj]['val'] + partialCopy[top_obj]['param'] < partialCopy[obj]['val'] + partialCopy[obj]['param']):
                        violated_objects.append(obj)
                        # sendConstraintViolation(self.node['partials'][top_obj])

        self.resolveLock.release()
        """

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
        elif (msgType == settings.MSG_CONST_VIOLATIONS):
            # Phase 2
            self.resolve(hn, data['data'])

        elif (msgType == 'getVals'):
            # Request to get all current values at this node
            # Simply send to the coordinator
            self.getVals(hn, requestSock)


    def sendStartCmd(self):
        for hn, node in self.nodes.iteritems():
            msg = {"msgType": settings.MSG_START_GEN, "hn": hn}
            comm.send_msg((node['ip'], self.nodeport), msg)


    def performInitialResolution(self):
            

        # TODO add resolution lock, only one resolution can occur at once
        # TODO separate these into functions, make it so initial top k query is the same as later queries

        # Start process to get initial top k values, this will be used to set thresholds at each node
        # Responses must be handled asynchronously
        out.info("Sending requests for all data to each node for initial top-k computation.\n")
        
        self.F_node = (1.0 - self.F_coord) / len(self.nodes) 

        out.info("F_node: %s.\n" % self.F_node)
        for hn, node in self.nodes.iteritems():
            node['partials'] = {}
            node['border'] = 0
            node['F'] = self.F_node

            node['waiting'] = True
            
            msg = {"msgType": settings.MSG_GET_OBJECT_COUNTS, "hn": hn}
            comm.send_msg((node['ip'], self.nodeport), msg)

        self.coordVals = {}
        self.coordVals['partials'] = {}
        self.coordVals['border'] = 0.0
        self.coordVals['F'] = self.F_coord
       
 
        out.info("Waiting for all responses to arrive.\n")
        # Will wait until all nodes have values
        self.waitForResponses()        
        
        out.info("Responses arrived, calculating leeway values.\n")

        self.resolveLock.acquire()
        self.calcEverything()

        self.resolveLock.release()

    def calcEverything(self):

        try:
            participatingSum = {}
            borderSum = 0
            aggregateSum = {}
    
            for hn, node in self.nodes.iteritems():
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
    
    
            out.info("Participating sum: %s.\n" % participatingSum)
            out.info("Aggregate sum: %s.\n" % aggregateSum)
            out.info("Border sum: %s.\n" % borderSum)
    
            ###################################################
            # SORT TO GET TOP K
            sortedVals = self.sortVals(aggregateSum)
            topk = sortedVals[0:self.k]
            participatingObjects = [a[0] for a in sortedVals]
            topKObjects = [a[0] for a in topk]
    
            out.info("sortedVals: %s.\n" % sortedVals)
            out.info("topk: %s.\n" % topk)
            out.info("participatingObjects: %s.\n" % participatingObjects)
            out.info("topKObjects: %s.\n" % topKObjects)
    
            ####################################################
            # CALCULATE LEEWAY
            leeway = {}
            
            for o in participatingObjects:
                # If the object is in the top k set, we need to include epsilon
                leeway[o] = participatingSum[o] - borderSum + self.epsilon
                if (o in topKObjects): 
                    leeway[o] += self.epsilon
    
    
            out.info("leeway: %s.\n" % leeway)
    
            #####################################################
            # ASSIGN ADJUSTMENT FACTORS
            for hn, node in self.nodes.iteritems():
                for o in participatingObjects:
                    border = node.get('border', 0.0)
                    if (o in node['partials']):
                        out.info("1\n")
                        partialVal = node['partials'][o]['val']
                    else:
                        out.info("2\n")
                        partialVal = 0.0
                    
                    allocLeeway = node['F']*leeway[o]
 
                    node['partials'][o]['param'] = border - partialVal + allocLeeway
           
            out.info("3\n")
            #####################################################
            # ASSIGN ADJUSTMENT FACTORS FOR COORDINATOR
            for o in participatingObjects:
                border = self.coordVals.get('border', 0.0)
                if (o not in self.coordVals['partials']):
                    self.coordVals['partials'][o] = {'val': 0.0, 'param': 0.0}

                partialVal = self.coordVals['partials'][o]['val']
                    
                out.info("4\n")
                allocLeeway = self.coordVals['F']*leeway[o]
                out.info("5\n")
                out.info("%s" % self.coordVals['partials'])
                self.coordVals['partials'][o]['param'] = border - partialVal + allocLeeway
                out.info("6\n")
                if (o in topKObjects):
                    out.info("6.5\n")
                    self.coordVals['partials'][o]['param'] -= self.epsilon
                out.info("7\n")
          

            out.info("5\n")
 
            ## Top k now determined, send message to each of the nodes with top k set and adjustment factors
            for hn, node in self.nodes.iteritems():
                sendData = {}
                sendData['topk'] = topKObjects
                sendData['partials'] = node['partials']
               
                 
                msg = {"msgType": settings.MSG_SET_NODE_PARAMETERS, 'hn': hn, 'data': sendData}
                comm.send_msg((node['ip'], self.nodeport), msg)
            out.info("6\n")


        except Exception as e:
            out.err('calcEverything Exception: %s\n' % e)    

    def waitForResponses(self):
        """
            waits until all nodes have responded with their partial values.

        """
        waiting = False

        while (True):
            waiting = False
            # Iterate through nodes, make sure all have responded
            for hn, node in self.nodes.iteritems():
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
        # TODO - use a copy of nodes so we can handle a resolution set that isn't everything
        out.info("Setting object stats for host: %s\n" % hn)
        self.dataLock.acquire()     
        self.nodes[hn]['partials'] = data['partials']
        self.nodes[hn]['border'] = data['border']
        self.nodes[hn]['waiting'] = False
        self.dataLock.release()



    


    def setTopK(sortVals):

        # Set the top k value, only keep the top however if not enough objs
        if (len(sortVals) < self.k):
            self.topk = sortVals
        else:
            self.topk = sortVals[0:self.k]


    def requestCurrentVals(self, objects, nodes):
        """
            Requests the current partial values from Nj
            nodes: Nj, nodes involved in resolution set
            objects: Oi, objects in resolution
            
        """
        pass    
    def sortVals(self, vals):
        """ 
            Expects a dictionary of d[key] = value
            Returns a sorted array of (key, value) tuples
        """

        sortedVals = sorted(vals.items(), key=operator.itemgetter(1), reverse=True)
        return sortedVals
