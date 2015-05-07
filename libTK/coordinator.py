import json, yaml
from libTK import *
import threading, socket
from libTK import comm
from libTK import settings
import time
import copy
import operator
import csv

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
        self.topk_iter = 0
        self.topk = []
        
        self.running = True
        
        self.results_path = 'results/%s.csv' % testname


        self.output_list = []

        self.dataLock = threading.Lock()
        self.resolveLock = threading.Lock()
        self.outputLock = threading.Lock()

        output_thread = threading.Thread(target=self.outputData)
        output_thread.start()


        # Original thread needs to stay open to listen as server
        # Contacts all nodes, performs resolution, sets up initial parameters       
        performInit_thread = threading.Thread(target=self.sendStartCmd)
        performInit_thread.start()

        # Wait for 10 seconds for enough data to be generated
        time.sleep(3)

        # Perform initial resolution
        initial_resolve_thread = threading.Thread(target=self.performInitialResolution)
        initial_resolve_thread.start()


    #########################################################################################################
    #########################################################################################################
    def outputData(self):
        while (self.running):
            self.outputLock.acquire()
            rowsOut = copy.deepcopy(self.output_list)
            self.output_list = []
            self.outputLock.release()

            if len(rowsOut) > 0:
                ##########################################
                # SAVE THE INCOMING DATA TO A FILE        
                f = open(self.results_path, 'ab')
                writer = csv.writer(f)
                writer.writerows(rowsOut)            
                f.close()
                ##########################################
            time.sleep(5)

    #########################################################################################################

    #########################################################################################################
    #########################################################################################################
    def stop(self):
        self.running = False
    #########################################################################################################

    #########################################################################################################
    #########################################################################################################
    def addToOut(self, row):
        currtime = time.time
        self.outputLock.acquire()
        self.output_list.append(row)
        self.outputLock.release()
    #########################################################################################################
    
    #########################################################################################################
    #########################################################################################################
    def send_msg(self, addr, msg):
        currtime = time.time()
        #out.warn("msg: %s\n" % msg)
        outrow = [currtime, 'send', msg['hn'], msg['msgType']]
    
        self.addToOut(outrow)
        comm.send_msg(addr, msg)    
    #########################################################################################################


    #########################################################################################################
    #########################################################################################################
    def getSomePartials(self, ignore_host, resolution_set):
        """
            Send a message to each node asking to get partial values. 
            Should set waiting to True for each node so we can wait for the proper response
        """
        for hn, node in self.nodes.iteritems():
            if (hn != ignore_host): 
                node['waiting'] = True
                
                msg = {"msgType": settings.MSG_GET_SOME_OBJECT_COUNTS, "hn": hn, "data": resolution_set}
                self.send_msg((node['ip'], self.nodeport), msg)
    #########################################################################################################

    #########################################################################################################
    #########################################################################################################
    def validationTest(self, hn, violated_objects, topk, partials_at_node):
        """


        """


        for top_obj in topk:
            # a, b, c
            for obj in violated_objects:
                partial_val = partials_at_node[obj]
                partial_val_topk = partials_at_node[top_obj]

                if (obj not in self.coordVals['partials']):
                    self.coordVals['partials'][obj] = {'val': 0.0, 'param': 0.0}
                if (top_obj not in self.coordVals['partials']):
                    self.coordVals['partials'][top_obj] = {'val': 0.0, 'param': 0.0}

                coord_param = self.coordVals['partials'][obj]['param']
                coord_param_topk = self.coordVals['partials'][top_obj]['param']

                if (partial_val['val'] + partial_val['param'] + coord_param > partial_val_topk['val'] + partial_val_topk['param'] + coord_param_topk):
                    # If any violated global totals are greater than any top k totals, we must do reallocation
                    return False

        # If we get here everything should be ok, so return True
        return True
    #########################################################################################################


    #########################################################################################################
    #########################################################################################################
    def receivedData(self, requestSock, msg):
        """ 
            Listens for any response messages. They are detailed below.
            Each response contains the name of the node for each lookup.


            getValsResponse:       

 
        """
        out.warn("RECV_MSG: %s\n" % msg)

        msgType = msg['msgType']
        hn = msg['hn']

        currtime = time.time()
        outrow = [currtime, 'recv', msg['hn'], msg['msgType']]
        self.addToOut(outrow)

        if   (msgType == settings.MSG_GET_OBJECT_COUNTS_RESPONSE):
            # From generator, request for a specific node should increment value by 1.
            self.setObjectStats(hn, msg['data'])
        elif (msgType == settings.MSG_CONST_VIOLATIONS):
            # Phase 2
            self.resolve(hn, msg['data'])

        elif (msgType == 'getVals'):
            # Request to get all current values at this node
            # Simply send to the coordinator
            self.getVals(hn, requestSock)
    #########################################################################################################


    #########################################################################################################
    #########################################################################################################
    def sendStartCmd(self):
        for hn, node in self.nodes.iteritems():
            msg = {"msgType": settings.MSG_START_GEN, "hn": hn}
            self.send_msg((node['ip'], self.nodeport), msg)
    #########################################################################################################

    
    #########################################################################################################
    #########################################################################################################
    def setBorderVal(self):
        # Compute Border Value B for this node
        # min adjusted value among topk items

        if len(self.topk) == 0:
            self.coordVals['border'] = 0.0
            return

        
        partials = self.coordVals['partials']

        # Max adjusted value among non top k items
        max_non_topk = 0
        for obj in partials.keys():
            if obj not in self.topk:
                if(partials[obj]['param'] > max_non_topk):
                    max_non_topk = partials[obj]['param']

        self.coordVals['border'] = max_non_topk
    #########################################################################################################

    #########################################################################################################
    #########################################################################################################
    def resolve(self, hn, data):
        """
            Perform the entire resolution.
            Check if we all global constraints are still valid
                If so we simply reallocate among the coord/specific node
                If not we must contact all nodes, get all the values in the resolution set.

        """
        # Get a lock so only one resolution
        self.resolveLock.acquire()

        out.info("Checking resolve.\n")

        violated_objects = data['violations']
        topk = data['topk']
        partials_at_node = data['partials']
        topk_iter = data['topk_iter']

        # Don't process any messages that refer to old data
        if (self.topk_iter > topk_iter):
            out.info("Data for %s out of date, removing.\n" % hn)
            self.resolveLock.release()
            return

        # Set new stats so reallocation works properly        
        self.setObjectStats(hn, data)

        resolution_set = violated_objects
        resolution_set.extend(topk)
        #out.info("Resolution set: %s.\n" % resolution_set)


        #out.info("checking if valid for host: %s.\n" % hn)
        stillValid = self.validationTest(hn, violated_objects, topk, partials_at_node)
        # Check if topk is valid, if not don't resolve
       
        if stillValid:
            out.info("TOPK still valid, performing reallocation.\n")
            self.performReallocation(res=resolution_set, host=hn, topkObjects=topk) 
        else:
            out.info("TOPK no longer valid, getting all partial objects.\n")
            self.getSomePartials(hn, resolution_set)

            # Blocking, will not complete until everything is completed
            self.waitForResponses()
            self.performReallocation(res=resolution_set, host=None, topkObjects=None) 


        out.err("TOPK OBJECTS: %s\n" % self.topk)
        self.resolveLock.release()
    #########################################################################################################


    #########################################################################################################
    #########################################################################################################
    def performReallocation(self, res=None, host=None, topkObjects=None):
        """
            Todo - will receive a list of hosts, and a list of objects
            Just allocates based on those objects
            
            Different cases:
                1. Initialization. We want all objects, and all nodes
                    res=None, host=None, topkObjects=None
                    Nodes: all
                    Objects: all

                2. Simple reallocation
                    res=[a, b, c], host=h1, topkObjects=[a]
                    Nodes: coord/specific node
                    Objects: resolution set

                3. Full reallocation
                    res=[a,b,c], host=None, topkObjects=None
                    Nodes: all
                    Objects: resolution set
                    All nodes


        """

        try:


            if (topkObjects):
                setTopK = False
            else:
                setTopK = True


            participatingSum = {}
            borderSum = 0
            aggregateSum = {}
    
            if (host):
                hns = [host]
            else:
                hns = self.nodes.keys()

            #out.info("res: %s.\n" % res)
            #out.info("host: %s.\n" % host)
            #out.info("topkObjects: %s.\n" % topkObjects)
            #out.info("1\n")
            for hn in hns:
                node = self.nodes[hn]
                borderSum += node['border']
                for key, info in node['partials'].iteritems():
                    if (res is not None and key not in res):
                        #out.warn("REALLOC: skipping object %s.\n" % key)
                        continue
 
                    # We have already seen key, just add to this key
                    if key in participatingSum:
                        participatingSum[key] += info['val'] + info['param']
                        aggregateSum[key] += info['val']
                    else:
                        participatingSum[key] = info['val'] + info['param']
                        aggregateSum[key] = info['val']

            #out.info("2\n")
            ###########################################################################
            self.setBorderVal()
            #out.info("3\n")
                
            # TODO add the max of the adjustment params at the coordinator not in resolution set
            borderSum += self.coordVals['border']
    
            #out.info("Participating sum: %s.\n" % participatingSum)
            #out.info("Aggregate sum: %s.\n" % aggregateSum)
            #out.info("Border sum: %s.\n" % borderSum)

            ###################################################
            # SORT TO GET TOP K
            if (topkObjects is None):
                sortedVals = self.sortVals(aggregateSum)
                # In single case, particating is the resolution set and topk is already known
                res = [a[0] for a in sortedVals]
                topkObjects = [a[0] for a in sortedVals[0:self.k]]
                self.topk = topkObjects

            #out.info("res: %s.\n" % res)
            #out.info("topkObjects: %s.\n" % topkObjects)

            ####################################################
            # CALCULATE LEEWAY
            leeway = {}
            
            for o in res:
                # If the object is in the top k set, we need to include epsilon
                leeway[o] = participatingSum[o] - borderSum + self.epsilon
                if (o in topkObjects): 
                    leeway[o] += self.epsilon
    
    
            #out.info("leeway: %s.\n" % leeway)
    
            #####################################################
            # ASSIGN ADJUSTMENT FACTORS
            for hn in hns:
                node = self.nodes[hn]
                for o in res:
                    border = node.get('border', 0.0)
                    if (o not in node['partials']):
                        node['partials'][o] = {'val': 0.0, 'param': 0.0}
                    

                    partialVal = node['partials'][o]['val']

                    allocLeeway = node['F']*leeway[o]
 
                    node['partials'][o]['param'] = border - partialVal + allocLeeway
           
            #out.info("4\n")
            #####################################################
            # ASSIGN ADJUSTMENT FACTORS FOR COORDINATOR
            for o in res:
                border = self.coordVals.get('border', 0.0)
                if (o not in self.coordVals['partials']):
                    self.coordVals['partials'][o] = {'val': 0.0, 'param': 0.0}

                partialVal = self.coordVals['partials'][o]['val']
                    
                allocLeeway = self.coordVals['F']*leeway[o]
                self.coordVals['partials'][o]['param'] = border - partialVal + allocLeeway
                if (o in topkObjects):
                    self.coordVals['partials'][o]['param'] -= self.epsilon
 
            #out.info("5\n")
            ## Top k now determined, send message to each of the nodes with top k set and adjustment factors
            if (setTopK):    
                self.topk_iter += 1


            for hn in hns:
                node = self.nodes[hn]
                sendData = {}
                sendData['partials'] = node['partials']
            
                if (setTopK):     
                    sendData['topk'] = topkObjects
                    sendData['topk_iter'] = self.topk_iter
                    msg = {"msgType": settings.MSG_SET_TOPK, 'hn': hn, 'data': sendData}
                else: 
                    msg = {"msgType": settings.MSG_SET_NODE_PARAMETERS, 'hn': hn, 'data': sendData}
               
                self.send_msg((node['ip'], self.nodeport), msg)

        except Exception as e:
            out.err('calcEverything Exception: %s\n' % e)    
    #########################################################################################################

    #########################################################################################################
    #########################################################################################################
    def waitForResponses(self):
        """
            waits until all nodes have responded with their partial values.

        """
        waiting = False

        while (self.running):
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
    #########################################################################################################

    #########################################################################################################
    #########################################################################################################
    def setObjectStats(self, hn, data):
        """
            Updates the initial values at node hn. 
        """
        # TODO - use a copy of nodes so we can handle a resolution set that isn't everything
        out.info("Setting object stats for host: %s\n" % hn)
        self.dataLock.acquire()     
        self.nodes[hn]['partials'] = data['partials']
        self.nodes[hn]['border'] = data['border']
        self.nodes[hn]['waiting'] = False
        self.dataLock.release()

    #########################################################################################################

    #########################################################################################################
    #########################################################################################################
    def setTopK(sortVals):

        # Set the top k value, only keep the top however if not enough objs
        if (len(sortVals) < self.k):
            self.topk = sortVals
        else:
            self.topk = sortVals[0:self.k]

    #########################################################################################################

    #########################################################################################################
    #########################################################################################################
    def sortVals(self, vals):
        """ 
            Expects a dictionary of d[key] = value
            Returns a sorted array of (key, value) tuples
        """

        sortedVals = sorted(vals.items(), key=operator.itemgetter(1), reverse=True)
        return sortedVals
    #########################################################################################################
    



    #########################################################################################################
    #########################################################################################################
    def performInitialResolution(self):
            

        # TODO add resolution lock, only one resolution can occur at once
        # TODO separate these into functions, make it so initial top k query is the same as later queries

        # Start process to get initial top k values, this will be used to set thresholds at each node
        # Responses must be handled asynchronously
        out.info("Sending requests for all data to each node for initial top-k computation.\n")
        
        self.F_node = (1.0 - self.F_coord) / len(self.nodes) 

        self.resolveLock.acquire()
        
        for hn, node in self.nodes.iteritems():
            node['partials'] = {}
            node['border'] = 0
            node['F'] = self.F_node

            node['waiting'] = True
            
            msg = {"msgType": settings.MSG_GET_OBJECT_COUNTS, 'hn': hn}
            self.send_msg((node['ip'], self.nodeport), msg)

        self.coordVals = {}
        self.coordVals['partials'] = {}
        self.coordVals['border'] = 0.0
        self.coordVals['F'] = self.F_coord
       
 
        out.info("Waiting for all responses to arrive.\n")
        # Will wait until all nodes have values
        self.waitForResponses()        
        
        out.info("Responses arrived, performing reallocation.\n")

        self.performReallocation()

        out.info("Initial reallocation complete.\n")
        self.resolveLock.release()

    #########################################################################################################
