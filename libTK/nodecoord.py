from libTK import *
import threading, socket
from libTK import comm
from libTK import settings
import copy


#TODO

# Store adjustment factors for each object


class NodeCoordinator():
    """
        Maintains the per node values
        Receives any new messages, and calculates if any updates are necessary

    """
    def __init__(self, master_address):
        out.info("Instantiating node coordinator class.\n")
        self.valLock = threading.Lock()
        self.paramLock = threading.Lock()


        self.topk = []
        self.valLock.acquire()
        self.node = {}
        self.node['border'] = 12.0
        self.node['partials'] = {}
        self.node['partials']['a'] = {'val': 15.0, 'param': 0.0}
        self.node['partials']['b'] = {'val': 12.0, 'param': 0.0}
        self.node['partials']['c'] = {'val': 9.0, 'param': 0.0}
        self.valLock.release()

        self.master_address = master_address


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
            obj = data['object']
            self.addRequest(obj)
        elif (msgType == settings.MSG_GET_OBJECT_COUNTS):
            # Request to get all current values at this node
            # Simply send to the coordinator
            out.info("Returning current object counts to coordinator.\n")
            self.getAllPartialVals(hn, srcIP)
        elif (msgType == settings.MSG_SET_NODE_PARAMETERS):
            # Request to set the adjustment parameters for each object at this node
            out.info("Setting node parameters assigned from coodinators.\n")
            params = data['params']
            
            self.setParams(params)


    def getAllPartialVals(self, hn, srcIP):

        self.valLock.acquire()
        nodeCopy = copy.deepcopy(self.node)
        self.valLock.release()
        
        addr = (srcIP, settings.RECV_PORT)
        msg = {'msgType': settings.MSG_GET_OBJECT_COUNTS_RESPONSE, 'data': nodeCopy, 'hn': hn}

        comm.send_msg(addr, msg) 


    def getSomePartialVals(self, hn, srcIP, whichVals):

        self.valLock.acquire()
        valsCopy = copy.deepcopy(self.vals)
        self.valLock.release()

        addr = (srcIP, settings.RECV_PORT)
        msg = {'msgType': settings.MSG_GET_OBJECT_COUNTS_RESPONSE, 'data': valsCopy, 'hn': hn}

        comm.send_msg(addr, msg) 



    def addRequest(self, obj):
        """
            Currently increments the single object by 1 on each request.
            This can be extended for more detailed value counts, like the 15 minutes sliding window mentioned in the paper.
        """
        self.valLock.acquire()
        if (obj in self.node['partials']):
            self.node['partials'][obj]['val'] += 1.0
        else:
            # New object, set the adjustment parameter to 0
            self.node['partials'][obj] = {'val': 1.0, 'param': 0.0}

        self.valLock.release()    

        self.checkParams()


    def checkParams(self):
        """
            Go through vals, determine if any were violated.
            If so, call send violated to send all violated constraints to coordinator.
            
        """
        # TODO - this is really slow, we could incrementally keep track of everything but for now this is easier to build
        self.valLock.acquire()
        partialCopy = copy.deepcopy(self.partials)        
        self.valLock.release()    

        # Loop through the topk nodes, get the lowest value  
        # Loop through each object and check against others
               
        # To coordinator, send a message containing: all members in resolution set, and special border values 



    def setParams(self, data):
        """
            Receives a message containing:
            data = {
                        'topk': [objA, objB, ...]
                        'params': {
                                     'objA': adjA
                                     'objB': adjB
                                  }
                    }
            Sets the topk set and adjustment parameters which will be monitored 
        """
        self.topk = data['topk']
        # Set up all the new parameters
        self.valLock.acquire()
      
        for obj, adjFactor in data['params'].iteritems():
            if obj in self.partials:
                self.partials[obj]['param'] = adjFactor
            else:
                self.partials[obj] = {'val': 0, 'param': adjFactor}

        self.valLock.release() 
