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

        self.valLock.acquire()
        self.vals = {}
        self.valLock.release()

        self.paramLock.acquire()
        self.params = {}
        self.paramLock.release()
        
        self.borderVal = 0
        
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
            self.addRequestToVals(obj)
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
        valsCopy = copy.deepcopy(self.vals)
        self.valLock.release()

        addr = (srcIP, settings.RECV_PORT)
        msg = {'msgType': settings.MSG_GET_OBJECT_COUNTS_RESPONSE, 'data': valsCopy, 'hn': hn}

        comm.send_msg(addr, msg) 

    def getSomePartialVals(self, hn, srcIP, whichVals):

        self.valLock.acquire()
        valsCopy = copy.deepcopy(self.vals)
        self.valLock.release()

        addr = (srcIP, settings.RECV_PORT)
        msg = {'msgType': settings.MSG_GET_OBJECT_COUNTS_RESPONSE, 'data': valsCopy, 'hn': hn}

        comm.send_msg(addr, msg) 

    def addRequestToVals(self, obj):

        self.valLock.acquire()
    
        if (obj in self.vals):
            self.vals['obj'] += 1
        else:
            self.vals['obj'] = 1
        
        self.valLock.release()    

    def checkParams(self, valsCopy):
        """
            Go through vals, determine if any were violated.
            If so, call send violated to send all violated constraints to coordinator.

        """

        
    def setParams(self, params):

        
        pass

