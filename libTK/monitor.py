from libTK import *
import threading, socket
from libTK import comm
from libTK import settings
import copy, yaml, time
from collections import deque
#TODO

# Store adjustment factors for each object

class Monitor():
    """
        Maintains the per node values
        Receives any new messages, and calculates if any updates are necessary

    """
    def __init__(self, master_address, hostname, testname):
        out.info("Instantiating node coordinator class.\n")
        self.valLock = threading.Lock()
        self.paramLock = threading.Lock()
        self.constraints_lock = threading.Lock()
      
        self.topk = []
        self.valLock.acquire()
        self.node = {}
        self.node['border'] = 0.0
        self.node['partials'] = {}
        self.valLock.release()
      
        self.topk_iter = 0 
        self.in_resolution = False
 
        self.hn = hostname
        self.testname = testname
 
        self.rollingWindow = deque()
        self.master_address = master_address
        
        self.perSecond = 1.0
        
        self.loadData()
        
        self.sendData_thread = threading.Thread(target=self.genData)
        self.sendData_thread.start()

        self.checkWindow_thread = threading.Thread(target=self.checkWindow)
        #self.checkWindow_thread.start()

        paramsChecker = threading.Timer(1.0/self.perSecond, self.checkParams)
        paramsChecker.start() 


    def loadData(self):
        """
            Opens a file which provides the json specification for test datasets
            Assumes that each key will default to generating at 1 val per second
        """
        self.loadedData = yaml.load(open('genData/%s/%s.txt' % (self.testname, self.hn), 'r'))

        self.data = [[0 for col in range(25)] for row in range(len(self.loadedData))]
        # Assume 10 seconds if not mentioned
        self.durations = [10 for row in range(len(self.loadedData))]
        for i, d in enumerate(self.loadedData):
            for key, val in d['freqs'].iteritems():
                self.data[i][ord(key)-ord('a')] = val
            self.durations[i] = d.get('duration', 10)
            
        out.info("Data initialized: %s\n" % self.data)
        out.info("Durations initialize: %s\n" % self.durations) 
 
        self.dataIndex = 0
        self.dataTicks = 0
        self.gen = False

        self.nextDistTicks = self.durations[0]*self.perSecond
        #self.startGen()

    def stopGen(self):
        self.gen = False
        self.waiting = False

    def startGen(self):
        self.gen = True

    def genData(self):
        if (self.gen):
            nextIter = threading.Timer(1.0/self.perSecond, self.genData)
            nextIter.start() 
            sendData = {}
            for i, d in enumerate(self.data[self.dataIndex]):
                # PWM to send data for each item
                if (d > 0):
                    sendData[chr(i+97)]=d
           
            self.addRequest(sendData)

            self.dataTicks += 1

            # We must move on the next distribution specified in the file
            if (self.dataTicks >= self.nextDistTicks):
                self.dataIndex += 1
                if (self.dataIndex >= len(self.durations)):
                    # Exit if there are no durations left specified
                    out.warn("No more distributions, exiting.\n")
                    self.stopGen()
                    nextIter.cancel()
                else:
                    # Otherwise calculate the next time we must switch
                    self.nextDistTicks = self.dataTicks + self.durations[self.dataIndex] * self.perSecond
                    out.warn("Switching to the next distribution.\n")

    def getAllPartialVals(self, srcIP):

        self.valLock.acquire()
        nodeCopy = copy.deepcopy(self.node)
        self.valLock.release()

        addr = (srcIP, settings.RECV_PORT)
        msg = {'msgType': settings.MSG_GET_OBJECT_COUNTS_RESPONSE, 'data': nodeCopy, 'hn': self.hn}

        comm.send_msg(addr, msg) 


    def getSomePartialVals(self, srcIP, whichVals):
        """ Needs to return partial values, along with the border value.
        """

        self.valLock.acquire()
        nodeCopy = copy.deepcopy(self.node)
        topkCopy = copy.deepcopy(self.topk)
        self.valLock.release()

        sendData = {'partials': {}}

        for w in whichVals:
            if (w in nodeCopy['partials']):
                sendData['partials'][w] = nodeCopy['partials'][w]

        sendData['border'] = self.findBorderVal(topkCopy, nodeCopy['partials'])

        addr = (srcIP, settings.RECV_PORT)
        msg = {'msgType': settings.MSG_GET_OBJECT_COUNTS_RESPONSE, 'data': sendData, 'hn': self.hn}

        comm.send_msg(addr, msg) 


    def checkWindow(self):
        while self.gen:
            
            out.info('checking window\n')
            currtime = time.time()
            while (len(self.rollingWindow) > 0 and (currtime - self.rollingWindow[0][0] > 15)):
                t, data = self.rollingWindow.popleft()

                self.valLock.acquire()

                for obj, val in data.iteritems():
                    self.node['partials'][obj]['val'] -= val
                
                self.valLock.release()

            # Sleep a little so we aren't doing this too often
            time.sleep(1)

    def addRequest(self, data):
        """
            Currently increments the single object by 1 on each request.
            This can be extended for more detailed value counts, like the 15 minutes sliding window mentioned in the paper.
        """
        currtime = time.time() 
        self.rollingWindow.append((currtime, data))
        self.valLock.acquire()
        #print("received values: %s" % data)
        for obj, val in data.iteritems():
            if (obj in self.node['partials']):
                self.node['partials'][obj]['val'] += val
            else:
                # New object, set the adjustment parameter to 0
                self.node['partials'][obj] = {'val': val, 'param': 0.0}

        self.valLock.release()    

    def setTopK(self, data):
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
        self.topk_iter = data['topk_iter']
        self.clearWaitingConstraints()
        # Set up all the new parameters
        self.valLock.acquire()
      
        for obj, partial in data['partials'].iteritems():
            if obj in self.node['partials']:
                self.node['partials'][obj]['param'] = partial['param']
            else:
                self.node['partials'][obj] = {'val': 0, 'param': partial['param']}

        self.valLock.release() 
    
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
        self.clearWaitingConstraints()
        # Set up all the new parameters
        self.valLock.acquire()
        for obj, partial in data['partials'].iteritems():
            if obj in self.node['partials']:
                self.node['partials'][obj]['param'] = partial['param']
            else:
                self.node['partials'][obj] = {'val': 0, 'param': partial['param']}

        self.valLock.release() 

    def receivedData(self, requestSock, msg):
        """ 
            Listens for data from the coordinator or generator. Handles appropriately.
        """
        #out.info("Node received message: %s\n" % data)
        msgType = msg['msgType']
        srcIP = requestSock.getpeername()[0]
        out.info('Received message: %s\n' % msgType)
        
        if   (msgType == settings.MSG_REQUEST_DATA):
            # From generator, request for a specific node should increment value by 1.
            obj  = msg['object']
            self.addRequest(obj)
        elif (msgType == settings.MSG_GET_OBJECT_COUNTS):
            # Request to get all current values at this node
            # Simply send to the coordinator
            out.info("Returning current object counts to coordinator.\n")
            self.getAllPartialVals(srcIP)
        elif (msgType == settings.MSG_GET_SOME_OBJECT_COUNTS):
            out.info("Returning some object counts to coordinator.\n")
            whichVals = msg['data']
            self.getSomePartialVals(srcIP, whichVals)

        elif (msgType == settings.MSG_SET_NODE_PARAMETERS):
            # Request to set the adjustment parameters for each object at this node
            out.info("Setting node parameters assigned from coodinators.\n")
            params = msg['data']
            self.setParams(params)
        elif (msgType == settings.MSG_SET_TOPK):
            # Request to set the adjustment parameters for each object at this node
            out.info("Setting topk and node parameters assigned from coodinators.\n")
            params = msg['data']
            self.setTopK(params)
        elif (msgType == settings.MSG_START_GEN):
            # Start generating data
            self.startGen()
            nextIter = threading.Timer(1.0/self.perSecond, self.genData)
            nextIter.start() 
            self.checkWindow_thread.start()
        elif (msgType == settings.MSG_STOP_GEN):
            self.stopGen()


    def sendConstraintViolation(self, violated_objects, partialCopy, topkCopy, topk_iterCopy):
        """
            Sends data = {
                            'topk' : {
                                        obj1:val1
                                        obj2:val2
                            }
                            'violations' : {
                                        obj1:val1
                                        obj2:val2
                            }
                            'border' : border_value
            }
        """

        print ("partialCopy: %s" % partialCopy)
        sendData = {}

        sendData['topk'] = topkCopy
        sendData['topk_iter'] = topk_iterCopy
        sendData['violations'] = violated_objects

        sendData['partials'] = {}
        for obj in topkCopy:
            sendData['partials'][obj] = partialCopy[obj]

        for obj in violated_objects:
            sendData['partials'][obj] = partialCopy[obj]
            

        sendData['border'] = self.findBorderVal(topkCopy, partialCopy)

       
        out.warn("Send violated constraints.\n")
        out.warn("%s\n" % sendData)
 
        msg = {"msgType" : settings.MSG_CONST_VIOLATIONS, 'hn': self.hn, 'data' : sendData}
        comm.send_msg(self.master_address, msg)

    def findBorderVal(self, topkCopy, partialCopy):
        # Compute Border Value B for this node
        # min adjusted value among topk items
        min_topk = 10000    
        for obj in topkCopy:
            if((partialCopy[obj]['val'] + partialCopy[obj]['param']) < min_topk):
                min_topk = (partialCopy[obj]['val']) + (partialCopy[obj]['param'])

        # Max adjusted value among non top k items
        max_non_topk = 0
        for obj in partialCopy.keys():
            if obj not in topkCopy:
                if((partialCopy[obj]['val'] + partialCopy[obj]['param']) > max_non_topk):
                    max_non_topk = (partialCopy[obj]['val']) + (partialCopy[obj]['param'])

        border_value = min(min_topk, max_non_topk)
        return border_value

    def setWaitingConstraints(self):
        self.constraints_lock.acquire()
        self.waiting = True
        self.constraints_lock.release()
    
    def clearWaitingConstraints(self):
        self.constraints_lock.acquire()
        self.waiting = False
        self.constraints_lock.release()
        
    def waitOnConstraints(self):
        while (self.waiting):
            out.info('waiting\n')
            time.sleep(1) 


    def checkParams(self):
        """
            Go through vals, determine if any were violated.
            If so, call send violated to send all violated constraints to coordinator.
            
        """
        
        # TODO - this is really slow, we could incrementally keep track of everything but for now this is easier to build
        self.valLock.acquire()
        partialCopy = copy.deepcopy(self.node['partials'])        
        topkCopy = copy.deepcopy(self.topk)
        topk_iterCopy = copy.deepcopy(self.topk_iter)
        self.valLock.release()    

        out.warn("checkParams\npartials: %s\ntopkCopy: %s\n" % (partialCopy, topkCopy))

        # Loop through the topk nodes, get the lowest value  
        # Loop through each object and check against others

        violated_objects = []
        for top_obj in topkCopy:
            # a, b, c
            for obj, vals in partialCopy.iteritems():
                # a: {val: 4, adj: 3}
                if(obj not in topkCopy):
                    if(partialCopy[top_obj]['val'] + partialCopy[top_obj]['param'] < partialCopy[obj]['val'] + partialCopy[obj]['param']):
                        violated_objects.append(obj)
                        # sendConstraintViolation(self.node['partials'][top_obj])
        if(len(violated_objects) > 0):
            out.warn("Detected violated objects: %s\n" % violated_objects)
            self.setWaitingConstraints()
            self.sendConstraintViolation(violated_objects, partialCopy, topkCopy, topk_iterCopy)
            self.waitOnConstraints()


        # don't schedule next thread until we have completed resolving
        paramsChecker = threading.Timer(1.0/self.perSecond, self.checkParams)
        paramsChecker.start() 
