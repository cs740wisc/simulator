import json, yaml
from libTK import *
import threading, socket, time
from libTK import comm
from libTK import settings

class Generator():
    """
        Generators values to the nodes which will store per-object counts
    """

    def __init__(self, nodeip, nodeport, sendtime):
        """ 
            Sends requests to its node every sendtime seconds.
        """

        out.info("Instantiating generator class.\n")

        self.nodeip = nodeip
        self.nodeport = nodeport
        self.sendtime = sendtime
        self.run = False

        self.loadedData = yaml.load(open('genData/h1.txt', 'r'))

        self.data = [[2 for col in range(25)] for row in range(len(self.loadedData))]
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
        self.perSecond = 1
        self.nextDistTicks = self.durations[0]*self.perSecond
        self.startGen()
        self.startGen_thread = threading.Thread(target=self.startGenThread)
        self.startGen_thread.start() 

    def startGenThread(self):
        threading.Timer(1.0/self.perSecond, self.genData).start() 


    def stopGen(self):
        self.run = False
        self.genData_thread.cancel() 

    def startGen(self):
        self.run = True

    def genData(self):
        if (self.run):
            nextIter = threading.Timer(1.0/self.perSecond, self.genData)
            nextIter.start() 
            sendData = {}
            for i, d in enumerate(self.data[self.dataIndex]):
                # PWM to send data for each item
                sendData[chr(i+97)]=d
            msg = {'msgType': settings.MSG_REQUEST_DATA, 'object': sendData, "hn": "h1"}
            comm.send_msg((self.nodeip, self.nodeport), msg)

            self.dataTicks += 1

            # We must move on the next distribution specified in the file
            if (self.dataTicks >= self.nextDistTicks):
                self.dataIndex += 1
                if (self.dataIndex >= len(self.durations)):
                    # Exit if there are no durations left specified
                    out.warn("No more distributions, exiting.\n")
                    self.run = False
                    nextIter.cancel()
                else:
                    # Otherwise calculate the next time we must switch
                    self.nextDistTicks = self.dataTicks + self.durations[self.dataIndex] * self.perSecond
                    out.warn("Switching to the next distribution.\n")

 
    def gen_message(self):
        import random
        import string

        obj = random.choice(string.ascii_lowercase)
        msg = {'msgType': settings.MSG_REQUEST_DATA, 'object': obj}

        comm.send_msg((self.nodeip, self.nodeport), msg)

    def receivedData(self, recvSock, data):

        out.info("Generator received mesage: %s\n" % data)

        msgType = data['msgType']
        hn = data['hn']

        if (msgType == settings.MSG_START_GEN):
            self.startGen()
        elif (msgType == settings.MSG_STOP_GEN):
            self.stopGen()
