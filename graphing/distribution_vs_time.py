import matplotlib.pyplot as plt
import matplotlib as m
import matplotlib.cm as cmx
import matplotlib.colors as colors
from numpy.random import rand
import numpy as np
import argparse
import json
import csv
from collections import deque


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
                #out.warn("No more distributions, exiting.\n")
                msg = {'msgType': settings.MSG_TEST_COMPLETE, 'hn': self.hn}
                comm.send_msg(self.master_address, msg)

                self.stopGen()
                nextIter.cancel()
            else:
                # Otherwise calculate the next time we must switch
                self.nextDistTicks = self.dataTicks + self.durations[self.dataIndex] * self.perSecond
                currtime = time.time()
                outrow = [currtime, 'switchgens', self.hn, 'None']
                f = open(self.output_name, 'ab+')
                writer = csv.writer(f)
                writer.writerow(outrow)
                f.close()

                out.err("Switching to the next distribution.\n")


def toInt(tup):
    key, val = tup
    return int(key.split('_')[-1]), val

def plotSingleTest(legend_str, data, scalarMap, color_index, running_avg_secs):
    times = []
    totalCost = []
    windowBytes = 0.0
    starttime = 0.0
    window = deque()

    print("plotting %s" % legend_str)

   
    starttime = float(data[0][0])
    stoptime = float(data[-1][0])
    runtime = stoptime - starttime

    i = 1
    currtime = 0.5
    while (currtime <= runtime):
        # add all data values which are greater than next step 
        while i<len(data):
            (time, send_rcv, host, msgType, numBytes, epsilon) = data[i]
            nexttime = float(time)
            if (send_rcv == 'STARTTEST' or send_rcv == 'STOPTEST' or send_rcv == 'startGen' or msgType == 'testComplete'):
                i += 1
                continue

            nexttime = nexttime - starttime
            if (nexttime < currtime):
                numBytes = int(numBytes)         
                i += 1
                # Append this time, bytes to deque 
                window.append((nexttime, numBytes))
                windowBytes += numBytes
            else:
                break
        

        # Remove any old times from total bytes
        while (len(window) > 0 and (currtime - window[0][0] > running_avg_secs)):
            t, b = window.popleft()
            windowBytes -= b
        
        # Divide band by avg_time to get average bandwidth
        curr_band = windowBytes/running_avg_secs

        times.append(currtime)
        totalCost.append(curr_band) 
         
        currtime += 1.0

        if (currtime + 20.0 > runtime):
            break  

    colorVal = scalarMap.to_rgba(color_index)
    plt.plot(times, totalCost, '.-', color=colorVal, label=legend_str)
    

def graph(epsilon_data, band_data, running_average_time):

    
    
    ########################################################################
    # Graph the varying bandwidths

    sorted_epsilon = iter(sorted(epsilon_data.items(), key=toInt))
    index = 0
    
    num_eps = len(epsilon_data)
    winter = plt.get_cmap('winter')
    cNorm = colors.Normalize(vmin=0, vmax=num_eps)
    scalarMap_epsilon = cmx.ScalarMappable(norm=cNorm, cmap=winter)
    
    for ep, data in sorted_epsilon:
        legend_str = 'ep=%s' % int(ep)
         
        plotSingleTest(legend_str, data, scalarMap_epsilon, index, running_average_time)
        index += 1

    ########################################################################
    # Graph the varying bandwidths
    sorted_band = iter(sorted(band_data.items(), key=toInt))
    index = 0
    
    num_bands = len(band_data)
    autumn = plt.get_cmap('autumn')
    cNorm = colors.Normalize(vmin=0, vmax=num_eps)
    scalarMap_band = cmx.ScalarMappable(norm=cNorm, cmap=autumn)
    
    for band, data in sorted_band:
        legend_str = 'band=%s' % int(band)
        plotSingleTest(legend_str, data, scalarMap_band, index, running_average_time)
        index += 1


    plt.title('Average Bandwidth vs Time')
    plt.legend(loc=2)
    
    plt.xlabel("Time") 
    plt.ylabel("Total Cost")
    plt.show()
    #plt.savefig('../report/images/train/err_comps/%s.png' % tests[i], dpi=300)
    #plt.close()


def setupArgParse():
    p = argparse.ArgumentParser(description='Graphing Bandwidth vs time for various epsilons/bandwidths')
    p.add_argument('-g', '--gendata', help='The file to output', type=str, default="none")
    return p

if __name__ == '__main__':
    import glob, json
    
    p = setupArgParse()
    args = p.parse_args()


    # Load the data
    testSpec = json.load(open('genData/%s.txt' % args.gendata, 'r'))
    nodeDistribution = testSpec[self.hn]
    out.info("nodeDistribution: %s\n" % nodeDistribution)


    
    self.data = [[0 for col in range(25)] for row in range(len(self.nodeDistribution))]
    # Assume 10 seconds if not mentioned
    self.durations = [10 for row in range(len(self.nodeDistribution))]
    for i, d in enumerate(self.nodeDistribution):
        for key, val in d['freqs'].iteritems():
            self.data[i][ord(key)-ord('a')] = val
        self.durations[i] = d.get('duration', 10)


    # Dictionary indexed by epsilon
    epsilon_data = {}
    
    # Dictionary indexed by bandwidth
    band_data = {}

    for path in glob.glob('%s/ep_*' % args.test_dir):
        epsilon = path.split('_')[-1]
        epsilon_data[epsilon] = []
        with open('%s/c0.csv' % path, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                epsilon_data[epsilon].append(row)

    for path in glob.glob('%s/band_*' % args.test_dir):
        band = path.split('_')[-1]
        band_data[band] = []
        with open('%s/c0.csv' % path, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                band_data[band].append(row)
    
    graph(epsilon_data, band_data, args.average)      
