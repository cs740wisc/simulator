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
            (time, send_rcv, host, msgType, numBytes) = data[i]
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
    p.add_argument('-t', '--test_dir', help='Directory of test to graph', type=str, default="none")
    p.add_argument('-a', '--average', help='Running Average to Use', type=int, default=5)
    return p

if __name__ == '__main__':
    import glob
    
    p = setupArgParse()
    args = p.parse_args()

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
