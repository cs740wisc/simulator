import matplotlib.pyplot as plt
import matplotlib as m
import matplotlib.cm as cmx
import matplotlib.colors as colors
from numpy.random import rand
import numpy as np
import argparse
import json
import csv
def toInt(tup):
    key, val = tup
    return int(key.split('_')[-1]), val


def graph(topk_data):
    index = 0


    num_eps = len(topk_data)
    winter = plt.get_cmap('winter')
    cNorm = colors.Normalize(vmin=0, vmax=num_eps)
    scalarMap = cmx.ScalarMappable(norm=cNorm, cmap=winter)
    

    sorted_topk = iter(sorted(topk_data.items(), key=toInt))
    for ep, data in sorted_topk:
        times = []
        totalCost = []
        totalBytes = 0 
        starttime = 0.0
        print("ep: %s" % ep)
        print("index: %s" % index)
        for i in data:
            print("i: %s" % i)
            (time, send_rcv, host, msgType, numBytes) = i
            time = float(time)
            if (send_rcv == 'STARTTEST'):
                print("foundStartTime: %s" % time)
                starttime = time
            elif (send_rcv == 'STOPTEST'):
                endtime = time
            elif (msgType == 'startGen'):
                continue
            elif (msgType == 'testComplete'):
                continue
            else:
                totalBytes += int(numBytes)
               
            currtime = time - starttime
            times.append(currtime)
            totalCost.append(totalBytes) 
              

        colorVal = scalarMap.to_rgba(index)
        plt.plot(times, totalCost, '.-', color=colorVal, label='topk, ep=%s' % int(ep))
        index += 1


    # TODO add extra plot for the mapreduce style

    plt.title('Communication Cost vs Time, Varying Epsilon')
    plt.legend(loc=2)
    
    plt.xlabel("Time") 
    plt.ylabel("Total Cost")
    plt.show()
    #plt.savefig('../report/images/train/err_comps/%s.png' % tests[i], dpi=300)
    #plt.close()

if __name__ == '__main__':
    import glob
    

    parser = argparse.ArgumentParser(
        description='Plot total communication cost vs time.')
    parser.add_argument('test_dir')
    args = parser.parse_args()

    # Dictionary indexed by epsilon
    topk_data = {}
    # Also include map reduce data
    # TODO - implement this
    mapr_data = {}

    for path in glob.glob('%s/ep_*' % args.test_dir):
        epsilon = path.split('_')[-1]
        topk_data[epsilon] = []
        with open('%s/c0.csv' % path, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                topk_data[epsilon].append(row)
    
    graph(topk_data)      
