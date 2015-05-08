import matplotlib.pyplot as plt
from numpy.random import rand
import numpy as np
import argparse
import json
import csv
def graph(topk_data):
    index = 0


    for ep, data in topk_data.iteritems():
        times = []
        totalCost = []
        totalBytes = 0 
        starttime = 0.0
        for i in data:
            (time, send_rcv, host, msgType) = i
            time = float(time)
            if (send_rcv == 'STARTTEST'):
                starttime = time
            elif (send_rcv == 'STOPTEST'):
                endtime = time
            elif (msgType == 'startGen'):
                continue
            elif (msgType == 'testComplete'):
                continue
            else:
                totalBytes += 1
               
            currtime = time - starttime
            times.append(time)
            totalCost.append(totalBytes) 
              

        plt.plot(times, totalCost, '.-', color=plt.cm.winter(index), label='topk, ep=%s' % ep)
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
