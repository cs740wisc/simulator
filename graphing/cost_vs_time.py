import matplotlib.pyplot as plt
from numpy.random import rand
import numpy as np
import argparse
import json
import csv
def graph(data):

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
        else:
            totalBytes += 1
           
        currtime = time - starttime
        times.append(time)
        totalCost.append(totalBytes) 
          

    plt.plot(times, totalCost, '.r-')


    plt.title('blah')
    plt.legend(loc=3)
    
    plt.xlabel("Time") 
    plt.ylabel("Total Cost")
    plt.show()
    #plt.savefig('../report/images/train/err_comps/%s.png' % tests[i], dpi=300)
    #plt.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Plot total communication cost vs time.')
    parser.add_argument('data_file')
    args = parser.parse_args()

    data = []
    with open(args.data_file, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            data.append(row)

    graph(data)
