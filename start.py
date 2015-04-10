#!/usr/bin/python

import json, argparse, sys
sys.path.append('/home/mininet/simulator/')

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel
from mininet.cli import CLI
from libTK import settings



class SingleSwitchTopo(Topo):
    "Single switch connected to n hosts."
    def build(self, n=2):
        mainSwitch = self.addSwitch('s0')
        coord = self.addHost('c0')
        self.addLink(coord, mainSwitch)

        # Python's range(N) generates 0..N-1
        for i in range(n):
            host = self.addHost('h%s' % (i + 1))
            gen = self.addHost('g%s' % (i + 1))
            sw = self.addSwitch('s%s' % (i + 1))

            self.addLink(sw, mainSwitch)
            self.addLink(gen, sw)
            self.addLink(host, sw)

def simpleTest(topk, nodes):
    "Create and test a simple network"
    topo = SingleSwitchTopo(nodes)
    net = Mininet(topo)
    net.start()

    ips = {}

    c = 'c0'
    ips[c] = net.get(c).IP() 
    
    # GET ALL ips of hosts for coordinator to read
    for i in range(nodes):
        h = 'h%s' % (i + 1)
        ips[h] = net.get(h).IP() 
        g = 'g%s' % (i + 1)
        ips[g] = net.get(g).IP() 
        
    # Save all ips to file, so each program can access them 
    json.dump(ips, open(settings.FILE_SIMULATION_IPS, 'w'))


    # START THE HOSTS
        

 
    CLI(net)

    

    net.stop()

def setupArgParse():
    p = argparse.ArgumentParser(description='Daemon for ParaDrop Framework Control Configuration server')
    p.add_argument('-k', '--topk', help='Top k objects', type=int, default=1)
    p.add_argument('-n', '--nodes', help='Number of nodes', type=int, default=8)

    return p

if __name__ == '__main__':

    p = setupArgParse()
    args = p.parse_args()

    # Tell mininet to print useful information
    setLogLevel('info')
    simpleTest(args.topk, args.nodes)
