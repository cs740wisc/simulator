# simulator
first implementation of simulator


SETUP:
    -need a mininet VM
    -sudo apt-get install screen
    -sudo apt-get install python-yaml
    
    -ssh with x forwarding to open up xterm
    




EXECUTION:

Run simulator:
sudo mn --custom topo-1sw-8hosts.py --topo mytopo

Execute nodes:
python node.py -i 10.0.0.2 -m 10.0.0.1 

Execute coordinator:
python master.py -i 10.0.0.1
