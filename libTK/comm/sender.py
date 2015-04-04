import json, sys, traceback, time, urllib
from twisted.internet import reactor
from twisted.internet.protocol import Protocol, ClientFactory, ReconnectingClientFactory
from twisted.protocols.basic import LineReceiver

#from lib.paradrop import timeflt 
from libTK import settings

#TODO : find a better way to do this than copying out functions from lib/paradrop/__init__/py
def convertUnicode(elem):
    """Converts all unicode strings back into UTF-8 (str) so everything works.
        Call this function like:
            json.loads(s, object_hook=convertUnicode)"""
    if isinstance(elem, dict):
        return {convertUnicode(key): convertUnicode(value) for key, value in elem.iteritems()}
    elif isinstance(elem, list):
        return [convertUnicode(element) for element in elem]
    elif isinstance(elem, unicode):
        return elem.encode('utf-8')
    #DFW: Not sure if this has to be here, but deal with possible "null" MySQL strings
    elif(elem == 'null'):
        return None
    else:
        return elem

def urlEncodeMe(elem, safe=' '):
    """
        Converts any values that would cause JSON parsing to fail into URL percent encoding equivalents.
        This function can be used for any valid JSON type including str, dict, list.
        Returns:
            Same element properly encoded.
    """
    # What type am I?
    if isinstance(elem, dict):
        return {urlEncodeMe(key, safe): urlEncodeMe(value, safe) for key, value in elem.iteritems()}
    elif isinstance(elem, list):
        return [urlEncodeMe(element, safe) for element in elem]
    elif isinstance(elem, str):
        # Leave spaces alone, they are save to travel for JSON parsing
        return urllib.quote(elem, safe)
    else:
        return elem

def urlDecodeMe(elem):
    """
        Converts any values that would cause JSON parsing to fail into URL percent encoding equivalents.
        This function can be used for any valid JSON type including str, dict, list.
        Returns:
            Same element properly decoded.
    """
    # What type am I?
    if isinstance(elem, dict):
        return {urlDecodeMe(key): urlDecodeMe(value) for key, value in elem.iteritems()}
    elif isinstance(elem, list):
        return [urlDecodeMe(element) for element in elem]
    elif isinstance(elem, str):
        # Leave spaces alone, they are save to travel for JSON parsing
        return urllib.unquote(elem)
    else:
        return elem

def json2str(j):
    """
        Properly converts and encodes all data related to the JSON object into a string format
        that can be transmitted through a network and stored properly in a database.
    """
    return json.dumps(urlEncodeMe(j), separators=(',', ':'))

def str2json(s):
    t = json.loads(s, object_hook=convertUnicode)
    # If t is a list, object_hook was never called (by design of json.loads)
    # deal with that situation here
    if(isinstance(t, list)):
        t = [convertUnicode(i) for i in t]
    # Make sure to still decode any strings
    return urlDecodeMe(t)

timeflt = lambda: time.time()
# End of copied functions from lib/paradrop/__init__.py

class TKSenderProtocol(LineReceiver):
    """
        ParaDropSenderProtocol class.
        This class is the low level Protocol object that works as the sender object.
        
        It mainly makes calls back to the Factory (TKSender) which allows functions
        to be overwritten by the implementer.
    """
    def __init__(self, addr, f):
        self.factory = f
        self.myhost = addr.host
        self.myport = addr.port
        self.myid = '%s:%d' % (self.myhost, self.myport)
    
    def logPrefix(self):
        return '[%s-%s @ %.2f]' % (self.factory.logprefix, self.myid, timeflt())

    def connectionMade(self):
        """@override
            Initial connection should generate a token and send a updateReqest to the master."""
        self.factory.connectionMade(self)
    
    def connectionLost(self, reason):
        """@override:
            On a lost connection this is called"""
        self.factory.connectionLost(self, reason)
    
    def lineReceived(self, data):
        """@override:
            Main function called by the LineReceiver class when it has a full line of data."""
        # JSONize the data first
        try:
            data = str2json(data.rstrip())
        except Exception as e:
            self.factory.dataReceivedFailure(self, data.rstrip(), e)
        self.factory.dataReceived(self, data)
    
    def sendData(self, data):
        """Function that transmits data back to the client using the protocol @self."""
        # unJSONize the data first
        # If an exception happens thats ok, we want the caller to know
        if(type(data) is not str):
            data = json2str(data)
        self.transport.write(data + "\r\n")

class TKSender(ClientFactory):
    """
        ParaDropSender class.
        This class is meant to be a wrapper around the Python Twisted framework.

        The implementer is expected to overwrite FOUR functions below which allows them to
        easily implement a Sender class using the Python Twisted event driven style structure.

        You should write a class which implements this TKSender class.
        This class should overwrite the following functions:

        - connectionMade(self, proto)
            When a new connection is made, this is the very first function called which lets us know.
        - connectionLost(self, proto, reason)
            When a protocol looses a connection to a client this function is called.
        - dataReceived(self, proto, data)
            When a client sends data via this protocol this function is called with @data as a string.
        - dataReceivedFailure(self, proto, data, e)
            When a client sends data that JSON cannot loads without error, this function is called.

        You do not have to overwrite all of these functions, for instance, if you do not care
        that a connection is lost then ignore the connectionLost function.
    """

    def __init__(self, lclreactor, addr, port, logprefix="TKSENDER", verbose=False):
        if(lclreactor):
            self.reactor = lclreactor
        else:
            self.reactor = reactor
        self.addr = addr
        self.port = port
        self.logprefix = logprefix
        self.verbose = verbose
        
        # Setup the listen
        self.reactor.connectTCP(self.addr, self.port, self)

    def logPrefix(self):
        """@override"""
        if(self.verbose):
            line = sys._getframe(1).f_lineno
            return '[%s(%d) @ %.2f]' % (self.logprefix, line, timeflt())
        else:
            return '[%s @ %.2f]' % (self.logprefix, timeflt())
    
    def buildProtocol(self, addr):
        """@override"""
        return TKSenderProtocol(addr, self)

    def connectionMade(self, proto):
        """@override:
            On connection made do this"""
        pass

    def dataReceived(self, proto, data):
        """@override:
            Main function which receives data from outside world"""
        pass
    
    def dataReceivedFailure(self, proto, data, e):
        """@override:
            On a JSON.loads failure, this function is called with the string form of the @data and the @proto it came from.
            It also contains the Exception object generated by the failure."""
        pass

    def connectionLost(self, proto, reason):
        """@override:
            On a lost connection this is called"""
        pass

class TKSenderReconnect(ReconnectingClientFactory, TKSender):
    """
        ParaDropSenderReconnect class.
        This class implements the standard TKSender interface as well as the ReconnectingClientFactory class.
    """
    def __init__(self, lclreactor, addr, port, maxDelay=None, initialDelay=None, factor=None, jitter=None, logprefix="TKSENDER", verbose=False):
        if(lclreactor):
            self.reactor = lclreactor
        else:
            self.reactor = reactor
        self.addr = addr
        self.port = port
        self.logprefix = logprefix
        self.verbose = verbose

        #Setup internals for reconnecting
        self.maxDelay = maxDelay or settings.RECONNECT_MAX_DELAY
        self.initialDelay = initialDelay or settings.RECONNECT_INIT_DELAY
        self.factor = factor or settings.RECONNECT_FACTOR
        self.jitter = jitter or settings.RECONNECT_JITTER
        
        # Setup the connect
        self.reactor.connectTCP(self.addr, self.port, self)
        #out.info('-- %s Setting up\n' % (self.logPrefix()))
        self.proto = None
    def connectionMade(self, proto):
        """@override:
            On connection made do this"""
        self.proto = proto

    def clientConnectionLost(self, connector, reason):
        """@override"""
        #out.warn('** %s Connection lost, reason: %s\n' % (self.logPrefix(), str(reason).rstrip()))
        return ReconnectingClientFactory.clientConnectionLost(self, connector, reason)
    
    def buildProtocol(self, addr):
        """@override
            This overrides the basic TKSender function because we must reset the delay when we connect."""
        self.resetDelay()
        self.proto = TKSenderProtocol(addr, self)
        return self.proto

class TKSenderOneShot(TKSender):
    """
        ParaDropSenderOneShot class.
        This class implements the standard TKSender interface but is designed to send one packet of data then destroy itself.
        
        It requires a few extra parameters to implement:
            @data : The data to be sent
            @timeout: Time to wait for a response before killing the connection, (if @callback != None)
            @callback: An optional callback function to be used if an ACK is expected.
            @cbArgs: An optional set of arguments to be used when calling the callback function.

        If implemented, the callback function will be called like:
        
            callback(cbArgs, data)
        
        Where @data is the response from the other node.
    """
    def __init__(self, lclreactor, addr, port, data, callback=None, cbArgs=None, timeout=5, logprefix="TKSENDERONESHOT", verbose=False):
        if(lclreactor):
            self.reactor = lclreactor
        else:
            self.reactor = reactor
        self.addr = addr
        self.port = port
        self.data = data
        self.timeout = timeout
        self.callback = callback
        self.cbArgs = cbArgs
        self.logprefix = logprefix
        self.verbose = verbose

        # Setup the connect
        self.reactor.connectTCP(self.addr, self.port, self)
    
    def killConnection(self, proto):
        """Setup to close the connection of the proto provided."""
        if(proto):
            proto.transport.loseConnection()
    
    def connectionMade(self, proto):
        """@override:
            On connection made do this"""
        proto.sendData(self.data)
        # Now check the callback function, if None loose the connection
        if(self.callback):
            self.reactor.callLater(self.timeout, killConnection, proto)
        else:
            self.killConnection(proto)

    def dataReceived(self, proto, data):
        """@override:
            Main function which receives data from outside world"""
        # See if we were supposed to do anything with the data received
        if(self.callback):
            self.callback(self.cbArgs, data)
    
    def dataReceivedFailure(self, proto, data, e):
        """@override:
            On a JSON.loads failure, this function is called with the string form of the @data and the @proto it came from.
            It also contains the Exception object generated by the failure."""
        #TODO probably should let the implementer add an errcallback function just like with deferreds
        pass
