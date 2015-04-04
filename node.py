import sys, json, argparse, copy, signal, time, os
sys.path.append('/home/mininet/simulator/')

import libTK as libTK
from libTK import out, logPrefix, timeint
from libTK.utils import tkutils as tkutil
from libTK.comm.listener import TKListener

from libTK import settings

from twisted.internet.task import LoopingCall
from twisted.internet import reactor

class TKNode(TKListener):
    """
        Implementing the TKListener to handle everything
    """
    def __init__(self, lclreactor, *args, **kwargs):
        self.reactor = lclreactor

        # Make sure we call the real init function
        TKListener.__init__(self, lclreactor, *args, **kwargs)

    def connectionLost(self, proto, reason):
        """@overwrite
            This function overwrites the default stub function found in PDSender."""
        out.warn('-- %s connectionLost\n' % logPrefix())

    def connectionMade(self, proto):
        """@override:
            On connection made do this"""
        out.verbose('-- %s Made connection\n' % logPrefix())

    def dataReceivedFailure(self, proto, data, e):
        """@overwrite
            This function overwrites the default stub function found in PDSender."""
        out.err('-- %s dataReceivedFailure (%s) "%s"|%s\n' % (logPrefix(), proto.myid, str(e), data))


    def sendUpdateResponse(self, proto, msg, numTries=0):
        """Use this function to send a response back to the PDFC config server
            telling them that we completed the updates."""
        try:
            # Only try so many times to send it, then fail
            if(numTries > 10):
                out.fatal('!! %s Unable to sendUpdateResponse, retry limit reached, updateToken: %s, msg: %s\n' % (logPrefix(), updateToken, msg))
                return
            out.info('-- %s Sending update response: %s\n' % (logPrefix(), updateToken))
            #Encode this data to deal with transport JSON parsing issues (double quotes generated in error messages will cause SQL statements to fail)
            if(chuteid is None):
                proto.sendData({'msgType': 'apUpdateResponse', 'apid': self.apid, 'token': self.token, 'updateToken': updateToken, 'updateType':updateType, 'payload': msg})
            else:
                proto.sendData({'msgType': 'apUpdateResponse', 'apid': self.apid, 'chuteid': chuteid, 'token': self.token, 'updateToken': updateToken, 'updateType':updateType, 'payload': msg})
        except Exception as e:
            r = random.randint(4, 9)
            out.err('!! %s Unable to send update response, retrying in %d seconds, %s\n' % (logPrefix(), r, str(e)))

            # Retry sending, track how many times we did it
            numTries += 1
            self.reactor.callLater(r, self.sendUpdateResponse, updateToken, updateType, proto, msg, chuteid, numTries)


    ###########################################################################
    # Main Data Function
    ###########################################################################
    def dataReceived(self, proto, data):
        """@overwrite
            This function overwrites the default stub function found in PDSender."""
        print("DATA received: %s" % data)



def setupArgParse():
    p = argparse.ArgumentParser(description='Daemon for ParaDrop Framework Control Configuration server')
    p.add_argument('-s', '--settings', help='Setting string to overwrite from settings.py, format is "KEY:VALUE"', action='append', type=str)
    p.add_argument('-p', '--port', help='Port to listen on', type=int, default=10000)

    return p

if (__name__ == "__main__"):

    p = setupArgParse()
    args = p.parse_args()

    out.info('-- %s Starting listening on port: %s\n' % (logPrefix(), args.port))
    tkn = TKNode(reactor, args.port)
    reactor.run()
    """
	# Setup the signal handler
	signal.signal(signal.SIGUSR1, caughtSIGUSR1)

	# Get stuff out of the arguments
	p = setupArgParse()
	args = p.parse_args()

	settingsList = args.settings
	if(settingsList):
	out.info("-- Overwriting settings with arguments...\n")
	settings.updateSettingsList(settingsList)

	if(args.verbose):
	caughtSIGUSR1(signal.SIGUSR1, None)

	# Paradrop DB
	if(args.development):
	out.info('-- %s Using DEVELOPMENT variables\n' % logPrefix())
	dbh = pdsql.PDSql(dbsettings.DEV_DB_UNAME, dbsettings.DEV_DB_PASSWD, dbsettings.DEV_DB_HOST, dbsettings.DEV_DB_DBNAME, cursorType=pdsql.CLIENT_SIDE)
	thePort = settings.FC_CONFIGBLOCK_PORT + 10000
	else:
	dbh = pdsql.PDSql(dbsettings.PROD_DB_UNAME, dbsettings.PROD_DB_PASSWD, dbsettings.PROD_DB_HOST, dbsettings.PROD_DB_DBNAME, cursorType=pdsql.CLIENT_SIDE)
	thePort = settings.FC_CONFIGBLOCK_PORT

	# Unittest mode
	if(args.unittest):
	out.info('-- %s Using UNITTEST variables\n' % logPrefix())
	thePort = settings.FC_CONFIGBLOCK_PORT + 20000
	pddb.setUnittestMode()

	# COAP DB
	dbh_coap = pdsql.PDSql(dbsettings.COAP_DB_UNAME, dbsettings.COAP_DB_PASSWD, dbsettings.COAP_DB_HOST, dbsettings.COAP_DB_DBNAME, cursorType=pdsql.CLIENT_SIDE)

	# Setup actual server
	pdl = VNetServer(dbh, dbh_coap, reactor, thePort)

	reactor.run()
    """
