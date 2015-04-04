##################################################################
# Copyright 2013-2014 All Rights Reserved
# Authors: The Paradrop Team
###################################################################

"""
    This file contains any settings required by ANY and ALL modules of the paradrop system.
    They are defaulted to some particular value and can be called by any module in the paradrop
    system with the following code:

        from paradrop import settings
        print(settings.STUFF)

    These settings can be overriden by a file defined which contains the following syntax:

        # This changes a string default setting
        EXACT_SETTING_NAME0 "new string setting"
        
        # This changes a int default setting
        EXACT_SETTING_NAME1 int0

    If settings need to be changed, they should be done so by the initialization code
    (such as pdfcd, pdapi_server, pdfc_config, etc...)

    This is done by calling the following function:
        settings.updateSettings(filepath)
"""

import os, re, sys

#
# comm.pdsender.pdsenderreconnect
#
RECONNECT_MAX_DELAY = 15
RECONNECT_INIT_DELAY = 5
RECONNECT_FACTOR = 2
RECONNECT_JITTER = 0.2

#
# message server (network out) 
#
MSG_SERVER_ADDR = "10.6.0.1"
MSG_SERVER_PORT = 10500
MSG_SERVER_LOG_FILE = '/data/log/pd_network.log'

#
# vnet.vnet_server
#
CONTROL_TIMEOUT = 20
CONTROL_UPDATERATE = 5

#
# fc.configblock
#
FC_CONFIGBLOCK_ADDR = "10.6.0.1"
FC_CONFIGBLOCK_PORT = 10101
FC_CONFIGBLOCK_FLUSH_TIMEOUT = 120
FC_CONFIGBLOCK_FLUSH_PERIOD = 30
FC_CONTROLBLOCK_PING_PERIOD = 30
FC_UPDATE_LIST_SIZE = 10
FC_CONFIGBLOCK_PENDING_PERIOD = 60
FC_CONFIGBLOCK_PENDING_TIMEOUT = 60

###############################################################################
# Helper functions
###############################################################################

def parseValue(key):
    """
        Description:
            Attempts to parse the key value, so if the string is 'False' it will parse a boolean false.

        Arguments:
            @key : the string key to parse

        Returns:
            The parsed value. If no parsing options are available it just returns the same string.
    """
    # Is it a boolean?
    if(key == 'True'):
        return True
    if(key == 'False'):
        return False
    
    # Is it None?
    if(key == 'None'):
        return None

    # Is it a float?
    if('.' in key):
        try:
            f = float(key)
            return f
        except:
            pass
    
    # Is it an int?
    try:
        i = int(key)
        return i
    except:
        pass

    # Otherwise, its just a string:
    return key

def updateSettingsFile(filepath):
    """
        Description:
            Take the file path provided, read and replace any setting defined with the contents of the file.
        
        Arguments:
            @filepath: The path to the settings file

        Returns:
            None

        Throws:
            PDError
    """
    # Find the file
    if(not os.path.exists(filepath)):
        raise PDError('SettingsFileMissing', "No filepath %s" % filepath)

    # Compile up the regexes
    strRegex = re.compile(r'^(.*) \"(.*)\"$')
    intRegex = re.compile(r'^(.*) ([0-9]*)$')
    fltRegex = re.compile(r'^(.*) ([0-9]*\.[0-9]*)$')

    # Get a handle to our settings defined above
    settingsModule = sys.modules[__name__]

    # Open and read in the file
    with open(filepath) as fd:
        while(True):
            line = fd.readline().rstrip()
            if(not line):
                break

            # Skip comments
            if(line.startswith('#')):
                continue


            # Interpret the line
            mats = strRegex.match(line)
            mati = intRegex.match(line)
            matf = fltRegex.match(line)
            
            # Is it a string?
            if(mats):
                word = mats.group(1)
                ans = mats.group(2)
            
            # Is it a int?
            elif(mati):
                word = mati.group(1)
                ans = mati.group(2)
            
            # Is it a float?
            elif(matf):
                word = matf.group(1)
                ans = matf.group(2)
            
            else:
                # Getting here is an error
                raise PDError('SettingsFileSyntax', "Bad syntax: %s" % line)

            # We can either replace an existing setting, or set a new value, we don't care
            setattr(settingsModule, word, parseValue(ans))


def updateSettingsList(slist):
    """
        Description:
            Take the list of key:value pairs, and replace any setting defined.
        
        Arguments:
            @slist: The key:value pairs

        Returns:
            None

        Throws:
            PDError
    """
    # Get a handle to our settings defined above
    settingsModule = sys.modules[__name__]
    for kv in slist:
        k,v = kv.split(':')
        # We can either replace an existing setting, or set a new value, we don't care
        setattr(settingsModule, k, parseValue(v))
