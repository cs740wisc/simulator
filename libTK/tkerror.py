

class TKError(Exception):
    """
        Exception class related to ParaDrop API calls.
    """
    def __init__(self, etype, msg):
        self.etype = etype
        self.msg = msg
    
    def __str__(self):
        return "TKError %s: %s" % (self.etype, self.msg)


