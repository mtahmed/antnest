import json
import socket
import os

class Node(object):
    '''
    An instance of this class represents a machine in our distributed system
    cluster.
    '''
    # A node can be in one of the following states:
    STATES = ('UP',          # Configured but not ready to accept any messages.
              'READY',       # Ready to accept messages etc.
              'WORKING',     # Currently actively working.
              'DORMANT',     # Temporarily down and will try to get UP.
              'DEAD'         # Failed to get UP multiple times.
             )

    def __init__(self, hostname, ip):
        # Set my hostname.
        self.hostname = socket.gethostname()
        # Set my ip.
        self.ip = ip
        # Set my state as UP.
        self.set_state('UP')

    def get_self_hostname():
        '''
        Get the hostname of the machine that this distributed system is running
        on.
        '''
        return self.hostname

    def get_ip(self):
        '''
        Get the ip of the Node that this object represents.
        '''
        return self.ip

    def get_hostname(self):
        '''
        Get te hostname of the Node that this object represents.
        '''
        return self.hostname

    def set_state(self, state):
        '''
        Always use this method to set the state of a Node. This does some
        checking to make sure that the state transition is a valid transition.
        '''
        if state not in self.STATES:
            raise Exception("Invalid state %s" % str(state))
        self.state = state
        return

    def get_state(self):
        '''
        Return the state of this node.
        '''
        return self.state


class RemoteNode(Node):
    '''
    This class represents a remote node.

    It is to be used by e.g. a master node to keep track of a its slave nodes.
    '''
    def __init__(self, hostname, ip):
        super().__init__(hostname, ip)


class LocalNode(Node):
    '''
    This class represents a local node.

    It is to be used on the machine that this class is instantiated to
    represent itself. A LocalNode must have a config_path defined.
    '''
    def __init__(self, config_path=None):
        ip = socket.gethostbyname(socket.getfqdn())
        hostname = socket.gethostname()

        if not config_path:
            config_filename = '%s-slave-config.json' % socket.gethostname()
            config_path = os.path.join('config', config_filename)
        try:
            with open(config_path) as config_path_handler:
                self.config = json.load(config_path_handler)
        except IOError:
            raise Exception("Failed to load config file " + config_path)

        super.__init__(hostname, ip)
