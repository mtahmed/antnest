import json
import socket

class Node(object):
    '''
    An instance of this class represents a machine in our distributed system
    cluster.

    A node can be in one of the following states:
        UP: This Node is configured but isn't ready to accept any messages yet.
        READY: This Node is ready to accept messages etc.
        WORKING: This Node is currently actively working.
        DORMANT: The Node is temporarily down and will retry to get UP.
        DEAD: The Node has failed to get UP multiple times.

    Configuration is done in the following decreasing order of preference:
    - __init__ paramter
    - config file
    - socket resolution
    '''
    STATES = ('UP',
              'READY',
              'WORKING',
              'DORMANT',
              'DEAD')

    def __init__(self, config_path=None, ip=None):
        if config_path:
            try:
                with open(config_path) as config_path_handler:
                    self.config = json.load(config_path_handler)
            except IOError as e:
                raise Exception("Failed to load config file " + config_path)

        # Set my hostname.
        if self.config['hostname'] is not "":
            self.hostname = self.config['hostname']
        else:
            self.hostname = socket.gethostname()

        # Set my ip.
        if ip:
            self.ip = ip
        elif self.config['ip'] is not "":
            self.ip = self.config['ip']
        else:
            self.ip = socket.gethostbyname(socket.getfqdn())

        # Set my state as UP.
        self.set_state('UP')

        return

    @staticmethod
    def get_self_hostname():
        '''
        Get the hostname of the machine that this distributed system is running
        on.
        '''
        return socket.gethostname()

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
