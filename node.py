import json
import socket

STATES = ('UP',       # This Node is configured but isn't ready to accept
                      # any messages yet.
          'IDLE',     # This Node doesn't really have anything to do right
                      # now. It is ready to accepts tasks though.
          'WORKING',  # This Node is currently actively working.
          'DORMANT',  # The Node is temporarily down and will retry to get
                      # UP.
          'DEAD',)    # The Node has failed to get UP multiple times.
class Node(object):
    '''
    An instance of this class represents a machine in our distributed system
    cluster.
    '''
    def __init__(self, config_path=None, ip=None, hostname=None):
        if config_path:
            try:
                with open(config_path) as config_path_handler:
                    self.config = json.load(config_path_handler)
            except IOError as e:
                raise Exception("Failed to load config file " + config_path)

        self.ip = ip
        self.hostname = hostname

        self.status = 'UP'

    @staticmethod
    def get_this_hostname():
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
            raise Exception('Invalid state transition from ' +
                            self.state +
                            ' to ' +
                            state +
                            '.')
        else:
            self.state = state

    def get_state(self):
        '''
        Return the status of this node.
        '''
        return self.status
