import json
import socket

class Node(object):
    '''Represents a machine in our distributed system cluster.
    '''
    # A node can be in one of the following states:
    STATE_UP       = 0  # Configured but not ready to accept any messages.
    STATE_READY    = 1  # Ready to accept messages etc.
    STATE_WORKING  = 2  # Currently actively working.
    STATE_DORMANT  = 3  # Temporarily down and will try to get UP.
    STATE_DEAD     = 4  # Failed to get UP multiple times.

    VALID_STATES = [STATE_UP,
                    STATE_READY,
                    STATE_WORKING,
                    STATE_DORMANT,
                    STATE_DEAD]

    def __init__(self, hostname, address):
        # Set my hostname.
        self.hostname = hostname
        # Set my address.
        self.address = address
        # Set my state as UP.
        self.set_state(self.STATE_UP)

    def get_ip(self):
        '''Get the ip of this node.
        '''
        return self.address[0]

    def get_port(self):
        '''Get the port being used by this node.
        '''
        return self.address[1]

    def get_hostname(self):
        '''Get the hostname of this node.
        '''
        return self.hostname

    def set_state(self, state):
        '''Set the state of a Node.

        This method does some checking to make sure that the state transition
        is a valid transition.
        '''
        if state not in self.VALID_STATES:
            raise Exception("Invalid state %s" % str(state))
        self._state = state
        return

    def get_state(self):
        '''Ghe state of this node.
        '''
        return self._state


class RemoteNode(Node):
    '''Represents a remote node.

    It is to be used by e.g. a master node to keep track of a its slave nodes.
    '''
    def __init__(self, hostname, address):
        super().__init__(hostname, address)


class LocalNode(Node):
    '''Represents a local node.

    It is to be used on the machine that this class is instantiated to represent
    itself. A LocalNode must have a config_path defined.
    '''
    def __init__(self, config_path=None):
        ip = socket.gethostbyname(socket.getfqdn())
        hostname = socket.gethostname()

        super().__init__(hostname, ip)

        self.config = dict()
        if config_path:
            try:
                with open(config_path) as config_path_handler:
                    self.config = json.load(config_path_handler)
            except IOError:
                raise Exception("Failed to load config file " + config_path)
