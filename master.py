# Custom imports
import node
import taskunit

class Master(node.Node):
    '''
    An instance of this class represents a master object who can assign work to
    its slaves after the job has been split up into work units.
    It then combines the results into the final expected result when it gets
    back the "intermediate results" from the slaves.
    '''

    def __init__(self,
                 ip=None,
                 hostname=None):
        self.ip = ip
        self.name = name

        super().__init__()
