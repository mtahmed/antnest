class Master(object):
    '''
    An instance of this class represents a master object who can assign work to
    its slaves after the job has been split up into work units.
    It then combines the results into the final expected result when it gets
    back the "intermediate results" from the slaves.
    '''

    def __init__(self, ip="", name="", slaves=[]):
        if slaves is []:
            self.dummy = True
        self.ip = [int(i) for i in ip.split(".")]
        assert len(self.ip) == 4
        self.name = name
        self.slaves = slaves

    def get_ip(self):
        return self.ip

    def get_name(self):
        return self.name
