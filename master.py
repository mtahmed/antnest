# Standard imports
import json
import time
# Custom imports
import node
import messenger
import taskunit
import job

class Master(node.Node):
    '''
    An instance of this class represents a master object who can assign work to
    its slaves after the job has been split up into work units.
    It then combines the results into the final expected result when it gets
    back the "intermediate results" from the slaves.
    '''
    def __init__(self, ip=None, hostname=None):
        '''
        :param ip: The ip of this node.
        :param hostname: This node's hostname.
        '''
        self.ip = ip
        self.hostname = hostname

        self.pending_jobs = []
        self.completed_jobs = []
        self.taskunits = []

        self.messenger = messenger.Messenger()

    def process_job(self, new_job):
        '''
        This method takes a job as input and "processes" it in the sense
        that it generates TaskUnits from the Job and sends them off to the
        "right" Slaves to be processed.
        It then collects the results from our taskunits queue and combines
        them to get the final result.
        It then writes the results to a file and returns.
        '''
        for new_taskunit in new_job.splitter.split:
            print(new_taskunit)

    def worker(self):
        '''
        This method keeps running for the life of the Slave. It asks for new
        messages from this Slave's messenger. It then appropriately handles
        the message. Some of the messages are TaskUnits return from the slaves
        after they have been processed.

        Other messages maybe status updates from the slaves or requests to send
        more work etc.
        '''
        while True:
            new_msg = self.messenger.receive()
            if new_msg is None:
                time.sleep(2)
                continue
            if isinstance(new_msg, job.Job):
                print("MASTER: Got a new job.")
                job = new_msg
