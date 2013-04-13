# Standard imports
import json
import time

# Custom imports
import node
import messenger
import taskunit
import job

class Master(node.LocalNode):
    '''
    An instance of this class represents a master object who can assign work to
    its slaves after the job has been split up into work units.
    It then combines the results into the final expected result when it gets
    back the "intermediate results" from the slaves.
    '''
    def __init__(self, ip=None):
        '''
        FIXME: This param is unused for now. Maybe in the future we will need it
               in case we need to specify which interface to use.
        :param ip: Dot-delimited string representation of the ip of this node.
        '''
        super().__init__()

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
            msg = self.messenger.receive(return_payload=False)
            if msg is None:
                time.sleep(2)
                continue
            deserialized_msg = self.messenger.deserialize_message_payload(msg)
            if isinstance(deserialized_msg, job.Job):
                print("MASTER: Got a new job.")
                job_object = deserialized_msg
                for tu in job_object.splitter.split(job_object.input_file,
                                                    job_object.processor):
                    print("MASTER: Running job...")
                    tu.run()
                    print(tu.result)
                    print(tu.retries)
                    print(tu.state)
