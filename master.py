# Standard imports
import inspect
import json
import time

# Custom imports
import job
import messenger
import message
import node
import schedule
import taskunit


class Master(node.LocalNode):
    '''An instance of this class represents a master node.

    A master node assigns work to its slaves after the job has been split up
    into work units. It then combines the results into the final expected result
    when it gets back the "intermediate results" from the slaves.
    '''
    def __init__(self, port):
        '''
        :param port: port number to run this master on.
        '''
        super().__init__()

        self.config['port'] = port

        self.pending_jobs = []
        self.completed_jobs = []
        self.slave_nodes = []
        self.scheduler = schedule.MinMakespan()
        self.messenger = messenger.Messenger(port=self.config['port'])
        # A map of job_ids to Jobs.
        self.jobs = {}

    def process_job(self, job):
        '''Process a job received from the user.

        It generates TaskUnits from the Job and sends them off to the Slaves to
        be processed. It then collects the results and combines them to get the
        final result. It then writes the results to a file and returns.
        '''
        self.jobs[j.id] = j
        for tu in j.splitter.split(j.input_data, j.processor):
            # The split method only fills in the data and the processor.
            # So we need to manually fill the rest.
            processor_source = inspect.get_source(tu.processor)
            taskunit_id = taskunit.TaskUnit.compute_id(tu.data,
                                                        processor_source)
            tu.id = taskunit_id
            tu.job_id = j.id

            # Store this taskunit in the job's taskunit map.
            j.taskunits[tu.id] = tu

            # Now schedule this task unit.
            next_slave = self.scheduler.schedule_job(tu)
            slave_address = self.slave_nodes[next_slave].address

            # Attributes to send to the slave.
            attrs = ['id', 'job_id', 'data', 'processor._code',
                        'retries']
            self.messenger.send_taskunit(tu, slave_address, attrs)
        j.pending_taskunits = len(j.taskunits)

        return

    def worker(self):
        '''This method keeps running for the life of the Slave.

        It asks for new messages from this Slave's messenger. It then
        appropriately handles the message.

        Messages could be new jobs, processed task units from slaves, status
        updates from slaves etc.
        '''
        while True:
            address, msg = self.messenger.receive(return_payload=False)
            if msg is None:
                time.sleep(2)
                continue

            msg_type = msg.msg_type
            if msg_type == message.Message.MSG_STATUS:
                status = int(msg.msg_payload.decode('utf-8'))
                # If the slave is sending a STATUS_UP, then store it in our
                # slave_nodes array.
                print("MASTER: STATE_UP received from %s:%d" % address)
                if status == node.Node.STATE_UP:
                    self.slave_nodes.append(node.RemoteNode(None, address))
                    self.scheduler.add_machine()
                    self.messenger.register_destination('slave1', address)
            elif msg_type == message.Message.MSG_JOB:
                print("MASTER: Got a new job.")
                object_dict = msg.msg_payload.decode('utf-8')
                j = job.Job.deserialize(object_dict)
                self.process_job(j)
            elif msg_type == message.Message.MSG_TASKUNIT_RESULT:
                # TODO: MA Handle failed tasks.
                print("MASTER: Got a taskunit result back.")
                tu = deserialized_msg
                job_id = tu.job_id
                j = self.jobs[job_id]
                taskunit_id = tu.id
                j.taskunits[taskunit_id].result = tu.result
                j.taskunits[taskunit_id].state = tu.state
                j.pending_taskunits -= 1
                if j.pending_taskunits == 0:
                    j.combiner.add_taskunits(j.taskunits.values())
                    j.combiner.combine()
