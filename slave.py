# Standard imports
import os
import socket
import time

# Custom imports
import messenger
import message
import node
import taskunit


class Slave(node.LocalNode):
    '''An instance of this class represents a slave node.

    A slave node can accept work units from a master and process and send the
    results back.
    '''
    def __init__(self, port, ip=None):
        '''
        :param port: port number to run this slave on.
        '''
        config_filename = '%s-slave-config.json' % socket.gethostname()
        config_path = os.path.join('config', config_filename)

        super().__init__(config_path=config_path)

        self.task_q = []
        self.master_nodes = []
        self.config['port'] = port

        self.messenger = messenger.Messenger(port=self.config['port'])

        for master in self.config['masters']:
            master_hostname = master['hostname']

            try:
                master_port = master['port']
            except KeyError:
                master_port = messenger.Messenger.DEFAULT_PORT

            try:
                master_ip = master['ip']
            except:
                master_ip = socket.gethostbyname(master['hostname'])

            self.master_nodes.append(node.RemoteNode(master_hostname,
                                                     (master_ip, master_port)))
            self.messenger.register_destination(master_hostname,
                                                (master_ip, master_port))

        # When everything is setup, associate with the master.
        self.associate()

        return

    def associate(self):
        '''Associate with the master(s).

        This involves sending a status update to the master.
        '''
        unacked_masters = self.master_nodes
        num_unacked_masters = len(unacked_masters)
        trackers = [None] * len(unacked_masters)
        while num_unacked_masters > 0:
            for index, master in enumerate(unacked_masters):
                if master is None:
                    continue
                tracker = trackers[index]
                if tracker is None:
                    tracker = self.messenger.send_status(node.Node.STATE_UP,
                                                         master.address,
                                                         track=True)
                    trackers[index] = tracker
                    continue
                elif tracker.state != message.MessageTracker.MSG_ACKED:
                    self.messenger.send_status(node.Node.STATE_UP,
                                               master.address)
                elif tracker.state == message.MessageTracker.MSG_ACKED:
                    self.messenger.delete_tracker(tracker)
                    unacked_masters[index] = None
                    num_unacked_masters -= 1
            time.sleep(10.0)
        return

    def worker(self):
        '''The main worker loop.

        This method keeps running for the life of Slave. It asks for new
        messages from this Slave's messenger. It then appropriately handles the
        message. Some of the messages are TaskUnits that need to be run.

        If the message happens to be a TaskUnit, then this method
        executes the run() method of the TaskUnit and waits for it to complete.
        It then sets the status of the TaskUnit appropriately and sends the it
        back to the master through the messenger.
        '''
        while True:
            address, msg = self.messenger.receive(return_payload=False)
            if msg is None:
                time.sleep(2)
                continue
            deserialized_msg = self.messenger.deserialize_message_payload(msg)
            if msg.msg_type == message.Message.MSG_TASKUNIT:
                tu = deserialized_msg
                # TODO(mtahmed): Run this in a new thread? Maybe? Investigate.
                tu.run()
                self.messenger.send_taskunit_result(tu, address)
