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

        messenger_type = messenger.ZMQMessenger.TYPE_CLIENT
        self.messenger = messenger.ZMQMessenger(type=messenger_type,
                                                port=self.config['port'])
        self.messenger.start()

        for master in self.config['masters']:
            master_hostname = master['hostname']

            try:
                master_port = master['port']
            except KeyError:
                master_port = messenger.UDPMessenger.DEFAULT_PORT

            try:
                master_ip = master['ip']
            except:
                master_ip = socket.gethostbyname(master['hostname'])

            self.master_nodes.append(node.RemoteNode(master_hostname,
                                                     (master_ip, master_port)))
            self.messenger.register_destination(master_hostname,
                                                (master_ip, master_port))

        # When everything is setup, associate with the master(s).
        self.associate()

        return

    def associate(self):
        '''Associate with the master(s).

        This involves sending a status update to the master.
        '''
        for master in self.master_nodes:
            self.messenger.connect(master.address)
            self.messenger.ping(master.address)

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
        for address, msg in self.messenger.receive(deserialize=False):
            #msg_type = msg.msg_type

            if msg == 'PONG':
                print("SLAVE: PONG from %s:%d" % address)
            elif msg['class'] == 'taskunit.TaskUnit':
                #object_dict = msg.msg_payload.decode('utf-8')
                tu = taskunit.TaskUnit.deserialize(msg)
                # TODO(mtahmed): Run this in a new thread? Maybe? Investigate.
                tu.run()
                self.messenger.send_taskunit_result(tu, address)
