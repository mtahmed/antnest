# Standard imports
import socket
import time
import os

# Custom imports
import node
import messenger
import taskunit


class Slave(node.Node):
    """
    This class defines a slave worker (a standalone machine)
    which can accept work units and process and send the results
    back.
    """

    def __init__(self, ip=None):
        """
        :param ip: Dot-delimited representation of the ip of the slave.
        """
        config_filename = '%s-slave-config.json' % socket.gethostname()
        config_path = os.path.join('config', config_filename)
        # __init__ Node
        super().__init__(config_path=config_path, ip=ip)

        self.task_q = []
        self.messenger = messenger.Messenger()

        for master in self.config["masters"]:
            if master['ip'] is not "":
                master_ip = master['ip']
            else:
                master_ip = socket.gethostbyname(master['hostname'])
            if master['hostname'] is '':
                raise Exception("The master hostname must always be present.")
            PORT = messenger.Messenger.DEFAULT_PORT
            self.messenger.register_destination(master['hostname'],
                                                (master_ip, PORT))


    def worker(self):
        """
        This method keeps running for the life of Slave. It asks for new
        messages from this Slave's messenger. It then appropriately handles the
        message. Some of the messages are TaskUnits that need to be run.

        If the message happens to be a TaskUnit, then this method
        executes the run() method of the TaskUnit and waits for it to complete.
        It then sets the status of the TaskUnit appropriately and sends the it
        back to the master through the messenger.
        """
        while True:
            msg = self.messenger.receive(return_payload=False)
            if msg is None:
                time.sleep(2)
                continue
            deserialized_msg = self.messenger.deserialize_message_payload(msg)
            if isinstance(deserialized_msg, TaskUnit):
                task = deserialized_msg
                # TODO MA Make this run in a new thread instead of directly here.
                try:
                    task.run()
                    if msg.return_address:
                        self.messenger.send_taskunit(task, msg.return_address)
                    else:
                        self.messenger.send_taskunit(task, msg.recvfrom)
                except Exception:
                    print("Failed to run taskunit:", taskunit.getid())
