# Standard imports
import socket
import time
import os

# Custom imports
import node
import messenger
import taskunit


class Slave(node.Node):
    '''
    This class defines a slave worker (a standalone machine)
    which can accept work units and process and send the results
    back.

    Although it says "send the results back" above, its not
    neccessarily true. We can ask (lol, more like tell) the slave
    to send the results back to another machine. This might be useful
    if the original job input is not neccessary to combine the results
    into a final result. This will allow one machine to only split larger jobs
    into smaller ones and dispatch them and another to combine the results
    from the slaves into a final result.
    '''

    def __init__(self,
                 ip=None,
                 name=None):
        '''
        :param ip: Dot-delimited representation of the ip of the slave.

        :param name: The hostname of the machine. (Note: This is not the FQHN)
        '''
        config_file = socket.gethostname() + "-slave-config.json"
        config_path = os.path.join('config', config_file)
        # __init__ Node
        super().__init__(config_path=config_path)

        self.task_q = []
        self.messenger = messenger.Messenger()

        if ip:
            split_ip = [int(i) for i in ip.split(".")]
            assert len(self.ip) == 4
            self.ip = ip
        if name:
            self.name = name

        for master in self.config["masters"]:
            self.messenger.register_destination(master['hostname'],
                                                (master['ip'], 33310))


    def worker(self):
        '''
        This method keeps running for the life of Slave. It asks for new
        messages from this Slave's messenger. It then appropriately handles the
        message. Some of the messages are TaskUnits that need to be run.

        If the message happens to be a TaskUnit, then this method either
        executes the run() method of the TaskUnit and waits for it to complete.
        It then sets the status of the TaskUnit appropriately and sends the it
        back to the master through the messenger.
        '''
        while True:
            new_msg = self.messenger.receive()
            if new_msg is None:
                time.sleep(2)
                continue
            if isinstance(new_msg, TaskUnit):
                task = new_msg
                # TODO MA Make this run in a new thread instead of directly here.
                try:
                    task.run()
                    self.messenger.send_taskunit(task)
                except Exception:
                    print("ERROR: Failed to run taskunit:", taskunit.getid())
