# Standard imports
import socket
import select
import time

# Custom imports
import messenger
import taskunit


class Slave(object):
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
                 config,
                 ip="",
                 name="",
                 master_combine=None,
                 master_assign=None):
        '''
        :type ip: str
        :param ip: Dot-delimited representation of the ip of the slave.

        :type name: str
        :param name: The hostname of the machine. (Note: This is not the FQHN)

        :type master_combine: Master
        :param master_combine: An instance of a Master who can combine the
        results from the different slaves into one final result. This is the
        master we send our completed (processed) results to.

        :type master_assign: str
        :param ip: An instance of a Master who can combine split up jobs and
        assign the resulting task units to slaves. This is the master that will
        assign us some work to do.
        '''
        if master_combine is None or master_assign is None:
            self.dummy = True
        if not ip:
            raise Exception("No ip provided to create an instance of slave.")
        else:
            split_ip = [int(i) for i in ip.split(".")]
            assert len(self.ip) == 4
            self.ip = ip
        self.name = name
        self.master_combine = master_combine
        self.master_assign = master_assign

        self.task_q = []

        destination = master_combine.get_address()
        self.sender_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sender_socket.setsockopt(socket.SOL_SOCKET,
                                      socket.SO_REUSEADDR,
                                      1)
        self.receiver_socket.bind(('0.0.0.0', 33100))

        source = master_assign.get_address()
        self.receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.receiver_socket.setsockopt(socket.SOL_SOCKET,
                                        socket.SO_REUSEADDR,
                                        1)
        self.receiver_socket.bind(('0.0.0.0', 33101))

        self.messenger = messenger.Messenger(sender_socket,
                                             destination,
                                             receiver_socket
                                             source)

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
                time.sleep(3)
                continue
            if isinstance(new_msg, TaskUnit):
                taskunit = new_msg
                # TODO Make this run in a new thread instead of directly here.
                try:
                    taskunit.run()
                    self.
                except Exception:
                    print("ERROR: Failed to run taskunit:", taskunit.getid())
