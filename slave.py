import socket
import select

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

    def __init__(self, ip="", name="", master_combine=None, master_assign=None):
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

        master_combine_address = master_combine.get_address()
        self.sender_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sender_socket.setsockopt(socket.SOL_SOCKET,
                                      socket.SO_REUSEADDR,
                                      1)
        self.receiver_socket.bind(('0.0.0.0', 43400))

        master_assign_address = master_assign.get_address()
        self.receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.receiver_socket.setsockopt(socket.SOL_SOCKET,
                                        socket.SO_REUSEADDR,
                                        1)
        self.receiver_socket.bind(('0.0.0.0', 43401))

    def receiver(self):
        '''
        This method polls this slaves receiver_socket to see if we have received
        any task units to work on.
        If yes, it puts the task unit on our task_queue and polls the socket
        again.
        If no, it goes back to sleep for 1 second.
        '''
        epoll = select.epoll()
        epoll.register(self.receiver_socket.fileno(), select.EPOLLIN)

        current_data = ""
        current_executable = b""
        currently_receiving = None

        while True:
            poll_responses = epoll.poll(1)
            for _, event in poll_responses:
                # We received something on our receiver socket.
                if event & select.EPOLLIN:
                    # We receive 1024 bytes of data at a time
                    # and put it in a temporary "buffer".
                    # When we receive an END OF MESSAGE message,
                    # we clear the "buffer" and put it on to the "raw"
                    # message queue. Another thread picks it up, processes
                    # it (slightly) and puts it in the task_q.
                    msg, _ = self.receiver_socket.recvfrom(1024)
                    decoded_msg = msg.decode("UTF-8")
                    if decoded_msg == ">>> START OF DATA <<<"
                        currently_receiving = "data"
                        continue
                    elif decoded_msg == ">>> END OF DATA <<<":
                        currently_receiving = "executable"
                    elif decoded_msg == ">>> END OF EXECUTABLE <<<":
                        self.raw_message_q = (current_data, current_executable)
                        current_data = ""
                        current_executable = b""
                        currently_receiving = "none"
                    elif currently_receiving == "data":
                        current_data += decoded_msg
                    elif currently_receiving == "executable":
                        current_executable += msg
