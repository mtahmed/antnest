# Standard imports
import time
import select
import socket

# Custom imports
import serialize
import message


class Messenger(object):
    '''
    An instance of this class represents an object that "serves" a node to
    communicate with other nodes (e.g. a slave's messenger will allow it to
    send completed task units back to the master).

    The messenger can be given an instance of a TaskUnit to send and it will
    take care of the serialization.
    '''
    def __init__(self,
                 port=33100):
        self.serializer = serialize.Serializer()
        self.port = port

        # Create the sockets.
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind('0.0.0.0', 33310)

        # Dictionaries and lists.
        self.hostname_to_ip = {}
        # The message ids for each of the remote destinations. The ids are
        # incremented as we go.
        self.msg_ids = {}
        # NOTE: inbound_queue is a list because we don't care who the message is
        # received from, we just want the next message when receive is called.
        # NOTE: outbound_queue, however, is a dictionary where the keys are the
        # destination of the message and values are the messages.
        self.inbound_queue = []
        self.outbound_queue = {}

    def get_ip_from_hostname(self, hostname):
        '''
        Return the ip address for the hostname `hostname`.
        '''
        return self.hostname_to_ip(hostname)

    def get_next_msg_id(self, hostname):
        '''
        Return the next message id for the hostname `hostname` and increment
        the message id by 1.
        '''
        msg_id = self.msg_ids[hostname]
        self.msg_ids[hostname] += 1
        return msg_id

    def register_destination(self, hostname, ip):
        '''
        Store the hostname as key with address as value for this destination
        so that the caller can later only supply destination as hostname
        to communicate with the destination.
        '''
        self.hostname_to_ip[hostname] = ip
        self.outbound_queue[hostname] = []
        self.msg_ids[hostname] = 0

    def receive(self):
        '''
        This method checks this messenger's inbound_queue and if its not empty, it
        returns the next element from the queue.
        '''
        if len(inbound_queue) > 0:
            msg = inbound_queue[0]
            obj = self.serializer.deserialize(msg)
            self.inbound_queue = self.inbound_queue[1:]
            return obj
        else:
            return None

    def send_taskunit(self, taskunit, dest_hostname):
        '''
        This method can be used to send a taskunit to a remote node.
        '''
        msg_id = self.get_next_msg_id(dest_hostname)
        serialized_taskunit = self.serializer.serialize(taskunit)
        messages = message.packed_messages_from_data(msg_id,
                                                     message.MSG_TASKUNIT,
                                                     serialized_taskunit)
        self.queue_for_sending(messages, dest_hostname)

    def send_status(self, status, dest_hostname):
        '''
        This method can be used to send the status to a remote node.
        '''
        msg_id = self.get_next_msg_id(dest_hostname)
        messages = message.packed_messages_from_data(msg_id,
                                                     message.MSG_STATUS_NOTIFY,
                                                     status)
        self.queue_for_sending(messages, dest_hostname)
                                                     
    def queue_for_sending(self, messages, dest_hostname):
        '''
        This method appends the new messages to the list for the dest in the
        outbound_queue.
        NOTE: This method takes a list of messages and not a single message.
        '''
        dest_ip = self.get_ip_from_hostname(dest_hostname)
        self.outbound_queue[dest_ip].extend(messages)

    def sender(self):
        '''
        This method watches the outbound task_q to see if we have any outbound
        messages and if we do, it picks up the message and sends it to its
        destination.
        In doing so, it also has to poll the sender_socket to make sure that
        we can send data.

        NOTE: This method goes over the list of destinations in a round-robin
        way and for each of them, it picks up the first outbound message for
        that destination and sends it out.
        '''
        poller = select.epoll()
        poller.register(self.sender_socket.fileno(),
                        select.EPOLLOUT | select.EPOLLET)  # Edge-triggered.

        while True:
            for dest in outbound_queue.keys():
                if len(self.outbound_queue[dest]) > 0:
                    outbound_msg = self.outbound_queue[dest][0]
                    # While the msg is still not sent...
                    while outbound_msg is not None:
                        # Poll with timeout of 1.0 seconds.
                        poll_responses = poller.poll(1.0)
                        for _, event in poll_responses:
                            # If we can send...
                            if event & select.EPOLLOUT:
                                bytes_sent = sender_socket.sendall(outbound_msg)
                                self.outbound_queue[dest] = outbound_queue[dest][1:]
                                outbound_msg = None
                                break
                            else:
                                print("WARNING: Unexpected event on sender socket")
                        else:
                            # Sleep for 3.0 seconds if we didn't get any event.
                            time.sleep(0.50)
            else:
                # Our outbound_queue has no destinations yet.
                time.sleep(3)  # Sleep for 3 seconds

    def receiver(self):
        '''
        This method polls the receiver_socket to see if it have received any
        messages.
        If yes, it puts the raw message on our inbound_queue and goes back to
        polling the socket.
        Sleeps for 3 seconds otherwise.
        '''
        poller = select.epoll()
        poller.register(self.receiver_socket.fileno(),
                        select.EPOLLIN | select.EPOLLET)  # Edge-triggered.

        buff = ""

        while True:
            # Poll with timeout of 1.0 seconds.
            poll_responses = poller.poll(1.0)
            for fileno, event in poll_responses:
                # We received something on our receiver socket.
                if event & select.EPOLLIN:
                    # We receive 4096 bytes of data at a time
                    # and put it in a temporary "buffer".
                    while True:
                        msg = self.receiver_socket.recv(4096)
                        decoded_msg = msg.decode('UTF-8')
                        if decoded_msg == '':
                            break
                        buff += msg
                    self.inbound_queue.append(buff)
                    buff = ""
                else:
                    print("WARNING: unexpected event on receiver socket.")
            else:
                # Sleep for 3.0 seconds if we didn't get any event this time.
                time.sleep(3.0)
