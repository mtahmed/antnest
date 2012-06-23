# Standard imports
import time
import select

# Custom imports
import serialize


class Messenger(object):
    '''
    An instance of this class represents an object that "serves" a node to
    communicate with other nodes (e.g. a slave's messenger will allow it to
    send completed task units back to the master).

    The messenger can be given an instance of a TaskUnit to send and it will
    take care of the serialization.
    '''
    def __init__(self, sender_socket, destination, receiver_socket, source):
        self.sender_socket = sender_socket
        self.receiver_socket = receiver_socket
        self.sender_socket.conenct(destination)
        self.receiver_socket.conenct(source)
        self.serializer = serialize.Serializer()

        # NOTE: inbound_q is a list because we don't care who the message is
        # received from, we just want the next message when receive is called.
        # NOTE: outbound_q, however, is a dictionary where the keys are the
        # destination of the message and values are the messages.
        self.inbound_q = []
        self.outbound_q = {}

    def receive(self):
        '''
        This method checks this messenger's inbound_q and if its not empty, it
        returns the next element from the queue.
        '''
        try:
            if len(inbound_q) > 0:
                msg = inbound_q[0]
                obj = self.serializer.deserialize(msg)
                self.inbound_q = self.inbound_q[1:]
                return obj
        except KeyError:
            print("MESSENGER ERROR: The messenger doesn't know the host:", src)

    def send(self, obj, dest):
        '''
        This method appends the new message to the list for the dest in the
        outbound_q. If the corresponding list doesn't exist, it creates a new
        list.
        '''
        try:
            _ = outbound_q[dest]
        except KeyError:
            outbound_q[dest] = []
        msg = self.serializer.serialize(obj)
        self.outbound_q[dest].append(msg)

    def sender(self):
        '''
        This method watches the outbound task_q to see if we have any outbound
        messages and if we do, it picks up the message, serializes it, and
        sends it to its destination.
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
            for dest in outbound_q.keys():
                if len(self.outbound_q[dest]) > 0:
                    outbound_msg = self.outbound_q[dest][0]
                    # While the msg is still not sent...
                    while outbound_msg is not None:
                        # Poll with timeout of 1.0 seconds.
                        poll_responses = poller.poll(1.0)
                        for _, event in poll_responses:
                            # If we can send...
                            if event & select.EPOLLOUT:
                                bytes_sent = sender_socket.sendall(outbound_msg)
                                self.outbound_q[dest] = outbound_q[dest][1:]
                                outbound_msg = None
                                break
                            else:
                                print("WARNING: Unexpected event on sender socket")
                        else:
                            # Sleep for 3.0 seconds if we didn't get any event.
                            time.sleep(0.50)
            else:
                # Our outbound_q has no destinations yet.
                time.sleep(3)  # Sleep for 3 seconds

    def receiver(self):
        '''
        This method polls the receiver_socket to see if it have received any
        messages.
        If yes, it puts the raw message on our inbound_q and goes back to
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
                        if decoded_msg = '':
                            break
                        buff += msg
                    self.inbound_q.append(buff)
                    buff = ""
                else:
                    print("WARNING: unexpected event on receiver socket.")
            else:
                # Sleep for 3.0 seconds if we didn't get any event this time.
                time.sleep(3.0)
