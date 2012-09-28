# Standard imports
import time
import select
import socket
import threading

# Custom imports
import serialize
import message
import job
import utils.logger


class Messenger(object):
    '''
    An instance of this class represents an object that "serves" a node to
    communicate with other nodes (e.g. a slave's messenger will allow it to
    send completed task units back to the master).

    The messenger can be given an instance of a TaskUnit to send and it will
    take care of the serialization.
    '''
    def __init__(self, port=33310):
        ''' TODO
        '''
        self.serializer = serialize.Serializer()
        self.port = port

        # Create the sockets.
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('0.0.0.0', self.port))

        # Dictionaries and lists.
        self.hostname_to_address = {}
        # The message ids for each of the remote destinations. The ids are
        # incremented as we go.
        self.msg_ids = {}
        # NOTE: inbound_queue is a list because we don't care who the message is
        # received from, we just want the next message when receive is called.
        # NOTE: outbound_queue, however, is a dictionary where the keys are the
        # destination of the message and values are the messages.
        self.inbound_queue = []
        self.outbound_queue = {}

        # Create and start the receiver and sender threads now.
        receiver_thread = threading.Thread(target=self.receiver,
                                           name='receiver_thread',
                                           args=(self,))
        sender_thread = threading.Thread(target=self.sender,
                                         name='sender_thread',
                                         args=(self,))
        receiver_thread.start()
        sender_thread.start()

        self.logger = utils.logger.Logger('MESSENGER')

    def get_address_from_hostname(self, hostname):
        '''
        Return the ip address for the hostname `hostname`.
        '''
        return self.hostname_to_address[hostname]

    def get_next_msg_id(self, hostname):
        '''
        Return the next message id for the hostname `hostname` and increment
        the message id by 1.
        '''
        msg_id = self.msg_ids[hostname]
        self.msg_ids[hostname] += 1
        return msg_id

    def register_destination(self, hostname, address):
        '''
        Store the hostname as key with address as value for this destination
        so that the caller can later only supply destination as hostname
        to communicate with the destination.
        '''
        self.hostname_to_address[hostname] = address
        self.outbound_queue[hostname] = []
        self.msg_ids[hostname] = 0

    def deserialize_message_payload(self, msg):
        '''
        Takes as input a Message object and deserializes the payload of the
        message and returns it.
        The return value depends on what kind of object the payload represents.
        '''
        return self.serializer.deserialize(msg)

    def receive(self, return_payload=True):
        '''
        This method checks this messenger's inbound_queue and if its not empty, it
        returns the next element from the queue.

        :param return_payload: If True, the message payload is deserialized
        and returned instead of the message itself.
        '''
        if len(self.inbound_queue) > 0:
            msg = self.inbound_queue[0]
            self.inbound_queue = self.inbound_queue[1:]
            if return_payload:
                return self.deserialize_message_payload(msg)
            else:
                return msg
        else:
            return None

    def send_job(self, job, dest_hostname):
        '''
        This method can be used to send a taskunit to a remote node.
        '''
        msg_id = self.get_next_msg_id(dest_hostname)
        serialized_job = self.serializer.serialize(job)
        print(serialized_job)
        messages = message.packed_messages_from_data(msg_id,
                                                     message.MSG_JOB,
                                                     serialized_job)
        self.queue_for_sending(messages, dest_hostname)

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
        try:
            self.outbound_queue[dest_hostname].extend(messages)
        except KeyError:
            raise Exception('MESSENGER: Doesn\'t know about the host \'' +
                            dest_hostname +
                            '\'. Perhaps you should register this destination.')

    @staticmethod
    def sender(messenger):
        '''
        This method watches the outbound task_q to see if we have any outbound
        messages and if we do, it picks up the message and sends it to its
        destination.
        In doing so, it also has to poll the sock to make sure that we can send
        data.

        NOTE: This method goes over the list of destinations in a round-robin
        way and for each of them, it picks up the first outbound message for
        that destination and sends it out.
        '''
        poller = select.epoll()
        poller.register(messenger.sock.fileno(),
                        select.EPOLLOUT | select.EPOLLET)  # Edge-triggered.

        while True:
            for dest in messenger.outbound_queue.keys():
                if len(messenger.outbound_queue[dest]) == 0:
                    messenger.logger.log('No messages for host: %s' % dest)
                    continue
                messenger.logger.log("MESSENGER: Found a message to send out.")
                outbound_msg = messenger.outbound_queue[dest][0]
                # While the msg is still not sent...
                while outbound_msg is not None:
                    # Poll with timeout of 1.0 seconds.
                    poll_responses = poller.poll(1.0)
                    for _, event in poll_responses:
                        # If we can send...
                        if event & select.EPOLLOUT:
                            address = messenger.get_address_from_hostname(dest)
                            bytes_sent = messenger.sock.sendto(outbound_msg,
                                                               address)
                            messenger.outbound_queue[dest] = messenger.outbound_queue[dest][1:]
                            outbound_msg = None
                            break
                        else:
                            messenger.logger.log("Messenger: Unexpected event on sender socket")
                    else:
                        # Sleep for 3.0 seconds if we didn't get any event.
                        time.sleep(0.50)
            else:
                # Our outbound_queue has no destinations yet.
                time.sleep(3)  # Sleep for 3 seconds

    @staticmethod
    def receiver(messenger):
        '''
        This method polls the receiver_socket to see if it have received any
        messages.
        If yes, it puts the raw message on our inbound_queue and goes back to
        polling the socket.
        Sleeps for 3 seconds otherwise.
        '''
        poller = select.epoll()
        poller.register(messenger.sock.fileno(),
                        select.EPOLLIN | select.EPOLLET)  # Edge-triggered.

        fragments_queue = {}

        while True:
            # Poll with timeout of 1.0 seconds.
            poll_responses = poller.poll(1.0)
            for fileno, event in poll_responses:
                # We received something on our socket.
                if event & select.EPOLLIN:
                    messenger.logger.log("MESSENGER: Received a message!")
                    data = messenger.sock.recv(65535)
                    decoded_data = data.decode('UTF-8')
                    # Make a message object out of the data and append it
                    # to the fragments queue...
                    msg = message.Message(data)
                    try:
                        fragments_queue[msg.msg_id]
                    except KeyError:
                        fragments_queue[msg.msg_id] = []
                    if not msg.is_last_frag:
                        fragments_queue[msg.msg_id].append(msg)
                    elif msg.is_last_frag:
                        total_frags = msg.msg_frag_id + 1
                        current_frags = len(fragments_queue[msg.msg_id])
                        fragments_queue[msg.msg_id].extend([None] * (total_frags - current_frags))
                        fragments_queue[msg.msg_id][-1] = msg
                    # If all the frags for this message have already been received...
                    if None not in fragments_queue[msg.msg_id]:
                        if fragments_queue[msg.msg_id][-1].is_last_frag:
                            catted_msg = message.cat_message_objects(fragments_queue[msg.msg_id])
                            messenger.inbound_queue.append(catted_msg)
                            fragments_queue[msg.msg_id] = None
                else:
                    messenger.logger.log("MESSENGER: unexpected event on receiver socket.")
            else:
                # Sleep for 3.0 seconds if we didn't get any event this time.
                time.sleep(3.0)
