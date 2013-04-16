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
    # Constants
    DEFAULT_PORT = 33310

    def __init__(self, port=DEFAULT_PORT):
        # Set the port.
        self.port = port

        # Create the sockets.
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('0.0.0.0', self.port))

        # Hostname <--> address maps.
        self.hostname_to_address = {}
        self.address_to_hostname = {}
        # The message ids for each of the remote destinations. The ids are
        # incremented as we go.
        self.msg_ids = {}
        # Both inbound_queue and outbound_queue contain tuples of
        # (address, message) that are received or need to be sent out.
        self.inbound_queue = []
        self.outbound_queue = []

        # Create and start the receiver and sender threads now.
        receiver_thread = threading.Thread(target=self.receiver,
                                           name='receiver_thread',
                                           args=(self,))
        sender_thread = threading.Thread(target=self.sender,
                                         name='sender_thread',
                                         args=(self,))
        receiver_thread.start()
        sender_thread.start()

        self.serializer = serialize.Serializer()
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
        self.logger.log("Register Distination %s:%s" % (hostname, address))
        self.hostname_to_address[hostname] = address
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
        if self.inbound_queue:
            msg = self.inbound_queue[0]
            self.inbound_queue = self.inbound_queue[1:]
            if return_payload:
                deserialized = self.deserialize_message_payload(msg)
                return deserialized
            else:
                return msg
        else:
            return (None, None)

    def send_job(self, job, dest_hostname):
        '''
        This method can be used to send a taskunit to a remote node.
        '''
        msg_id = self.get_next_msg_id(dest_hostname)
        serialized_job = self.serializer.serialize(job)
        messages = self.packed_messages_from_data(msg_id,
                                                  message.Message.MSG_JOB,
                                                  serialized_job)
        self.queue_for_sending(messages, dest_hostname)

    def send_taskunit(self, taskunit, dest_hostname):
        '''
        This method can be used to send a taskunit to a remote node.
        '''
        msg_id = self.get_next_msg_id(dest_hostname)
        serialized_taskunit = self.serializer.serialize(taskunit)
        messages = self.packed_messages_from_data(msg_id,
                                                  message.Message.MSG_TASKUNIT,
                                                  serialized_taskunit)
        self.queue_for_sending(messages, dest_hostname)

    def send_status(self, status, dest_hostname):
        '''
        This method can be used to send the status to a remote node.
        '''
        msg_id = self.get_next_msg_id(dest_hostname)
        serialized_status = self.serializer.serialize(status)
        messages = self.packed_messages_from_data(msg_id,
                                                  message.Message.MSG_STATUS,
                                                  serialized_status)
        self.queue_for_sending(messages, dest_hostname)

    def queue_for_sending(self, messages, dest_hostname):
        '''
        This method appends the new messages to the list for the dest in the
        outbound_queue.
        NOTE: This method takes a list of messages and not a single message.
        '''
        try:
            address = self.hostname_to_address[dest_hostname]
            for message in messages:
                self.outbound_queue.append((address, message))
        except KeyError:
            raise Exception('Unknown host: %s. Perhaps you should register this'
                            ' destination.' % dest_hostname)

    ##### Message-specific methods.
    def packed_messages_from_data(self, msg_id, msg_type, data):
        '''
        This function takes raw bytes string and the type of message that needs
        to be constructed and returns a list of Message objects which are fragments
        of the data. Fragmentation is done to make sure the message can be sent
        over UDP/IP.
        '''
        if msg_type not in message.Message.VALID_MSG_TYPES:
            raise Exception('Invalid message type: %d', msg_type)

        # Split the data into fragments of size 65500 bytes.
        data_frags = []
        while len(data) > message.Message.MSG_SIZE:
            data_frags.append(data[:message.Message.MSG_SIZE+1])
            data = data[message.Message.MSG_SIZE+1:]
        else:
            data_frags.append(data)

        packed_messages = []
        for msg_frag_id, data_frag in enumerate(data_frags):
            msg_flags = 0
            if msg_frag_id == len(data_frags) - 1:
                msg_flags = msg_flags | 0x1
            msg_object = message.Message(packed_msg=None,
                                         msg_id=msg_id,
                                         msg_meta1=msg_frag_id,
                                         msg_meta2=None,
                                         msg_meta3=None,
                                         msg_type=msg_type,
                                         msg_flags=msg_flags,
                                         msg_payload=data_frag)
            packed_messages.append(msg_object.packed_msg)

        return packed_messages

    def data_from_packed_messages(self, packed_messages):
        '''
        This function takes a list of packed messages and extracts the payload
        from them and reconstructs the data from the fragments.
        '''
        return self.message_object_from_packed_messages(packed_messages).msg_payload

    def message_object_from_packed_messages(packed_messages):
        '''
        This function takes a list of packed messages and extracts all the fields
        from them and reconstructs a Message object.
        '''
        unpacked_messages = [Message(packed_msg=packed_message)
                            for packed_message
                            in packed_messages]

        return self.cat_message_objects(message_objects)

    def cat_message_objects(self, message_objects):
        '''
        This function takes a list of Message objects and concatenates (cats) the
        messages into one Message object.
        '''
        # msg_meta1 is msg_frag_id
        message_objects.sort(key=lambda msg: msg.msg_meta1)

        # If the last frag doesn't claim to be the last fragment...
        if not self.is_last_frag(message_objects[-1]):
            raise Exception('Malformed fragments. Unable to construct data.')
        # FIXME: Crude check to make sure that all the fragments are present.
        last_frag_id = message_objects[-1].msg_meta1
        if last_frag_id != (len(message_objects) - 1):
            raise Exception('Missing a fragment. Unable to construct data.')

        data = b''
        for message_object in message_objects:
            data += message_object.msg_payload

        # Reconstruct one message object representing these fragments.
        catted_message = message_objects[0]
        catted_message.msg_payload = data
        catted_message.msg_frag_id = None

        return catted_message

    def is_last_frag(self, msg):
        if msg.msg_flags & 0x1 == 0x1:
            return True

    @staticmethod
    def sender(messenger):
        '''
        This method watches the outbound task_q to see if we have any outbound
        messages and if we do, it picks up the message and sends it to its
        destination.
        In doing so, it also has to poll the socket to make sure that we can send
        data.

        NOTE: This method goes over the list of destinations in a round-robin
        way and for each of them, it picks up the first outbound message for
        that destination and sends it out.
        '''
        poller = select.epoll()
        poller.register(messenger.socket.fileno(),
                        select.EPOLLOUT | select.EPOLLET)  # Edge-triggered.

        while True:
            if len(messenger.outbound_queue) == 0:
                time.sleep(3.0)
                continue
            else:
                address, msg = messenger.outbound_queue[0]
                messenger.outbound_queue = messenger.outbound_queue[1:]

            messenger.logger.log("Sending a message to %s ..." % address[0])
            # While the msg is still not sent...
            while msg is not None:
                # Poll with timeout of 1.0 seconds.
                poll_responses = poller.poll(1.0)
                for _, event in poll_responses:
                    # If we can send...
                    if event & select.EPOLLOUT:
                        bytes_sent = messenger.socket.sendto(msg,
                                                             address)
                        messenger.outbound_queue = messenger.outbound_queue[1:]
                        msg = None
                        break
                    else:
                        messenger.logger.log("Unexpected event on sender socket.")
                else:
                    # Sleep for 0.5 seconds if we couldn't send out.
                    time.sleep(0.5)

    @staticmethod
    def receiver(messenger):
        '''
        This method polls the socket to see if it has received any messages.
        If yes, it puts the raw message on a fragments queue and cats the
        messages together if all the fragments are received.
        Sleeps for 3 seconds otherwise.
        '''
        poller = select.epoll()
        poller.register(messenger.socket.fileno(),
                        select.EPOLLIN | select.EPOLLET)  # Edge-triggered.

        fragments_map = {}

        while True:
            # Poll with timeout of 1.0 seconds.
            poll_responses = poller.poll(1.0)
            for fileno, event in poll_responses:
                # We received something on our socket.
                if event & select.EPOLLIN:
                    messenger.logger.log("Received a message!")
                    data, address = messenger.socket.recvfrom(message.Message.MSG_SIZE)
                    # Make a message object out of the data and append it
                    # to the fragments_map...
                    msg = message.Message(packed_msg=data)
                    try:
                        fragments_map[msg.msg_id]
                    except KeyError:
                        fragments_map[msg.msg_id] = []

                    if not messenger.is_last_frag(msg):
                        fragments_map[msg.msg_id].append(msg)
                    else:
                        msg_frag_id = msg.msg_meta1
                        total_frags = msg_frag_id + 1
                        current_frags = len(fragments_map[msg.msg_id])
                        fragments_map[msg.msg_id].extend([None] * (total_frags - current_frags))
                        fragments_map[msg.msg_id][-1] = msg
                    # If all the frags for this message have already been received...
                    if None not in fragments_map[msg.msg_id]:
                        if messenger.is_last_frag(fragments_map[msg.msg_id][-1]):
                            catted_msg = messenger.cat_message_objects(fragments_map[msg.msg_id])
                            messenger.inbound_queue.append((address, catted_msg))
                            del fragments_map[msg.msg_id]
                else:
                    messenger.logger.log("Unexpected event on receiver socket.")
            else:
                # Sleep for 3.0 seconds if we didn't get any event this time.
                messenger.logger.log("No messages received. "
                                     "Sleeping for 3.0 seconds.")
                time.sleep(3.0)
