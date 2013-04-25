# Standard imports
import time
import select
import socket
import threading

# Custom imports
import serialize
import message
import job
import taskunit
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
        # Both inbound_queue and outbound_queue contain tuples of
        # (address, message) that are received or need to be sent out.
        self.inbound_queue = []
        self.outbound_queue = []
        # This dict is used to keep track of MessageTracker objects which can
        # be used to track message status.
        self.trackers = {}

        self.logger = utils.logger.Logger('MESSENGER')

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

    def get_address_from_hostname(self, hostname):
        '''
        Return the ip address for the hostname `hostname`.
        '''
        return self.hostname_to_address[hostname]

    def register_destination(self, hostname, address):
        '''
        Store the hostname as key with address as value for this destination
        so that the caller can later only supply destination as hostname
        to communicate with the destination.
        '''
        self.logger.log("Register destination %s:%s" % (hostname, address))
        self.hostname_to_address[hostname] = address
        self.address_to_hostname[address] = hostname

    def deserialize_message_payload(self, msg):
        '''
        Takes as input a Message object and deserializes the payload of the
        message and returns it.
        The return value depends on what kind of object the payload represents.
        '''
        return self.serializer.deserialize(msg)

    def receive(self, return_payload=True):
        '''
        This method checks this messenger's inbound_queue and if its not empty,
        it returns the next element from the queue.

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

    def send_status(self, status, address, track=False):
        '''
        Send a status update to a remote node.

        If track is True, then this method returns a MessageTracker object
        which can be used to check the state of the message sending.
        '''
        serialized_status = self.serializer.serialize(status)
        msg_id, messages = self.packed_messages_from_data(message.Message.MSG_STATUS,
                                                          serialized_status,
                                                          address)
        self.queue_for_sending(messages, address)
        if track:
            tracker = message.MessageTracker()
            self.trackers[msg_id] = tracker
            return tracker

    def send_ack(self, msg, address, track=False):
        '''
        Send an ack for msg to a remote node.

        If track is True, then this method returns a MessageTracker object
        which can be used to check the state of the message sending.
        '''
        msg_id = msg.msg_id
        msg_id, messages = self.packed_messages_from_data(message.Message.MSG_ACK,
                                                          msg_id,
                                                          address)
        self.queue_for_sending(messages, address)
        if track:
            tracker = message.MessageTracker()
            self.trackers[msg_id] = tracker
            return tracker

    def send_job(self, job, address, track=False):
        '''
        Send a job to a remote node.

        If track is True, then this method returns a MessageTracker object
        which can be used to check the state of the message sending.
        '''
        serialized_job = self.serializer.serialize(job)
        msg_id, messages = self.packed_messages_from_data(message.Message.MSG_JOB,
                                                          serialized_job,
                                                          address)
        self.queue_for_sending(messages, address)
        if track:
            tracker = message.MessageTracker()
            self.trackers[msg_id] = tracker
            return tracker

    def send_taskunit(self, tu, address, track=False, attrs=['taskunit_id',
          'job_id', 'data', 'processor._code', 'retries', 'state', 'result']):
        '''
        Send a taskunit to a remote node.

        If track is True, then this method returns a MessageTracker object
        which can be used to check the state of the message sending.
        '''
        serialized_taskunit = self.serializer.serialize(tu, attrs)
        msg_id, messages = self.packed_messages_from_data(message.Message.MSG_TASKUNIT,
                                                          serialized_taskunit,
                                                          address)
        self.queue_for_sending(messages, address)
        if track:
            tracker = message.MessageTracker()
            self.trackers[msg_id] = tracker
            return tracker

    def send_taskunit_result(self, tu, address, track=False,
          attrs=['taskunit_id', 'job_id', 'state', 'result']):
        '''
        Send the result of running taskunit.
        '''
        serialized_result = self.serializer.serialize(tu, attrs)
        MSG_TASKUNIT_RESULT= message.Message.MSG_TASKUNIT_RESULT
        msg_id, messages = self.packed_messages_from_data(MSG_TASKUNIT_RESULT,
                                                          serialized_result,
                                                          address)
        self.queue_for_sending(messages, address)
        if track:
            tracker = message.MessageTracker()
            self.trackers[msg_id] = tracker
            return tracker

    def queue_for_sending(self, messages, address):
        '''
        This method appends the new messages to the list for the dest in the
        outbound_queue.
        NOTE: This method takes a list of messages and not a single message.
        '''
        self.outbound_queue.extend([(address, message)
                                    for message
                                    in messages])

    ##### Message-specific methods.
    def delete_tracker(self, msg_id):
        '''
        The tracker for msg_id is no longer needed. Delete it.
        '''
        del self.trackers[msg_id]

    def packed_messages_from_data(self, msg_type, msg_payload, address):
        '''
        This function takes raw bytes string and the type of message that needs
        to be constructed and returns a list of Message objects which are fragments
        of the data. Fragmentation is done to make sure the message can be sent
        over UDP.
        '''
        if msg_type not in message.Message.VALID_MSG_TYPES:
            raise Exception('Invalid message type: %d', msg_type)

        # Split the msg_payload into fragments of size 65500 bytes.
        msg_frags = []
        while len(msg_payload) > message.Message.MSG_PAYLOAD_SIZE:
            msg_frags.append(msg_payload[:message.Message.MSG_PAYLOAD_SIZE+1])
            msg_payload = msg_payload[message.Message.MSG_PAYLOAD_SIZE+1:]
        else:
            msg_frags.append(msg_payload)

        # Compute the message id.
        msg_id = message.compute_msg_id(msg_payload, msg_type, address)

        packed_messages = []
        for msg_frag_id, msg_frag in enumerate(msg_frags):
            msg_flags = 0
            if msg_frag_id == len(msg_frags) - 1:
                msg_flags = msg_flags | 0x1
            msg_object = message.Message(packed_msg=None,
                                         msg_id=msg_id,
                                         msg_meta1=msg_frag_id,
                                         msg_meta2=None,
                                         msg_meta3=None,
                                         msg_type=msg_type,
                                         msg_flags=msg_flags,
                                         msg_payload=msg_frag)
            packed_messages.append(msg_object.packed_msg)

        return (msg_id, packed_messages)

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

        messenger.logger.log("Sender up!")
        while True:
            if len(messenger.outbound_queue) == 0:
                time.sleep(3.0)
                continue
            else:
                address, msg = messenger.outbound_queue[0]

            messenger.logger.log("Sending message to %s:%d" % address)
            # While the msg is still not sent...
            while msg is not None:
                # Poll with timeout of 1.0 seconds.
                poll_responses = poller.poll(1.0)
                for _, event in poll_responses:
                    # If we can send...
                    if event & select.EPOLLOUT:
                        bytes_sent = messenger.socket.sendto(msg, address)
                        messenger.outbound_queue = messenger.outbound_queue[1:]
                        # If we have a tracker for this msg, then we need to
                        # mark it as sent if this is the last frag for the msg
                        # being sent out.
                        try:
                            msg_object = message.Message(packed_msg=msg)
                            if messenger.is_last_frag(msg_object):
                                tracker = messenger.trackers[msg_object.msg_id]
                                tracker.set_state(message.MessageTracker.MSG_SENT)
                        except KeyError:
                            pass

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

        messenger.logger.log("Receiver up!")
        while True:
            # Poll with timeout of 1.0 seconds.
            poll_responses = poller.poll(1.0)
            for fileno, event in poll_responses:
                # We received something on our socket.
                if event & select.EPOLLIN:
                    data, address = messenger.socket.recvfrom(message.Message.MSG_SIZE)
                    messenger.logger.log("Received message from %s:%d" % address)
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
                            # If it is an ack message, then we don't need to put it on the
                            # inbound_queue.
                            msg_id = catted_msg.msg_id
                            if catted_msg.msg_type == message.Message.MSG_ACK:
                                try:
                                    MSG_ACKED = message.MessageTracker.MSG_ACKED
                                    acked_msg_id = catted_msg.msg_payload
                                    tracker = messenger.trackers[acked_msg_id]
                                    tracker.set_state(MSG_ACKED)
                                except KeyError:
                                    pass
                                continue
                            messenger.inbound_queue.append((address, catted_msg))
                            # Send an ack now that we have received the msg.
                            messenger.send_ack(catted_msg, address)
                            del fragments_map[msg.msg_id]
                else:
                    messenger.logger.log("Unexpected event on receiver socket.")
            else:
                # Sleep for 3.0 seconds if we didn't get any event this time.
                time.sleep(3.0)
