# Standard imports
import collections
import select
import socket
import threading

# Custom imports
import job
import message
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
        self.inbound_queue = collections.deque()
        self.outbound_queue = collections.deque()
        self.outbound_queue_sem = threading.Semaphore(value=0)
        self.inbound_queue_sem = threading.Semaphore(value=0)
        # Fragments map for inbound messages.
        self.fragments_map = {}
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

    def receive(self, deserialize=True):
        '''Yield the next message from the inbound_queue.

        :param deserialize: If True, the message payload is deserialized
        and generated instead of the Message object itself.
        '''
        while self.inbound_queue_sem.acquire():
            msg = self.inbound_queue.popleft()

            if not deserialize:
                yield msg
                continue

            msg_type = msg.msg_type
            decoded_msg = msg.msg_payload.decode('UTF-8')
            if msg_type == message.Message.MSG_STATUS:
                yield int(decoded_msg)
            elif msg_type == message.Message.MSG_TASKUNIT:
                yield taskunit.TaskUnit.deserialize(decoded_msg)
            elif msg_type == message.Message.MSG_TASKUNIT_RESULT:
                yield taskunit.TaskUnit.deserialize(decoded_msg)
            elif msg_type == message.Message.MSG_JOB:
                yield job.Job.deserialize(decoded_msg)

    def send_status(self, status, address, track=False):
        '''
        Send a status update to a remote node.

        If track is True, then this method returns a MessageTracker object
        which can be used to check the state of the message sending.
        '''
        # Trivially serializeable.
        serialized_status = str(status)
        msg_id, messages = message.Message.packed_fragments(
            message.Message.MSG_STATUS,
            serialized_status,
            address)
        tracker = message.MessageTracker(msg_id, isinuse=track)
        self.trackers[msg_id] = tracker
        self.queue_for_sending(messages, address)
        if track:
            return tracker

    def send_ack(self, msg, address, track=False):
        '''
        Send an ack for msg to a remote node.

        If track is True, then this method returns a MessageTracker object
        which can be used to check the state of the message sending.
        '''
        msg_id = msg.msg_id
        msg_id, messages = message.Message.packed_fragments(
            message.Message.MSG_ACK,
            msg_id,
            address)
        tracker = message.MessageTracker(msg_id, isinuse=track)
        self.trackers[msg_id] = tracker
        self.queue_for_sending(messages, address)
        if track:
            return tracker

    def send_job(self, job, address, track=False):
        '''
        Send a job to a remote node.

        If track is True, then this method returns a MessageTracker object
        which can be used to check the state of the message sending.
        '''
        serialized_job = job.serialize(json_encode=True)
        msg_id, messages = message.Message.packed_fragments(
            message.Message.MSG_JOB,
            serialized_job,
            address)
        tracker = message.MessageTracker(msg_id, isinuse=track)
        self.trackers[msg_id] = tracker
        self.queue_for_sending(messages, address)
        if track:
            return tracker

    def send_taskunit(self, tu, address, track=False,
                      attrs=['id', 'job_id', 'data', 'retries', 'state',
                             'result']):
        '''
        Send a taskunit to a remote node.

        If track is True, then this method returns a MessageTracker object
        which can be used to check the state of the message sending.
        '''
        serialized_taskunit = tu.serialize(include_attrs=attrs,
                                           json_encode=True)
        msg_id, messages = message.Message.packed_fragments(
            message.Message.MSG_TASKUNIT,
            serialized_taskunit,
            address)
        tracker = message.MessageTracker(msg_id, isinuse=track)
        self.trackers[msg_id] = tracker
        self.queue_for_sending(messages, address)
        if track:
            return tracker

    def send_taskunit_result(self, tu, address, track=False,
                             attrs=['id', 'job_id', 'state', 'result']):
        '''
        Send the result of running taskunit.
        '''
        serialized_result = tu.serialize(include_attrs=attrs, json_encode=True)
        msg_id, messages = message.Message.packed_fragments(
            message.Message.MSG_TASKUNIT_RESULT,
            serialized_result,
            address)
        tracker = message.MessageTracker(msg_id, isinuse=track)
        self.trackers[msg_id] = tracker
        self.queue_for_sending(messages, address)
        if track:
            return tracker

    def queue_for_sending(self, messages, address):
        '''
        This method appends the new messages to the list for the dest in the
        outbound_queue.
        NOTE: This method takes a list of messages and not a single message.
        '''
        for message in messages:
            self.outbound_queue.append((address, message))
            self.outbound_queue_sem.release()

    def delete_tracker(self, tracker):
        '''
        The tracker for msg_id is no longer needed. Delete it.
        '''
        msg_id = tracker.msg_id
        del self.trackers[msg_id]

    @staticmethod
    def sender(messenger):
        '''Send messages out through the sender socket. Forever.
        '''
        poller = select.epoll()
        poller.register(messenger.socket.fileno(),
                        select.EPOLLOUT | select.EPOLLET)  # Edge-triggered.

        messenger.logger.log("Sender up!")
        while True:
            messenger.outbound_queue_sem.acquire()
            address, msg = messenger.outbound_queue.popleft()

            messenger.logger.log("Sending message to %s:%d" % address)
            # While the msg is still not sent...
            while msg is not None:
                # Poll with timeout of 1.0 seconds.
                poll_responses = poller.poll(1.0)
                for _, event in poll_responses:
                    # If we can send...
                    if event & select.EPOLLOUT:
                        bytes_sent = messenger.socket.sendto(msg, address)
                        if bytes_sent == 0:
                            raise Exception("Couldn't send out the message.")
                        # If we have a tracker for this msg, then we need to
                        # mark it as sent if this is the last frag for the msg
                        # being sent out.
                        try:
                            msg_object = message.Message(packed_msg=msg)
                            if msg_object.is_last_frag():
                                tracker = messenger.trackers[msg_object.msg_id]
                                tracker.set_state(
                                    message.MessageTracker.MSG_SENT)
                        except KeyError:
                            pass

                        msg = None
                        break
                    else:
                        messenger.logger.log(
                            "Unexpected event on sender socket.")

    def handle_received_msg(self, msg, address):
        '''Handle received message.
        '''
        fragments_map = self.fragments_map

        msg = message.Message(packed_msg=msg)
        try:
            fragments_map[msg.msg_id]
        except KeyError:
            fragments_map[msg.msg_id] = []

        if not msg.is_last_frag():
            fragments_map[msg.msg_id].append(msg)
        else:
            msg_frag_id = msg.msg_meta1
            total_frags = msg_frag_id + 1
            current_frags = len(fragments_map[msg.msg_id])
            fragments_map[msg.msg_id].extend(
                [None] * (total_frags - current_frags))
            fragments_map[msg.msg_id][-1] = msg
        # If all the frags for this message have already been received.
        if None not in fragments_map[msg.msg_id]:
            if fragments_map[msg.msg_id][-1].is_last_frag():
                msg = message.Message.glue_fragments(fragments_map[msg.msg_id])
                # If it is an ack message, then we don't need to put it on the
                # inbound_queue.
                msg_id = msg.msg_id
                # If this message is an ack, then update the tracker.
                if msg.msg_type == message.Message.MSG_ACK:
                    MSG_ACKED = message.MessageTracker.MSG_ACKED
                    acked_msg_id = msg.msg_payload
                    tracker = self.trackers[acked_msg_id]
                    tracker.set_state(MSG_ACKED)
                    # If the tracker is not being used, delete it.
                    if not tracker.isinuse:
                        self.delete_tracker(tracker)
                        return
                self.inbound_queue.append((address, msg))
                self.inbound_queue_sem.release()
                # Send an ack now that we have received the msg.
                self.send_ack(msg, address)
                del fragments_map[msg_id]

        return

    @staticmethod
    def receiver(messenger):
        '''Receive messages on the receiver socket. Forever.
        '''
        poller = select.epoll()
        poller.register(messenger.socket.fileno(),
                        select.EPOLLIN | select.EPOLLET)  # Edge-triggered.

        messenger.logger.log("Receiver up!")
        while True:
            poll_responses = poller.poll()
            for fileno, event in poll_responses:
                if not event & select.EPOLLIN:
                    messenger.logger.log(
                        "Unexpected event on receiver socket.")
                    continue
                data, address = messenger.socket.recvfrom(
                    message.Message.MSG_SIZE)
                messenger.logger.log("Received message from %s:%d" % address)

                messenger.handle_received_msg(data, address)
