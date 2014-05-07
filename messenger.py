# Standard imports
import collections
import json
import select
import socket
import threading
import zmq

# Custom imports
import job
import message
import taskunit
import utils.logger


class Messenger:
    '''A class representing a messenger that handles all communication.
    '''
    def __init__(self):
        # identity <--> address maps.
        self.identity_to_address = {}
        self.address_to_identity = {}

        # Both inbound_queue and outbound_queue contain tuples of
        # (address, message) that are received or need to be sent out.
        self.inbound_queue = collections.deque()
        self.outbound_queue = collections.deque()
        self.outbound_queue_sem = threading.Semaphore(value=0)
        self.inbound_queue_sem = threading.Semaphore(value=0)

        # This dict is used to keep track of MessageTracker objects which can
        # be used to track message status.
        self.trackers = {}

        self.logger = utils.logger.Logger('MESSENGER')

        return

    def start(self):
        '''Start the messenger.
        '''
        pass

    def get_host_by_name(self, name):
        '''Return the address for the hostname.
        '''
        return self.identity_to_address[name]

    def register_destination(self, name, address):
        '''
        Store the hostname as key with address as value for this destination
        so that the caller can later only supply destination as hostname
        to communicate with the destination.
        '''
        self.identity_to_address[name] = address
        self.address_to_identity[address] = name

        return

    def send(self, msg, address):
        '''Send the msg to the address.
        '''
        self.outbound_queue.append((address, msg))
        self.outbound_queue_sem.release()

        return

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

    def queue_for_sending(self, messages, address):
        '''Add messages to the outbound queue for sending.

        NOTE: This method takes a list of messages and not a single message.
        '''
        for message in messages:
            self.outbound_queue.append((address, message))
            self.outbound_queue_sem.release()

        return

    def delete_tracker(self, tracker):
        '''
        The tracker for msg_id is no longer needed. Delete it.
        '''
        msg_id = tracker.msg_id
        del self.trackers[msg_id]

        return

    def sender(self):
        '''Send messages out through the sender socket. Forever.
        '''
        pass

    def receiver(self):
        '''Receive messages on the receiver socket. Forever.
        '''
        pass


class UDPMessenger(Messenger):
    '''A Messenger that uses UDP sockets for communication.

    This messenger implements custom fragmentation, ack etc.
    '''
    # Constants
    DEFAULT_IP   = '0.0.0.0'
    DEFAULT_PORT = 33310

    def __init__(self, ip=DEFAULT_IP, port=DEFAULT_PORT):
        super().__init__()

        self.ip = ip
        self.port = port

        # Fragments map for inbound messages.
        self.fragments_map = {}

        return

    def start(self):
        '''Start the messenger.
        '''
        # Create the sockets.
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('0.0.0.0', self.port))

        # Create and start the receiver and sender threads now.
        receiver_thread = threading.Thread(target=self.receiver,
                                           name='receiver_thread')
        sender_thread = threading.Thread(target=self.sender,
                                         name='sender_thread')
        receiver_thread.start()
        sender_thread.start()

        return

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

    def sender(self):
        '''Send messages out through the sender socket. Forever.
        '''
        poller = select.epoll()
        poller.register(self.socket.fileno(),
                        select.EPOLLOUT | select.EPOLLET)  # Edge-triggered.

        self.logger.log("Sender up!")
        while True:
            self.outbound_queue_sem.acquire()
            address, msg = self.outbound_queue.popleft()

            self.logger.log("Sending message to %s:%d" % address)
            # While the msg is still not sent...
            while msg is not None:
                # Poll with timeout of 1.0 seconds.
                poll_responses = poller.poll(1.0)
                for _, event in poll_responses:
                    # If we can send...
                    if event & select.EPOLLOUT:
                        bytes_sent = self.socket.sendto(msg, address)
                        if bytes_sent == 0:
                            raise Exception("Couldn't send out the message.")
                        # If we have a tracker for this msg, then we need to
                        # mark it as sent if this is the last frag for the msg
                        # being sent out.
                        try:
                            msg_object = message.Message(packed_msg=msg)
                            if msg_object.is_last_frag():
                                tracker = self.trackers[msg_object.msg_id]
                                tracker.set_state(
                                    message.MessageTracker.MSG_SENT)
                        except KeyError:
                            pass

                        msg = None
                        break
                    else:
                        self.logger.log("Unexpected event on sender socket.")

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

    def receiver(self):
        '''Receive messages on the receiver socket. Forever.
        '''
        poller = select.epoll()
        poller.register(self.socket.fileno(),
                        select.EPOLLIN | select.EPOLLET)  # Edge-triggered.

        self.logger.log("Receiver up!")
        while True:
            poll_responses = poller.poll()
            for fileno, event in poll_responses:
                if not event & select.EPOLLIN:
                    self.logger.log(
                        "Unexpected event on receiver socket.")
                    continue
                data, address = self.socket.recvfrom(message.Message.MSG_SIZE)
                self.logger.log("Received message from %s:%d" % address)

                self.handle_received_msg(data, address)


class ZMQMessenger(Messenger):
    # Constants
    DEFAULT_PORT = 33310
    NUM_TRIES = 3

    # Messenger types
    TYPE_SERVER = 0  # Listener socket. Accepts connections.
    TYPE_CLIENT = 1  # Client socket. Connects to server.
    VALID_TYPES = [TYPE_SERVER, TYPE_CLIENT]

    def __init__(self, type, ip=None, port=DEFAULT_PORT):
        '''
        :param type: The type of Messenger. Can be SERVER or CLIENT messenger.
        :param ip: The ip of the interface the socket should use.
        :param port: The port the socket should use.
        '''
        super().__init__()

        self.type = type
        self.ip = ip
        self.port = port

        self.context = zmq.Context()

        return

    def start(self):
        if self.ip:
            public_ip = ip
        else:
            public_ip = self.get_public_ip()

        identity = 'tcp://%s:%d' % (public_ip, self.port)
        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.setsockopt(zmq.IDENTITY, bytes(identity, 'UTF-8'))

        if self.type == self.TYPE_SERVER:
            self.socket.bind(identity)

        return

    def connect(self, address):
        '''Connect to address and PING NUM_TRIES times till PONG received.

        Raises ConnectionError if failed to connect after NUM_TRIES tries. None
        otherwise.
        '''
        self.socket.connect('tcp://%s:%d' % address)
        for _ in range(self.NUM_TRIES):
            self.ping(address)
            try:
                msg_address, msg = next(self.receive(block=False, timeout=0.5))
                if msg_address == address and msg == 'PONG':
                    return
            except:
                pass
        else:
            raise ConnectionError("Failed to connect.")

    def ping(self, address):
        self.send(json.dumps('PING'), address)

        return

    def pong(self, address):
        self.send(json.dumps('PONG'), address)

        return

    def receive(self, deserialize=False, block=True, timeout=0):
        while True:
            flags = 0 if block else zmq.NOBLOCK
            if timeout > 0.0:
                if self.socket.poll(timeout=timeout*1000) == 0:
                    raise TimeoutError()
            address = self.socket.recv_string(flags=flags)
            assert self.socket.recv() == b""  # Empty delimiter
            msg = self.socket.recv_json()

            # FIXME(mtahmed): This would probably fail for IPV6.
            address = address.split(':')[1:]
            address[0] = address[0][2:]
            address[1] = int(address[1])
            address = tuple(address)

            # FIXME(mtahmed): The PING-PONG should be taken care of in Messenger.

            if not deserialize:
                yield (address, msg)
                continue

            # FIXME
            msg_type = msg.msg_type
            decoded_msg = msg.msg_payload.decode('UTF-8')
            if msg_type == message.Message.MSG_STATUS:
                yield (address, int(decoded_msg))
            elif msg_type == message.Message.MSG_TASKUNIT:
                yield (address, taskunit.TaskUnit.deserialize(decoded_msg))
            elif msg_type == message.Message.MSG_TASKUNIT_RESULT:
                yield (address, taskunit.TaskUnit.deserialize(decoded_msg))
            elif msg_type == message.Message.MSG_JOB:
                yield (address, job.Job.deserialize(decoded_msg))

    def send(self, msg, address):
        address = 'tcp://%s:%d' % address
        self.socket.send_string(address, zmq.SNDMORE)
        self.socket.send_string("", zmq.SNDMORE)
        self.socket.send_string(msg)

        return

    def send_job(self, job, address):
        '''Send a job to a remote node.
        '''
        serialized_job = job.serialize(json_encode=True)
        self.send(serialized_job, address)

        return

    def send_taskunit(self, tu, address,
                      attrs=['id', 'job_id', 'data', 'retries', 'state',
                             'result']):
        '''Send a taskunit to a remote node.
        '''
        serialized_taskunit = tu.serialize(include_attrs=attrs,
                                           json_encode=True)
        self.send(serialized_taskunit, address)

        return

    def send_taskunit_result(self, tu, address,
                             attrs=['id', 'job_id', 'state', 'result']):
        '''Send the result of running taskunit.
        '''
        serialized_result = tu.serialize(include_attrs=attrs, json_encode=True)
        self.send(serialized_result, address)

        return

    @staticmethod
    def get_public_ip():
        '''Get the ip address of the external interface.

        This tries to connect to some public service to try to see what
        interface the socket binds to and uses that interface's address.
        '''
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        google_addr = socket.gethostbyname('www.google.com')
        s.connect((google_addr, 80))
        addr = s.getsockname()[0]
        s.close()

        return addr
