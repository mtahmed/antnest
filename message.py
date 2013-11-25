# Standard imports
import hashlib
import struct


class MessageTracker(object):
    '''Represents a tracker used to track the status of a message.
    '''
    MSG_QUEUED = 0
    MSG_SENT   = 1
    MSG_ACKED  = 2

    VALID_STATES = [MSG_QUEUED,
                    MSG_SENT,
                    MSG_ACKED]

    def __init__(self, msg_id, isinuse=False):
        self.msg_id = msg_id
        self.state = MessageTracker.MSG_QUEUED
        # Says whether the this tracker is in use.
        self.isinuse = False

    def set_state(self, state):
        if not state in MessageTracker.VALID_STATES:
            raise Exception("Unknown tracker state: %d" % state)
        self.state = state

    def get_state(self):
        return self.state

    def is_sent(self):
        return self.state == MessageTracker.MSG_SENT

    def is_acked(self):
        return self.state == MessageTracker.MSG_ACKED

    def not_in_use(self):
        self.isinuse = False


class Message(object):
    '''Represents a message that can be sent over the network.
    '''
    # Constants
    MSG_HEADER_SIZE = 21
    MSG_PAYLOAD_SIZE = 4096
    MSG_SIZE = MSG_HEADER_SIZE + MSG_PAYLOAD_SIZE

    # <MSG_ID><MSG_META1><MSG_META2><MSG_META3><MSG_TYPE><MSG_FLAGS><PAYLOAD>
    MSG_FORMAT = '16sBBBBB%ds'

    # Types of messages
    MSG_STATUS = 0
    MSG_ACK = 1
    MSG_TASKUNIT = 2
    MSG_TASKUNIT_RESULT = 3  # This is basically a taskunit with only status
                             # and/or result.
    MSG_JOB = 4

    VALID_MSG_TYPES = [MSG_STATUS,
                       MSG_ACK,
                       MSG_TASKUNIT,
                       MSG_TASKUNIT_RESULT,
                       MSG_JOB]

    def __init__(self,
                 packed_msg=None,
                 msg_id=None,
                 msg_meta1=None,
                 msg_meta2=None,
                 msg_meta3=None,
                 msg_type=None,
                 msg_flags=None,
                 msg_payload=None):

        # If packed_msg is provided, then we need to unpack.
        if packed_msg:
            self.packed_msg = packed_msg
            if len(self.packed_msg) > Message.MSG_SIZE:
                raise Exception("Message size shouldn't exceed %d bytes." % (Message.MSG_SIZE))
            try:
                # Unpack the raw bytes.
                self.payload_size = len(self.packed_msg) - Message.MSG_HEADER_SIZE
                unpacked_msg = struct.unpack(Message.MSG_FORMAT % (self.payload_size),
                                            self.packed_msg)
            except:
                raise Exception('The raw data is malformed. Unable to '
                                'reconstruct the message.')
            self.msg_id      = unpacked_msg[0]
            self.msg_meta1   = unpacked_msg[1]
            self.msg_meta2   = unpacked_msg[2]
            self.msg_meta3   = unpacked_msg[3]
            self.msg_type    = unpacked_msg[4]
            self.msg_flags   = unpacked_msg[5]
            self.msg_payload = unpacked_msg[6]
        # If msg_payload is provided, then we need to pack.
        elif msg_payload:
            if len(msg_payload) > Message.MSG_PAYLOAD_SIZE:
                raise Exception("Message payload size shouldn't exceed %d "
                                "bytes." % Message.MSG_PAYLOAD_SIZE)

            if isinstance(msg_payload, bytes):
                self.msg_payload = msg_payload
            else:
                self.msg_payload = bytes(msg_payload, 'UTF-8')

            self.msg_id      = msg_id
            self.msg_meta1   = 0xFF if msg_meta1 is None else msg_meta1
            self.msg_meta2   = 0xFF if msg_meta2 is None else msg_meta2
            self.msg_meta3   = 0xFF if msg_meta3 is None else msg_meta3
            self.msg_type    = msg_type
            self.msg_flags   = msg_flags
            self.payload_size= len(msg_payload)
            self.packed_msg  = struct.pack(Message.MSG_FORMAT % self.payload_size,
                                           self.msg_id,
                                           self.msg_meta1,
                                           self.msg_meta2,
                                           self.msg_meta3,
                                           self.msg_type,
                                           self.msg_flags,
                                           self.msg_payload)
        else:
            raise Exception("Either packed_message or msg_payload must be "
                            "given")

        return


    @staticmethod
    def compute_msg_id(msg_payload, msg_type, dest_address):
        '''Compute the message id which is a 16-byte md5 hash.
        '''
        m = hashlib.md5()

        if isinstance(msg_payload, bytes):
            msg_payload_bytes = msg_payload
        else:
            msg_payload_bytes = bytes(msg_payload, 'UTF-8')

        hashable = (bytes(str(msg_type), 'UTF-8') +
                    bytes(dest_address[0], 'UTF-8') +
                    bytes(str(dest_address[1]), 'UTF-8') +
                    msg_payload_bytes)
        m.update(hashable)

        return m.digest()
