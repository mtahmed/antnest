# Standard imports
import struct


class Message(object):
    '''
    An instance of this class represents a message that can be sent over the network.
    '''
    # Constants
    MSG_HEADER_SIZE = 9
    MSG_DATA_SIZE = 4096
    MSG_SIZE = MSG_HEADER_SIZE + MSG_DATA_SIZE

    # <MSG_ID><MSG_META1><MSG_META2><MSG_META3><MSG_TYPE><MSG_FLAGS><PAYLOAD>
    MSG_FORMAT = 'IBBBBB%ds'

    # Types of messages
    MSG_STATUS_NOTIFY = 0
    MSG_TASKUNIT = 1
    MSG_JOB = 2

    VALID_MSG_TYPES = [ MSG_STATUS_NOTIFY
                      , MSG_TASKUNIT
                      , MSG_JOB]

    def __init__(self,
                 packed_msg=None,
                 msg_id=None,
                 msg_meta1=None,
                 msg_meta2=None,
                 msg_meta3=None,
                 msg_type=None,
                 msg_flags=None,
                 msg_payload=None):

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
        elif msg_payload:
            if len(msg_payload) > Message.MSG_DATA_SIZE:
                raise Exception("Message payload size shouldn't exceed %d "
                                "bytes." % Message.MSG_DATA_SIZE)
            self.msg_id      = msg_id
            self.msg_meta1   = 0xFF if msg_meta1 is None else msg_meta1
            self.msg_meta2   = 0xFF if msg_meta2 is None else msg_meta2
            self.msg_meta3   = 0xFF if msg_meta3 is None else msg_meta3
            self.msg_type    = msg_type
            self.msg_flags   = msg_flags
            self.msg_payload = msg_payload
            self.payload_size= len(msg_payload)
            self.packed_msg  = struct.pack(Message.MSG_FORMAT % self.payload_size,
                                           self.msg_id,
                                           self.msg_meta1,
                                           self.msg_meta2,
                                           self.msg_meta3,
                                           self.msg_type,
                                           self.msg_flags,
                                           bytes(self.msg_payload, 'UTF-8'))
        else:
            raise Exception("Either packed_message or msg_payload must be "
                            "given")
