# Standard imports
import struct


# Constants
MAX_MSG_SIZE = 65507
MSG_FORMAT = 'IBBB%ds' # <MSG_ID><MSG_TYPE><MSG_FLAGS><MSG_FRAG_ID><PAYLOAD>
MSG_HEADER_SIZE = 7

# Types of messages
MSG_STATUS_NOTIFY = 0
MSG_TASKUNIT = 1

VALID_MSG_TYPES = [MSG_STATUS_NOTIFY, MSG_TASKUNIT]

def packed_messages_from_data(msg_id, msg_type, data):
    '''
    This function takes raw bytes string and the type of message that needs
    to be constructed and returns a list of Message objects which are fragments
    of the data. Fragmentation is done to make sure the message can be sent
    over UDP/IP.
    '''
    # Split the data into fragments of size 65500 bytes.
    data_frags = []
    while len(data) > 65500:
        data_frags.append(data[:65501])
        data = data[65501:]

    if msg_type not in VALID_MSG_TYPES:
        raise Exception('Invalid message type: %d', msg_type)
    packed_messages = []
    for msg_frag_id, data_frag in enumerate(data_frags):
        msg_flags = 0
        if index == len(data_frags) - 1:
            msg_flags = msg_flags | 0x1
        packed = struct.pack(MSG_FORMAT % len(data_frag),
                             msg_id,
                             msg_type,
                             msg_flags,
                             msg_frag_id,
                             data_frag)
        packed_messages.append(packed)

    return messages

def data_from_packed_messages(packed_messages):
    '''
    This function takes a list of packed messages and extracts the payload
    from them and reconstructs the data from the fragments.
    '''
    unpacked_messages = [Message(packed_message)
                        for packed_message
                        in packed_messages]
    unpacked_messages.sort(key=message.frag_id)

    # If the last frag doesn't claim to be the last fragment...
    if not unpacked_messages[-1].is_last_frag:
        raise Exception('Malformed fragments. Unable to construct data.')
    if not unpacked_messages[-1].msg_frag_id == (len(unpacked_messages) - 1):
        raise Exception('Missing a fragment. Unable to construct data.')

    data = b''
    for unpacked_message in unpacked_messages:
        data += unpacked_message.msg_payload

    return data
        
class Message(object):
    '''
    An instance of this class represents a that can be sent over the network.
    This really is a packet decoder. It just facilitates in decoding
    the packets to read.
    '''
    def __init__(packed):
        self.packed = packed
        if len(packed) > MAX_MSG_SIZE:
            raise Exception("Message size shouldn't exceed " +
                            str(MAX_MSG_SIZE))
        try:
            # Unpack the raw bytes.
            unpacked_tuple = struct.unpack(MSG_FORMAT % (len(packed) -
                                                         MSG_HEADER_SIZE),
                                           packed)
        except:
            raise Exception('ERROR: The raw data is malformed. Unable to '
                            'reconstruct the message.')
        self.msg_id = unpacked_tuple[0]
        self.msg_type = unpacked_tuple[1]
        self.msg_flags = unpacked_tuple[2]
        self.msg_frag_id = unpacked_tuple[3]
        self.msg_payload = unpacked_tuple[4]
        # Now unpack the flags.
        mask = 0x1
        self.is_last_frag = self.flags & mask
