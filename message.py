# Standard imports
import struct


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
    else:
        data_frags.append(data)

    if msg_type not in Message.VALID_MSG_TYPES:
        raise Exception('Invalid message type: %d', msg_type)
    packed_messages = []
    for msg_frag_id, data_frag in enumerate(data_frags):
        msg_flags = 0
        if msg_frag_id == len(data_frags) - 1:
            msg_flags = msg_flags | 0x1
        packed = struct.pack(Message.MSG_FORMAT % len(data_frag),
                             msg_id,
                             msg_type,
                             msg_flags,
                             msg_frag_id,
                             bytes(data_frag, 'UTF-8'))
        packed_messages.append(packed)

    return packed_messages

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

def cat_message_objects(message_objects):
    '''
    This function takes a list of Message objects and concatenates (cats) the
    messages into one Message object.
    '''
    message_objects.sort(key=lambda message: message.msg_frag_id)

    # If the last frag doesn't claim to be the last fragment...
    if not message_objects[-1].is_last_frag:
        raise Exception('Malformed fragments. Unable to construct data.')
    # FIXME: Crude check to make sure that all the fragments are present.
    if not message_objects[-1].msg_frag_id == (len(message_objects) - 1):
        raise Exception('Missing a fragment. Unable to construct data.')

    data = b''
    for message_object in message_objects:
        data += message_object.msg_payload

    # Reconstruct one message object representing these fragments.
    catted_message = message_objects[0]
    catted_message.msg_payload = data
    catted_message.msg_frag_id = None

    return catted_message

def message_object_from_packed_message(packed_message):
    '''
    This function takes a packed message and extracts all the fields
    from them and reconstruct the Message object.
    '''
    return Message(packed_message)

def message_object_from_packed_messages(packed_messages):
    '''
    This function takes a list of packed messages and extracts all the fields
    from them and reconstructs a Message object.
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

    # Reconstruct one message object representing these fragments.
    message_object = unpacked_messages[0]
    message_object.msg_payload = data
    message_object.msg_frag_id = None

    return message_object
        
class Message(object):
    '''
    An instance of this class represents a that can be sent over the network.
    This really is a packet decoder. It just facilitates in decoding
    the packets to read.
    '''
    # Constants
    MSG_HEADER_SIZE = 7
    MSG_DATA_SIZE = 65500
    MSG_MAX_SIZE = MSG_HEADER_SIZE + MSG_DATA_SIZE
    MSG_FORMAT = 'IBBB%ds' # <MSG_ID><MSG_FRAG_ID><MSG_TYPE><MSG_FLAGS><PAYLOAD>

    # Types of messages
    MSG_STATUS_NOTIFY = 0
    MSG_TASKUNIT = 1
    MSG_JOB = 2

    VALID_MSG_TYPES = [ MSG_STATUS_NOTIFY
                      , MSG_TASKUNIT
                      , MSG_JOB]

    def __init__(self,
                 packed_message=None,
                 msg_id=None,
                 msg_frag_id=None,
                 msg_type=None,
                 msg_flags=None,
                 msg_payload=None):
        if packed_message:
            self.__init_from_packed_message__(self, packed_message)
        elif msg_payload:
            self.msg_id = msg_id
            self.msg_frag_id = msg_frag_id
            self.msg_type = msg_type
            self.msg_flags = msg_flags
            self.msg_payload = msg_payload

    def __init_from_packed_message__(self, packed_message):
        self.packed_message = packed_message
        if len(self.packed_message) > Message.MSG_MAX_SIZE:
            raise Exception("Message size shouldn't exceed %d bytes." % (Message.MSG_MAX_SIZE))
        try:
            # Unpack the raw bytes.
            self.payload_size = len(self.packed_message) - Message.MSG_HEADER_SIZE
            unpacked_message = struct.unpack(Message.MSG_FORMAT % (self.payload_size),
                                           packed)
        except:
            raise Exception('The raw data is malformed. Unable to '
                            'reconstruct the message.')
        self.msg_id = unpacked_message[0]
        self.msg_frag_id = unpacked_message[1]
        self.msg_type = unpacked_message[2]
        self.msg_flags = unpacked_message[3]
        self.msg_payload = unpacked_message[4]

    def is_last_frag(self):
        mask = 0x1
        return True if self.msg_flags & mask == 0x1 else False
