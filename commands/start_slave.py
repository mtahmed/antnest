# Standard imports
import argparse
import os
import sys

# Set environment variable.
sys.path.append(os.getcwd())

# Custom imports
import messenger
import slave

def start_slave(port):
    '''Create and start a new slave.
    '''
    this_node = slave.Slave(port)
    this_node.worker()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Starts this Node as an instance'
                                                 'of a slave Node.')
    parser.add_argument('--port', '-p', type=int,
                        help='the port the slave should use')


    args = parser.parse_args()
    port = args.port if args.port is not None else messenger.UDPMessenger.DEFAULT_PORT
    start_slave(port)
