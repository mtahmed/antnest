# Standard imports
import argparse
import os
import sys

# Set environment variable.
sys.path.append(os.getcwd())

# Custom imports
import master
import messenger

def start_master(port):
    '''Create and start a new master.
    '''
    this_node = master.Master(port)
    this_node.worker()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Starts this Node as an instance'
                                                 'of a slave Node.')
    parser.add_argument('--port', '-p', type=int,
                        help='the port the master should use')


    args = parser.parse_args()
    port = args.port if args.port else messenger.UDPMessenger.DEFAULT_PORT
    start_master(port)
