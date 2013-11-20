# Standard imports
import argparse
import os
import socket
import sys

# Set environment variable.
sys.path.append(os.getcwd())

# Custom imports
import messenger
import slave

def start_slave(port):
    # Create a new Slave instance. Note that the conditions to run a slave on this
    # Node must be met before this command is called.
    # One of the conditions is the the appropriate config file for this node
    # be present in the config directory.
    this_node = slave.Slave(port)
    # Now call the worker method of this Slave.
    this_node.worker()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Starts this Node as an instance'
                                                 'of a slave Node.')
    parser.add_argument('--port', '-p', type=int,
                        help='the port the slave should use')


    args = parser.parse_args()
    port = args.port if args.port else messenger.Messenger.DEFAULT_PORT
    start_slave(port)
