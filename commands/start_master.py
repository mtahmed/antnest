# Standard imports
import argparse
import socket
import sys
import os

# Set environment variable.
sys.path.append(os.getcwd())

# Custom imports
import master

def start_master():
    # Create a new Master instance. Note that the conditions to run a master on this
    # Node must be met before this command is called.
    this_node = master.Master()
    # Now call the worker method of this Slave.
    this_node.worker()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Starts this Node as an instance'
                                                 'of a slave Node.')

    args = parser.parse_args()
    start_master()
