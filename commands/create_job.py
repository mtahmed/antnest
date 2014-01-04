# Standard imports
import argparse
import os
import socket
import sys
import time

# Set environment variable.
sys.path.append(os.getcwd())

# Custom imports
from job import Job, Splitter, Combiner
import messenger
import message

def enqueue_job(iszmq, jobpath, destip, destport):
    # Bind to some other port. Not to the main 33310.
    if iszmq:
        messenger_type = messenger.ZMQMessenger.TYPE_CLIENT
        m = messenger.ZMQMessenger(port=0, type=messenger_type)
    else:
        m = messenger.UDPMessenger(port=0)
    m.start()
    my_hostname = socket.gethostname()
    m.register_destination(my_hostname,
                           (destip, destport))
    # This file contains at most 3 methods: split, combine, processor and
    # at most 1 variables: input_data
    jobdir, jobfile = os.path.split(jobpath)
    job_module_name = jobfile[:-3]
    pkg = __import__(jobdir, globals(), locals(), [job_module_name], 0)
    jobcode = getattr(pkg, job_module_name)
    try:
        combiner = Combiner()
        combiner.set_combine_method(jobcode.combine)
    except:
        combiner = None

    try:
        splitter = Splitter()
        splitter.set_split_method(jobcode.split)
    except:
        splitter = None

    job = Job(processor=jobcode.processor,
              input_data=jobcode.input_data,
              splitter=splitter,
              combiner=combiner)
    try:
        job.input_data = jobcode.input_data
    except:
        pass

    if iszmq:
        m.connect((destip, destport))
        m.send_job(job, (destip, destport))
    else:
        tracker = m.send_job(job, (destip, dest_port), track=True)
        while tracker.state != message.MessageTracker.MSG_ACKED:
            time.sleep(2.0)

    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='As a master node, enqueue a '
                                                 'new job to be processed by '
                                                 'this system.')
    parser.add_argument('--jobpath', '-j',
                        type=str,
                        help='the path to the file describing the job')
    parser.add_argument('--zmq', '-z',
                        action='store_true',
                        help='send a job to a zmq socket')
    parser.add_argument('--destip',
                        help='send to this destination ip')
    parser.add_argument('--destport',
                        type=int,
                        help='send to this destination port')

    args = parser.parse_args()
    if args.zmq:
        destport = args.destport or messenger.ZMQMessenger.DEFAULT_PORT
        destip = args.destip or '0.0.0.0'
    else:
        destport = args.destport or messenger.UDPMessenger.DEFAULT_PORT
        destip = args.destip or messenger.UDPMessenger.DEFAULT_IP
    enqueue_job(args.zmq, args.jobpath, destip, destport)
