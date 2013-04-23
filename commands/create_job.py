# Standard imports
import argparse
import socket
import time
import sys
import os

# Set environment variable.
sys.path.append(os.getcwd())

# Custom imports
import messenger
from job import Job, Splitter, Combiner

def enqueue_job(jobpath):
    # Bind to some other port. Not to the main 33310.
    m = messenger.Messenger(port=0)
    my_hostname = socket.gethostname()
    m.register_destination(my_hostname,
                           ('0.0.0.0', 33310))
    # This file contains at most 3 methods: split, combine, processor and
    # at most 2 variables: input_data, input_file
    jobdir, jobfile = os.path.split(jobpath)
    job_module_name = jobfile[:-3]
    print(jobdir, jobfile, job_module_name)
    pkg = __import__(jobdir,
                     globals(),
                     locals(),
                     [job_module_name],
                     0)
    jobcode = getattr(pkg, job_module_name)
    try:
        combiner = Combiner()
        combiner.combine = jobcode.combine
    except:
        combiner = None

    try:
        splitter = Splitter()
        splitter.split = jobcode.split
    except:
        splitter = None

    job = Job(processor=jobcode.processor,
              input_file=jobcode.input_file,
              splitter=splitter,
              combiner=combiner)
    try:
        job.input_data = jobcode.input_data
    except:
        pass

    m.send_job(job, ('0.0.0.0', 33310))
    while len(m.outbound_queue):
        print("Job still not sent out...sleeping.")
        time.sleep(2)
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='As a master node, enqueue a '
                                                 'new job to be processed by '
                                                 'this system.')
    parser.add_argument('--jobpath', '-j',
                        type=str,
                        help='the path to the file describing the job')

    args = parser.parse_args()
    enqueue_job(args.jobpath)
