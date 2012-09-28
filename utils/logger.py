# Standard Imports
import datetime


class Logger:
    '''
    A class owned by each module of the system to use for logging its messages.
    '''
    def __init__(self, logname):
        self.logname = logname
        # Log format:
        self.date_format = '%Y-%m-%d %H:%M:%S'

    def log(self, log):
        '''
        :param log: The log to print to STDOUT.
        '''
        print(datetime.datetime.now().strftime(self.date_format),
              '%s: %s' % (self.logname, log))
