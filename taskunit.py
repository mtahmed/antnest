# Standard imports
import hashlib
import inspect
import serialize
import types


class TaskUnit(serialize.Serializable):
    '''
    An instance of this class represents a task unit that is processed by a
    slave node. A task unit is small enough to be processed on its own (that is,
    it does not depend on any other data for its processing). A task unit has
    the following parts to it:
    # id: See the compute_id static method to see how it's computed.
    # data: Some data the task is to be run on. E.g. data could be a number
      which the slave machine has to find all the factors for.
      Data must be something that is "serializable". For now, we will define the
      following objects/types that are serializable:
        - str (including unicode, binary, raw etc.)
        - int
        - float
        - list of either of the above
        - tuple of either of the above
        - dictionary with key/values of either of the above type
        - byte array
    # processor: A method that takes Data (see previous) and processes it in
      some way to produce the result. E.g. the processor could be a method which
      factorizes a number and produces a list of factors.
    # run: This method calls the processor(see #2) with with the data(see #1).
      It then returns the result which is then put into the task unit's
      result(see #4) attribute.
    # result: Initially the result is empty (when the task unit is created by
      the master. When the slave's processor(see previous) is done its work,
      it returns the results. The run method(see #3) takes these results and
      puts them in the task unit's result attribute.

    # state: The state of a task unit. The state can be one of the following:
        DEFINED: The task is created and not found its way to the slaves task
            queue yet.
        PENDING: The task is received by the Slave and unpacked and put on the
            task queue.
        RUNNING: The task is picked up from a Slave's task queue and is being
            processed.
        FAILED: There was an attempt to run the task but it failed. It may or
            may not be picked up by the Slave again at a later time to retry.
        BAILED: The task exceeded the retry limit. It will not be picked up by
            the same Slave again.
        REFUSED: The Slave refused to do it (who does he think he is?). There
            could be several reasons for this. One of them could be that the
            Slave has already done this task before. If a task is REFUSED, the
            master should still check if the result is None. In most situations
            the result will be returned by the slave even if the task was
            REFUSED.
        COMPLETED: The slave processed the task and set the result attribute
            successfully.
    '''
    STATES = ('DEFINED',
              'PENDING',
              'RUNNING',
              'FAILED',
              'BAILED',
              'REFUSED',
              'COMPLETED')

    def __init__(self, id=None, job_id=None, data=None, processor=None,
                 retries=0, state='DEFINED'):
        '''
        :param id: The TaskUnit id. (see ``compute_id`` method)
        :param job_id: The id of the Job this TaskUnit is part of.
        :param data: The data to run the processor on.
        :param processor: A function that takes data and processes it to produce
        the results required.
        :param retries: Number of retries after failures allowed.
        :param state: The state of the TaskUnit. (see ``STATES``)
        '''
        super().__init__()
        self.noserialize += ['STATES', 'set_processor', 'setstate', 'run',
                             'retries', 'compute_id']
        self.id = id
        self.job_id = job_id
        self.data = data
        if processor:
            self.set_processor(processor)
        if retries >= 0:
            self.retries = retries
        else:
            raise Exception("Retries must be >= 0.")
        self.setstate(state)

        self.result = None

    def set_processor(self, processor):
        '''Set the processor method for this TaskUnit.
        '''
        while inspect.ismethod(processor):
            processor = processor.__func__
        self.processor = types.MethodType(processor, self)

    def setstate(self, state):
        '''Set the state of this TaskUnit.
        '''
        if state not in TaskUnit.STATES:
            raise ValueError('Unknown state: %s' % state)
        else:
            self.state = state

    def run(self):
        '''Run the the TaskUnit.

        This method is called by the Slave node to "execute" the task unit to
        get the desired results into the task unit.
        '''
        self.setstate('RUNNING')
        try:
            result = self.processor(self.data)
            self.result = result
            self.setstate('COMPLETED')
        except Exception as e:
            if self.retries == 0:
                self.state = 'BAILED'
            else:
                self.state = 'FAILED'
                self.retries -= 1

    def processor(self):
        '''The function that is applied to the data to produce results.
        '''
        pass

    @staticmethod
    def compute_id(data, processor_code):
        '''Compute the taskunit_id.

        The taskunit_id is the MD5 hash of the concatenation of the taskunit's
        data and the processor_code.
        '''
        m = hashlib.md5()
        if isinstance(data, bytes):
            data_bytes = data
        else:
            data_bytes = bytes(data, 'UTF-8')

        if isinstance(processor_code, bytes):
            processor_code_bytes = processor_code
        else:
            processor_code_bytes = bytes(processor_code, 'UTF-8')

        hashable = (data_bytes +
                    processor_code_bytes)
        m.update(hashable)

        return m.hexdigest()
