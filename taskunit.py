# Standard imports
import hashlib
import inspect


class TaskUnit:
    '''
    An instance of this class represents a task unit that is processed by a
    slave node. A task unit is small enough to be processed on its own (that is,
    it does not depend on any other data for its processing). A task unit has
    the following parts to it:
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

    def __init__(self,
                 data=None,
                 processor=None,
                 retries=0,
                 state='DEFINED'):
        '''
        :type data: any "serializable" object/value
        :param data: The data to run the processor on.

        :type processor: function
        :param processor: A function that takes data and processes it to produce
        the results required.
        '''
        self.processor = processor

        if retries >= 0:
            self.retries = retries
        else:
            raise Exception("Acceptable values for retries positive integers and 0.")

        self.data = data
        self.result = None
        self.setstate(state)

    def setstate(self, state):
        '''
        Set the state of this TaskUnit.
        '''
        if state not in self.STATES:
            raise Exception('Unknown statue: ' + state)
        else:
            self.state = state

    def run(self):
        '''
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
        '''
        This method will be derived from the method provided by the user for
        the processor.
        This method defines how the data will be processed.
        '''
        pass
