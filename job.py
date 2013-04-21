# Standard imports
import json


class Job:
    '''
    An instance of this class represents a job to be run on a distributed
    system cluster. The job defines a splitter, a combiner, the input to the
    job, the processor for the taskunits.
    '''
    def __init__(self,
                 processor,
                 input_file=None,
                 splitter=None,
                 combiner=None,):
        '''
        :param input_file: A path to a file to be used by splitter. Optional.

        :param splitter: An instance of a splitter. If None provided, an
        instance of the default splitter is used.

        :param combiner: An instance of a combiner. If None provided, an
        instance of the default combiner is used.

        :param processor: A function which every taskunit needs to be able
        to processor some given data into the required result.
        '''
        self.processor = processor

        if splitter is None:
            self.splitter = Splitter()
        else:
            self.splitter = splitter

        if combiner is None:
            self.combiner = Combiner()
        else:
            self.combiner = combiner

        self.input_file = input_file
        if self.input_file:
            self.input_data = open(self.input_file).read()
        else:
            self.input_data = None


class Splitter:
    '''
    An instance of this class represents a splitter used by a master to "split"
    a job into smaller task units to be assigned to the slaves.
    The users of the system can define their own splitters to be used by the
    master.
    '''
    def __init__(self):
        pass

    def set_split_method(self, split_method):
        '''
        Set the method to be used to split a job into taskunits.

        :param split_method: The new method to be used instead of the default
        split method below.
        '''
        self.__class__.split = split_method

    def split(self, input_file, processor):
        '''
        This method generates taskunits given an input file and the number of
        slaves to generate the taskunits for. The input_file is split at
        newlines and one taskunit is created for each line.

        This method can be overwritten if the users of the system decides to
        use their own splitter.

        :param processor: The processor for each "split". Each split is
        basically a taskunit.
        '''
        f = open(input_file, 'r')
        input_data = f.read()
        input_lines = input_data.split('\n')
        for input_line in input_lines:
            t = taskunit.TaskUnit(input_line, processor)
            yield t


class Combiner:
    '''
    An instance of this class represents a combiner used by a master to
    combine the results from taskunits.

    The users of the system can define their own combiners to be used by the
    master.
    '''
    def __init__(self):
        pass

    def set_combine_method(self, combine_method):
        '''
        Set the method to be used to combine the results from taskunits.
        '''
        self.__class__.combine = combine_method

    def combine(self, taskunits):
        '''
        This method takes as input a list of taskunits and combines their
        results into the final result.

        This method just uses the "sum" operator to combine all the results
        and then dumps the results as a JSON string to the file
        result_<date>.json

        In most situations, the system users would want to define their own
        combine method to combine the results.

        :param taskunits: A list of taskunit objects.
        '''
        results = [t.result for t in taskunits]
        combined_result = sum(results)  # Just sum the values.
        json_string = json.dumps(combined_result, indent=2)
        result_file = open('result_' + time.strftime('%Y-%m-%d_%H:%M:%S'), 'w')
        result_file.write(json_string)
        result_file.close()
