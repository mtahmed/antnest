# Standard imports
import hashlib
import json

# Custom imports
import serialize
import taskunit


class Job(serialize.Serializable):
    '''Represents a job to be handled by a master node.

    An instance of this class represents a job to be run on a distributed
    system cluster. The job defines a splitter, a combiner, the input to the
    job, the processor for the taskunits.
    '''
    def __init__(self, id=None, input_data=None, processor=None, splitter=None,
                 combiner=None):
        '''
        :param input_data: An elementary type.
        :param splitter: An instance of Splitter. Default used if None.
        :param combiner: An instance of Combiner. Default used if None.
        :param processor: A function which processes input to a TaskUnit.
        '''
        super().__init__(recursive_serialize=True)
        self.noserialize += ['taskunits', 'compute_id']
        self.id = id

        self.__class__.processor = processor

        self.input_data = input_data

        self.splitter = splitter if splitter else Splitter()
        self.combiner = combiner if combiner else Combiner()

        # Map of taskunit ids to TaskUnits.
        self.taskunits = {}

    @staticmethod
    def compute_id(input_data, processor_code, split_code, combine_code):
        '''Compute the job_id.

        The job_id is the MD5 hash of the concatenation of the job's data,
        the processor_code, split method's code, combine_method's code.

        :param processor_code: source code for processor method
        :param split_code: source code for the split method of the splitter
        :param combine_code: source code for the combine method of the combiner
        :returns: MD5 hex digest
        '''
        m = hashlib.md5()
        if isinstance(input_data, bytes):
            input_data_bytes = input_data
        else:
            input_data_bytes = bytes(input_data, 'UTF-8')

        if isinstance(processor_code, bytes):
            processor_code_bytes = processor_code
        else:
            processor_code_bytes = bytes(processor_code, 'UTF-8')

        if isinstance(split_code, bytes):
            split_code_bytes = split_code
        else:
            split_code_bytes = bytes(split_code, 'UTF-8')

        if isinstance(combine_code, bytes):
            combine_code_bytes = combine_code
        else:
            combine_code_bytes = bytes(combine_code, 'UTF-8')

        hashable = (input_data_bytes + processor_code_bytes + split_code_bytes +
                    combine_code_bytes)
        m.update(hashable)

        return m.hexdigest()


class Splitter(serialize.Serializable):
    '''Represents a splitter used by master to split jobs.

    The users of the system can define their own splitters to be used by the
    master.
    '''
    def __init__(self):
        super().__init__()
        self.noserialize += ['set_split_method']

    def set_split_method(self, split_method):
        '''Set the method to be used to split a job into taskunits.

        :param split_method: The new method to be used instead of the default
        split method below.
        '''
        self.__class__.split = split_method

    def split(self, input_data, processor):
        '''Generate splits (taskunits) given an input file and a processor.

        The input_data is split at newlines and one taskunit is created for each
        line.

        This method can be overwritten if the user of the system decides to use
        their own splitter.

        :param input_data: The data to work with.
        :param processor: The processor for each generated taskunit.
        :generates: TaskUnit
        '''
        input_lines = input_data.split('\n')
        for input_line in input_lines:
            t = taskunit.TaskUnit(data=input_line, processor=processor)
            yield t


class Combiner(serialize.Serializable):
    '''Represents a combiner used by a master to combine TaskUnit results.

    The users of the system can define their own combiners to be used by the
    master.
    '''
    def __init__(self):
        super().__init__()
        self.noserialize += ['set_combine_method', 'add_taskunits', 'taskunits']
        self.taskunits = []

    def set_combine_method(self, combine_method):
        '''Set the method to be used to combine the results from taskunits.
        '''
        self.__class__.combine = combine_method

    def add_taskunits(self, tu):
        '''Add a taskunit to combine.

        When all the taskunits are available (determined by the master),
        the combine() method needs to called to actually combine the results.
        '''
        self.taskunits.extend(tu)

    def combine(self):
        '''Combine all the added TaskUnit results.

        This method just uses the "sum" operator to combine all the results
        and then dumps the results as a JSON string to the file
        result_<date>.json

        In most situations, the system users would want to define their own
        combine method to combine the results.
        '''
        taskunits = self.taskunits
        results = [t.result for t in taskunits]
        combined_result = sum(results)  # Just sum the values.
        json_string = json.dumps(combined_result, indent=2)
        result_file = open('result_' + time.strftime('%Y-%m-%d_%H:%M:%S'), 'w')
        result_file.write(json_string)
        result_file.close()
