# Standard imports
import inspect
import json

# Custom imports
import taskunit
import job
import message


class Serializer(object):
    '''
    An instance of this class represents a an object that can serialize a given
    object. This method assumes prior knowledge of the object structure to be
    serialized.
    '''
    def serialize(self, obj):
        if isinstance(obj, taskunit.TaskUnit):
            return self.serialize_taskunit(obj)
        elif isinstance(obj, job.Job):
            return self.serialize_job(obj)
        elif (isinstance(obj, str) or
              isinstance(obj, int) or
              isinstance(obj, float) or
              isinstance(obj, list) or
              isinstance(obj, tuple) or
              isinstance(obj, dict)):
            return json.dumps(obj)

    def deserialize(self, msg):
        msg_type = msg.msg_type
        if isinstance(msg, taskunit.TaskUnit):
            return self.deserialize_taskunit(msg)
        elif isinstance(msg, job.Job):
            return self.deserialize_taskunit(msg)
        elif (isinstance(msg, str) or
              isinstance(msg, int) or
              isinstance(msg, float) or
              isinstance(msg, list) or
              isinstance(msg, tuple) or
              isinstance(msg, dict)):
            return json.loads(msg)

    def serialize_taskunit(self, taskunit):
        '''
        This method serializes a task unit and returns the result as a JSON
        string.
        '''
        processor = taskunit.processor
        if processor is not None:
            processor_code = inspect.getsource(processor)
        else:
            processor_code = None
        if taskunit.data is not None:
            data = taskunit.data
        else:
            data = None
        state = taskunit.state
        if taskunit.result is not None:
            result = taskunit.result
        else:
            result = None
        retries = taskunit.retries

        serialized_taskunit = {'processor': processor_code,
                               'data': data,
                               'state': state,
                               'result': result,
                               'retries': retries}

        return json.dumps(serialized_taskunit)

    def deserialize_taskunit(self, serialized_taskunit):
        '''
        This method deserializes a task unit and returns the result as a
        TaskUnit object.
        '''
        taskunit_dict = json.loads(serialized_taskunit)

        processor_code = taskunit_dict['processor']
        exec(processor_code)  # This defines the processor method in this scope
        data = taskunit_dict['data']
        state = taskunit_dict['state']
        result = taskunit_dict['result']
        retries = taskunit_dict['retries']

        tu = taskunit.TaskUnit(data=data,
                               processor=processor,
                               retries=retries)
        tu.setstate(state)

        return tu

    def serialize_job(self, job):
        '''
        This method serializes a job and returns the result as a JSON string.
        '''
        processor = job.processor
        splitter = job.splitter.split
        combiner = job.combiner.combine
        processor_code = inspect.getsource(processor)
        splitter_code = inspect.getsource(splitter)
        combiner_code = inspect.getsource(combiner)
        input_data = job.input_data

        serialized_job = {'processor': processor_code,
                          'splitter': splitter_code,
                          'combiner': combiner_code,
                          'input_data': input_data}

        return json.dumps(serialized_job)
