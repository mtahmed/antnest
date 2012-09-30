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
        if msg_type == message.MSG_STATUS_NOTIFY:
            pass
        elif msg_type == message.MSG_TASKUNIT:
            return self.deserialize_taskunit(msg.msg_payload.decode('UTF-8'))
        elif msg_type == message.MSG_JOB:
            return self.deserialize_job(msg.msg_payload.decode('UTF-8'))

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

    def serialize_job(self, job_object):
        '''
        This method serializes a job and returns the result as a JSON string.
        '''
        processor = job_object.processor
        splitter = job_object.splitter.split
        combiner = job_object.combiner.combine
        processor_code = inspect.getsource(processor)
        splitter_code = inspect.getsource(splitter)
        combiner_code = inspect.getsource(combiner)
        input_data = job_object.input_data

        serialized_job = {'processor': processor_code,
                          'splitter': splitter_code,
                          'combiner': combiner_code,
                          'input_data': input_data}

        return json.dumps(serialized_job)

    def deserialize_job(self, serialized_job):
        '''
        This method deserializes the job and returns the result as a Job object.
        '''
        job_dict = json.loads(serialized_job)

        processor_code = job_dict['processor']
        splitter_code = job_dict['splitter']
        combiner_code = job_dict['combiner']
        input_data = job_dict['input_data']

        exec(processor_code, globals())
        exec(splitter_code, globals())
        exec(combiner_code, globals())

        j = job.Job(processor)
        j.input_data = input_data
        j.combiner.set_combine_method(combine)
        j.splitter.set_split_method(split)

        return j
