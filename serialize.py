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
        if msg_type == message.Message.MSG_STATUS:
            return int(msg.msg_payload.decode('UTF-8'))
        elif msg_type == message.Message.MSG_TASKUNIT:
            return self.deserialize_taskunit(msg.msg_payload.decode('UTF-8'))
        elif msg_type == message.Message.MSG_JOB:
            return self.deserialize_job(msg.msg_payload.decode('UTF-8'))

    def serialize_taskunit(self, tu):
        '''
        This method serializes a task unit and returns the result as a JSON
        string.
        '''
        taskunit_id = tu.taskunit_id
        processor_code = tu.processor_code
        if tu.data is not None:
            data = tu.data
        else:
            data = None
        state = tu.state
        if tu.result is not None:
            result = tu.result
        else:
            result = None
        retries = tu.retries

        serialized_taskunit = {}
        if not taskunit_id:
            raise Exception('taskunit_id must be present for serialization')
        serialized_taskunit['taskunit_id'] = taskunit_id
        if data:
            serialized_taskunit['data'] = data
        if processor_code:
            serialized_taskunit['processor'] = processor_code
        if state:
            serialized_taskunit['state'] = state
        if result:
            serialized_taskunit['result'] = result
        if retries:
            serialized_taskunit['retries'] = retries

        return json.dumps(serialized_taskunit)

    def deserialize_taskunit(self, serialized_taskunit):
        '''
        This method deserializes a task unit and returns the result as a
        TaskUnit object.
        '''
        taskunit_dict = json.loads(serialized_taskunit)

        taskunit_id    = taskunit_dict['taskunit_id']
        try:
            processor_code = taskunit_dict['processor']
            # This defines the processor method in this scope
            exec(processor_code, globals())
        except:
            processor_code = None

        try:
            data = taskunit_dict['data']
        except:
            data = None

        try:
            state = taskunit_dict['state']
        except:
            state = None

        try:
            result = taskunit_dict['result']
        except:
            result = None

        try:
            retries = taskunit_dict['retries']
        except:
            retries = None

        tu = taskunit.TaskUnit(taskunit_id=taskunit_id,
                               data=data,
                               processor=processor,
                               retries=retries,
                               state=state)

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
        j.processor_code = processor_code
        j.splitter_code = splitter_code
        j.combiner_code = combiner_code

        j.input_data = input_data
        j.combiner.set_combine_method(combine)
        j.splitter.set_split_method(split)

        return j
