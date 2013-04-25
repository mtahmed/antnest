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
    def serialize(self, obj, attrs=None):
        if isinstance(obj, taskunit.TaskUnit):
            return self.serialize_taskunit(obj, attrs)
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
        elif msg_type == message.Message.MSG_TASKUNIT_RESULT:
            return self.deserialize_taskunit(msg.msg_payload.decode('UTF-8'))
        elif msg_type == message.Message.MSG_JOB:
            return self.deserialize_job(msg.msg_payload.decode('UTF-8'))

    def serialize_taskunit(self, tu, attrs):
        '''
        This method serializes a task unit and returns the result as a JSON
        string.
        '''
        if (not 'taskunit_id' in attrs or
            not 'job_id' in attrs):
            raise Exception('taskunit_id and job_id must be present for serialization')

        serialized_taskunit = {}
        for attr in attrs:
            sub_obj = tu
            for sub_attr in attr.split('.'):
                sub_obj = getattr(sub_obj, sub_attr)
            serialized_taskunit[attr] = sub_obj

        return json.dumps(serialized_taskunit)

    def deserialize_taskunit(self, serialized_taskunit):
        '''
        This method deserializes a task unit and returns the result as a
        TaskUnit object.
        '''
        taskunit_dict = json.loads(serialized_taskunit)

        taskunit_id = taskunit_dict['taskunit_id']
        job_id = taskunit_dict['job_id']
        try:
            processor_code = taskunit_dict['processor._code']
            # This defines the processor method in this scope
            exec(processor_code, globals())
            processor._code = processor_code
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
            retries = 0

        tu = taskunit.TaskUnit(taskunit_id=taskunit_id,
                               job_id=job_id,
                               data=data,
                               processor=processor,
                               retries=retries,
                               state=state)
        tu.result = result
        tu.processor.__func__._code = processor_code

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

        if job_object.job_id:
            job_id = job_object.job_id
        else:
            # If job_id isn't already computed, then compute it.
            job_id = job.compute_job_id(input_data,
                                        processor_code,
                                        splitter_code,
                                        combiner_code)

        serialized_job = {'job_id': job_id,
                          'processor': processor_code,
                          'splitter': splitter_code,
                          'combiner': combiner_code,
                          'input_data': input_data}

        return json.dumps(serialized_job)

    def deserialize_job(self, serialized_job):
        '''
        This method deserializes the job and returns the result as a Job object.
        '''
        job_dict = json.loads(serialized_job)

        job_id = job_dict['job_id']
        processor_code = job_dict['processor']
        splitter_code = job_dict['splitter']
        combiner_code = job_dict['combiner']
        input_data = job_dict['input_data']

        exec(processor_code, globals())
        exec(splitter_code, globals())
        exec(combiner_code, globals())

        j = job.Job(job_id=job_id,
                    processor=processor,
                    input_data=input_data)
        j.processor_code = processor_code
        j.splitter_code = splitter_code
        j.combiner_code = combiner_code
        j.combiner.set_combine_method(combine)
        j.splitter.set_split_method(split)


        return j
