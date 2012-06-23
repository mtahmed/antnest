import inspect
import json

class Serializer(object):
    '''
    An instance of this class represents a an object that can serialize a given
    object. This method assumes prior knowledge of the object structure to be
    serialized.
    '''
    def serialize(self, obj):
        if isinstance(obj, TaskUnit):
            return self.serialize_taskunit(obj)
        elif (isinstance(obj, str) or
              isinstance(obj, int) or
              isinstance(obj, float) or
              isinstance(obj, list) or
              isinstance(obj, tuple) or
              isinstance(obj, dict)):
            return json.dumps(obj)

    def deserialize(self, obj):
        if isinstance(obj, TaskUnit):
            return self.deserialize_taskunit(obj)
        elif (isinstance(obj, str) or
              isinstance(obj, int) or
              isinstance(obj, float) or
              isinstance(obj, list) or
              isinstance(obj, tuple) or
              isinstance(obj, dict)):
            return json.loads(obj)

    def serialize_taskunit(self, taskunit):
        '''
        This method serializes a task unit and returns the result as a JSON
        string.
        '''
        processor = taskunit.processor
        processor_code = inspect.getsource(processor)
        data = taskunit.data
        state = taskunit.state
        result = taskunit.result
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

        taskunit = TaskUnit(data=data, processor=processor, retries=retries)
        taskunit.setstate(state)

        return taskunit
