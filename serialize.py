# Standard imports
import hashlib
import inspect
import json
import os
import types


class Serializable:
    '''
    This is a base class which provides the basic serialization methods to be
    used from within the class derived from this class.
    '''
    def __init__(self,
                 noserialize=['__init__', 'noserialize', 'serialize_method',
                              'serialize', 'deserialize', 'get_vars',
                              'get_methods', 'get_serializables', '__class__',
                              'recursive_serialize'],
                 recursive_serialize=False):
        # List of methods that are not to be serialized.
        self.noserialize = noserialize
        # Whether to recursively serialize
        self.recursive_serialize = recursive_serialize
        return

    def get_vars(self):
        '''Get all the variable attributes of this class.

        :returns: A dictionary of variable name to value.
        :rtype: dict
        '''
        return {var: getattr(self, var) for var in dir(self)
                if not inspect.ismethod(getattr(self, var)) and
                   not inspect.isfunction(getattr(self, var)) and
                   not var.startswith('__') and
                   not var in self.noserialize and
                   not isinstance(getattr(self, var), Serializable)}

    def get_serializables(self):
        '''Get all the attributes of this class that inherit from Serialazble.

        :returns: A dictionary of variable name to value.
        :rtype: dict
        '''
        return {var: getattr(self, var).serialize() for var in dir(self)
                if isinstance(getattr(self, var), Serializable) and
                var != '__class__'}

    def get_methods(self):
        '''Get all the method attributes of this class.

        NOTE: This only returns true methods, bound to the class. To add a bound
        method to an object, look at ``types.MethodType``.

        :returns: A dictionary of method name to method (function type).
        :rtype: dict
        '''
        return {name: method for name, method in
                inspect.getmembers(self, predicate=callable)
                if name not in self.noserialize and
                not inspect.isbuiltin(method) and
                str(type(method)) != '<class \'method-wrapper\'>'}

    def serialize_method(self, method):
        ''' Serialize a method.

        This function also normalizes the method source code:

        1. Removes all leading whitespace from all lines such that the ``def``
           line of the method starts at column 1.
        2. Strips any leading or trailing whitespace.

        :param method: The method to be serialized.
        :type method: A function object.
        :returns: A string for the source code of the ``method``.
        :rtype: str
        '''
        source = inspect.getsource(method)
        # Now remove all white space at the start of each line such that
        # the indentation of the code is maintained and there's no whitespace
        # before the "def ..." on the first line.
        whitespace = source.find('def')
        source = '\n'.join([line[whitespace:] for line in source.split('\n')])
        source = source.strip()

        return source

    def serialize(self, include_attrs=[], exclude_attrs=[], json_encode=False):
        '''Serialize this object.

        If both include_attrs and exclude_attrs is empty, all the attributes of
        the object are serialized.
        If include_attrs is non-empty, only the attributes in include_attrs
        are serialized.
        If exclude_attrs is non-empty, all attributes except the ones in
        exclude_attrs are serialized.
        If include_attrs and exclude_attrs overlap, the behaviour is undefined.

        :param include_attrs: A list of names of attributes to serialize.
        :type include_attrs: list
        :param exclude_attrs: A list of names of attributes to NOT serialize.
        :type exclude_attrs: list
        :param json_encode: Encode the result as json string if True.
        :type json_encode: True or False
        :returns: A serialized representation of this object.
        :rtype: str
        '''
        all_vars = self.get_vars()
        all_methods = self.get_methods()
        if not include_attrs and not exclude_attrs:
            serialize_vars = all_vars
            serialize_methods = all_methods
        elif include_attrs:
            serialize_vars = {var: value for
                              var, value in all_vars
                              if var in include_attrs}
            serialize_methods = {name: method for
                                 name, method in all_methods
                                 if name in include_attrs}
        elif exclude_attrs:
            serialize_vars = {var: value for
                              var, value in all_vars
                              if var not in exclude_attrs}
            serialize_methods = {name: method for
                                 name, method in all_methods
                                 if name not in exclude_attrs}

        # Attribute dictionary.
        attr_dict = {}
        for var, value in serialize_vars.items():
            attr_dict[var] = json.dumps(value)
        for name, method in serialize_methods.items():
            attr_dict[name] = self.serialize_method(method)
        if self.recursive_serialize:
            for var, value in self.get_serializables().items():
                attr_dict[var] = value

        serialized = {'class': self.__module__ + '.' + self.__class__.__name__,
                      'attrs': attr_dict}
        if json_encode:
            return json.dumps(serialized)

        return serialized

    @classmethod
    def deserialize(cls, serialized):
        '''Deserialize the ``serialized`` string.

        Note that this method relies on getting the mandatory arguments to
        ``__init__`` as in the serialized string. So there are two options to
        use this method:

        1. Don't have any mandatory arguments to ``__init__``.
        2. Use the same name for attributes as for mandatory arguments (e.g.
           if a arg1 is a mandatory argument, then the object must have arg1
           as one of its attributes in the serialized string so it can be passed
           to ``__init__`` when deserializing and initializing the object.

        :param cls: The class to which to deserialize the string to.
        :type cls: class
        :param serialized: JSON or it's string version.
        :returns: An instance of ``cls`` representing the ``serialized`` string.
        :rtype: instance of ``cls``
        '''
        cls_name = cls.__name__
        if isinstance(serialized, str):
            serialized = json.loads(serialized)
        serialized_attrs = serialized['attrs']

        # If there are other serialized objects embedded in this one, then
        # deserliaze them as well.
        # For deserialization, first check that the module is available in our
        # scope. If not, try to import it.
        for key, val in serialized_attrs.items():
            if isinstance(val, dict) and 'class' in val.keys():
                subclass_module, subclass = (val['class'].rsplit('.', 1))
                if subclass not in globals():
                    pkg = __import__(subclass_module, globals(), locals(),
                                     [subclass], 0)
                    globals()[subclass] = getattr(pkg, subclass)
                val = globals()[subclass].deserialize(val)
                serialized_attrs[key] = val
        # Get the list of arguments to init.
        argspec = inspect.getargspec(cls.__init__)
        args = argspec.args
        args_defaults = argspec.defaults
        len_args = 0 if args is None else len(args)
        len_args_defaults = 0 if args_defaults is None else len(args_defaults)
        num_mandatory_args = len_args - len_args_defaults
        serialized_attrs_keys = serialized_attrs.keys()
        mandatory_args = []
        for index in range(1, num_mandatory_args):
            mandatory_args.append(serialized_attrs[args[index]])
        optional_args = {}
        for index in range(num_mandatory_args, len_args):
            optional_args[args[index]] = serialized_attrs[args[index]]
        # Execute any functions so that they are defined in local scope.
        # All functions/methods start with 'def ' string.
        # Also "cache" all the functions. This allows us to implement proper
        # caching (TODO) and also allows us to get the source for functions.
        for key, val in serialized_attrs.items():
            if isinstance(val, str) and val.startswith('def '):
                # The filename for the cached source is:
                # <cls_name>_<method_name>_<source_md5_hash>.py
                m = hashlib.md5()
                m.update(val.encode('utf-8'))
                md5_hash = m.hexdigest()
                fname = '%s_%s_%s.py' % (cls_name.lower(), key, md5_hash)
                f = open(os.path.join('cache_store', fname), mode='w')
                f.write(val)
                f.close()
                pkg = __import__('cache_store', globals(), locals(),
                                 [fname[:-3]], 0)
                globals()[key] = getattr(getattr(pkg, fname[:-3]), key)
        # Now initialize the object with the provided init args.
        deserialized = cls(*mandatory_args, **optional_args)
        # Now add all the attributes that aren't mandatory arguments to __init__
        # to the deserialized object.
        init_args = mandatory_args + list(optional_args.keys())
        for key, val in serialized_attrs.items():
            if key in init_args:
                continue
            # If it's a method, we need to make a bound method to deserialized.
            if val.startswith('def '):
                val = types.MethodType(globals()[key], deserialized)
            # Now add the attribute to the deserialized object.
            setattr(deserialized, key, val)

        return deserialized
