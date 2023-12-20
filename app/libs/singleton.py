
class MetaSingleton(type):
    """
    Creating the instance of any class which has MetaSingleton as metaclass 
    will trigger the check, whether such an instance was already created.
    If so, instead of new object only reference to existing object will be
    returned.
    """

    # this is the list of objects which are deemed to be singletons
    _singleton_instances = {}

    # __call__ of MetaSingleton will be called before the construction of the derived class
    def __call__(cls, *args, **kwargs):
        if cls not in cls._singleton_instances:
            cls._singleton_instances[cls] = super(MetaSingleton, cls).__call__(*args, **kwargs)
        return cls._singleton_instances[cls]