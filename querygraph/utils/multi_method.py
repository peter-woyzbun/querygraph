

class multimethod(object):
    """Decorator for multiple dispatch of functions and methods.

    """

    def __init__(self, *types, **kwargs):
        self.types = types
        self.condition = kwargs.pop('condition', None)
        if kwargs:
            raise TypeError("multimethod() got an unexpected keyword argument {0!r}".format(kwargs.keys()[0]))

    class _Dispatcher(object):

        def __init__(self):
            self.typemap = []
            self.ismethod = False

        def __get__(self, obj, type=None):
            if obj is None:
                return self
            from functools import partial
            self.ismethod = True
            return partial(self, obj)

        def __call__(self, *args):
            matchable = args[1:] if self.ismethod else args
            for types, condition, function, argspec in self.typemap:
                if len(matchable) == len(types) and all(isinstance(m, t) for m, t in zip(matchable, types)):
                    if condition is None:
                        return function(*args)
                    else:
                        l = dict(zip(argspec.args, args))
                        if eval(condition, globals(), l):
                            return function(*args)
            raise ValueError("multimethod: no matching method found")

    def __call__(self, function):
        import inspect
        frame = inspect.currentframe()
        try:
            dispatcher = frame.f_back.f_locals.get(function.__name__, self._Dispatcher())
        finally:
            del frame
        dispatcher.typemap.append((self.types, self.condition, function, inspect.getargspec(function)))
        return dispatcher
