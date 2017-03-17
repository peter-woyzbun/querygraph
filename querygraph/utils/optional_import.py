

class NotInstalled(object):
    def __init__(self, name):
        self.__name = name

    def __getattr__(self, item):
        raise ImportError('The {0} package is required to use this '
                          'optional feature'.format(self.__name))