from __future__ import absolute_import

from . import errors

class MulticastDelegate(object):
    def __init__(self):
        self.delegates = []

    def __iadd__(self, delegate):
        self.add(delegate)
        return self

    def add(self, delegate):
        self.delegates.append(delegate)

    def __isub__(self, delegate):
        self.delegates.remove(delegate)
        return self

    def __call__(self, *args, **kwargs):
        for d in self.delegates:
            d(*args, **kwargs)


def coerce_positional(query, paramset):
    params = tuple("$%d" % (i + 1) for i in xrange(len(paramset)))

    try:
        return query % params, lambda params: params
    except TypeError:
        raise errors.ProgrammingError(
                "Incorrect number of positional parameters "
                "passed to execute()/executemany()")

def coerce_named(query, paramset):
    param_names = []
    index = [0]
    class D(dict):
        def __getitem__(self, item):
            param_names.append(item)
            index[0] += 1
            return "$%d" % index[0]
    def convert_params(params):
        try:
            return [params[name] for name in param_names]
        except KeyError, k:
            raise errors.ProgrammingError(
                        "Parameter %s not present" % k.message)
    try:
        return query % D(), convert_params
    except KeyError, k:
        raise errors.ProgrammingError(
                        "Parameter %s not present" % k.message)

def class_memoized(fn):
    return fn()