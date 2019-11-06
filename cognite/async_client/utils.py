import collections
import copy
import functools
import inspect
import math
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


def extends_class(cls=None, extends=None):
    if cls is None:
        return functools.partial(extends_class, extends=extends)
    for name, fn in inspect.getmembers(cls, predicate=inspect.isfunction):
        setattr(extends, name, fn)
    return cls


def timedelta_to_granularity(td):
    """ finds the most suitable granularity supported by CDF that matches a given timedelta
    td: timedelta or number of milliseconds"""
    _granularities_in_s = [["s", 60], ["m", 60], ["h", 24], ["d", 1e9]]
    n = td.total_seconds() if isinstance(td, timedelta) else td / 1000.0
    i = 0
    while n > _granularities_in_s[i][1]:
        n = n / _granularities_in_s[i][1]
        i += 1
    return "%d%s" % (math.ceil(n - 1e-6), _granularities_in_s[i][0])  # round up, but avoid rounding 1.00000001 to 2


def to_list(x):
    """ ensures x is a list (or None) """
    if x is None:
        return x  # don't return [None] or such
    elif not isinstance(x, collections.abc.Iterable) or isinstance(x, (str, bytes)):
        return [x]  # convert single numbers/strings to a one element list
    else:
        return list(x)  # convert numpy arrays and pd series to list
