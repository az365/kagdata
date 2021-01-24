import math
import numpy as np
from scipy import interpolate
import pandas as pd
from matplotlib import pyplot as plt


def is_defined(value):
    return value is not None and value is not np.nan and not math.isnan(value)


def is_nonzero(value):
    return (value or 0) > 0 or (value or 0) < 0


def diff(c, v, take_abs=False):
    result = v - c
    if take_abs:
        return abs(result)
    else:
        return result


def is_local_extremum(x_left, x_center, x_right, local_max=True, local_min=True):
    result = False
    if local_max:
        result = x_center > x_left and x_center >= x_right
    if local_min:
        result = result or (x_center < x_left and x_center <= x_right)
    return result


def spline_interpolate(x, y):
    assert len(x) == len(y)
    kind = 'cubic' if len(x) > 3 else 'linear'
    return interpolate.interp1d(x, y, kind=kind)


def get_dataframe(*args, **kwargs):
    return pd.DataFrame(*args, **kwargs)


def plot(*args, **kwargs):
    plt.plot(*args, **kwargs)


def plot_dates(*args, **kwargs):
    plt.plot_date(*args, **kwargs)
