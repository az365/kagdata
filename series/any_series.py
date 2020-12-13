import math
import numpy as np


def is_defined(value):
    return value is not None and value is not np.nan and not math.isnan(value)


def is_nonzero(value):
    return (value or 0) > 0 or (value or 0) < 0


class AnySeries:
    def __init__(
            self,
            values=list(),
    ):
        self.values = values

    def get_items(self):
        return self.values

    def get_values(self):
        return self.values

    def get_list(self):
        return list(self.get_items())

    def get_iter(self):
        yield from self.get_items()

    def get_count(self):
        return len(self.get_values())

    def get_item_no(self, n):
        return self.get_list()[n]

    def get_items_from_to(self, n_start, n_end):
        return self.get_list()[n_start: n_end]

    def get_slice(self, n_start, n_end):
        return AnySeries(self.get_items_from_to(n_start, n_end))

    def append(self, value, return_series=False):
        self.values.append(value)
        if return_series:
            return self

    def add(self, series, return_series=True):
        for i in series.get_items():
            self.append(i)
        if return_series:
            return self

    def filter(self, function):
        # return __class__(
        return AnySeries(
            filter(function, self.get_items()),
        )

    def filter_values(self, function):
        return __class__(
            values=[v for v in self.get_values() if function(v)]
        )

    def filter_values_defined(self):
        return self.filter_values(is_defined)

    def filter_values_nonzero(self):
        return self.filter_values(is_nonzero())

    def mean_value(self):
        return np.mean(
            self.filter_values_defined().get_values(),
        )

    def smooth(self, window=3):
        result = AnySeries()
        center = int((window - 1) / 2)
        count = self.get_count()
        for n in range(count):
            if n < center or n >= count - center:
                result.append(self.get_item_no(n))
            else:
                sub_series = self.get_slice(n - center, n + center + 1)
                result.append(sub_series.mean_value())
        return result
