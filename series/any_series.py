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
        self.values = values.get_list() if isinstance(values, AnySeries) else list(values)

    def new(self, *args, **kwargs):
        return self.__class__(*args, **kwargs)

    def copy(self):
        return self.__class__(
            values=self.values.copy(),
        )

    def get_values(self):
        return self.values

    def set_values(self, values):
        new = self.new()
        new.values = list(values)
        return new

    def get_items(self):
        return self.values

    def set_items(self, items):
        result = self.new()
        result.values = list(items)
        return result

    def get_iter(self):
        yield from self.get_items()

    def get_list(self):
        return list(self.get_items())

    def get_count(self):
        return len(self.get_values())

    def get_range_numbers(self):
        return range(self.get_count())

    def set_count(self, count, default=None):
        if count > self.get_count():
            return self.add(
                self.new().set_values([default] * count),
            )
        else:
            return self.slice(0, count)

    def drop_item_no(self, no):
        return self.slice(0, no).add(
            self.slice(no + 1, self.get_count()),
        )

    def get_item_no(self, no, extend=False, default=None):
        if extend:
            if no < self.get_count():
                return self.get_list()[no]
            else:
                return default
        else:
            return self.get_list()[no]

    def get_items_no(self, numbers, extend=False, default=None):
        for no in numbers:
            yield self.get_item_no(no, extend=extend, default=default)

    def get_items_from_to(self, n_start, n_end):
        return self.get_list()[n_start: n_end]

    def slice(self, n_start, n_end):
        return self.new().set_items(self.get_items_from_to(n_start, n_end))

    def items_no(self, numbers, extend=False, default=None):
        return self.new().set_items(
            self.get_items_no(numbers, extend=extend, default=default)
        )

    def extend(self, series, default=None):
        count = series.get_count()
        if self.get_count() < count:
            return self.set_count(count, default)
        else:
            return self

    def intersect(self, series):
        count = series.get_count()
        if self.get_count() > count:
            return self.set_count(count)
        else:
            return self

    def shift(self, distance):
        return self.shift_value_positions(distance)

    def shift_values(self, diff):
        assert isinstance(diff, (int, float))
        return self.map_values(lambda v: v + diff)

    def shift_value_positions(self, distance, default=None):
        if distance > 0:
            return self.__class__(
                values=[default] * distance + self.get_values()
            )
        else:
            return self.slice(n_start=-distance, n_end=self.get_count())

    def append(self, value, inplace=True):
        if inplace:
            self.values.append(value)
        else:
            new = self.copy()
            new.append(value, inplace=True)
            return new

    def preface(self, value, inplace=False):
        return self.insert(
            pos=0,
            value=value,
            inplace=inplace,
        )

    def insert(self, pos, value, inplace=False):
        if inplace:
            self.values.copy().insert(pos, value)
        else:
            new = self.copy()
            new.insert(pos, value, inplace=True)
            return new

    def add(self, series, to_the_begin=False):
        if to_the_begin:
            values = series.get_list() + self.get_list()
        else:
            values = self.get_list() + series.get_list()
        return self.__class__(values=values)

    def filter(self, function):
        return self.new().set_items(
            filter(function, self.get_items()),
        )

    def filter_values(self, function):
        return self.new().set_values(
            [v for v in self.get_values() if function(v)]
        )

    def filter_values_defined(self):
        return self.filter_values(is_defined)

    def filter_values_nonzero(self):
        return self.filter_values(is_nonzero())

    def condition_values(self, function):
        return self.map_values(
            lambda v: v if function else None,
        )

    def map(self, function):
        return self.new().set_items(
            map(function, self.get_items()),
        )

    def map_values(self, function):
        return self.new().set_values(
            map(function, self.get_values())
        )

    def map_zip_values(self, function, *series):
        return self.new().set_values(
            map(
                function,
                self.get_values(),
                *[s.get_values() for s in series],
            )
        )

    def map_extend_zip_values(self, function, *series):
        count = max([s.get_count() for s in [self] + series])
        return self.set_count(count).map_zip_values(
            function,
            *[s.set_count(count) for s in series]
        )

    def map_optionally_extend_zip_values(self, function, extend, *series):
        if extend:
            return self.map_extend_zip_values(function, *series)
        else:
            return self.map_zip_values(function, *series)

    def get_sum(self):
        return sum(
            self.filter_values_defined().get_values(),
        )

    def get_mean(self):
        values_defined = self.filter_values_defined().get_values()
        return sum(values_defined) / len(values_defined)

    def norm(self, rate=None, default=None):
        if rate is None:
            rate = self.get_mean()
        return self.map_values(lambda v: v / rate if rate else default)

    def divide(self, series, default=None, extend=False):
        return self.map_optionally_extend_zip_values(
            lambda x, y: x / y if y else default,
            extend,
            series,
        )

    def subtract(self, series, default=None, extend=False):
        return self.map_optionally_extend_zip_values(
            lambda x, y: x - y if y is not None else default,
            extend,
            series,
        )

    def derivative(self, extend=False, default=None):
        if extend:
            return self.preface(default).subtract(
                self,
            )
        else:
            return self.slice(0, -1).subtract(
                self.shift(-1)
            )

    def get_sliding_window(self, window=[-1, 0, 1], extend=True, default=None, as_series=True):
        if extend:
            n_min = 0
            n_max = self.get_count()
        else:
            n_min = - min(window)
            n_max = self.get_count() - max(window)
        for center in range(n_min, n_max):
            sliding_window = [center + n for n in window]
            if as_series:
                yield self.items_no(sliding_window, extend=extend, default=default)
            else:
                yield self.get_item_no(sliding_window, extend=extend, default=default)

    def apply_window_func(self, function, window=[-1, 0, 1], extend=True, default=None, as_series=False):
        return self.new().set_items(
            map(function, self.get_sliding_window(window, extend=extend, default=default, as_series=as_series))
        )

    def mark_local_max(self):
        return self.apply_window_func(
            lambda x_left, x_center, x_right: x_center > x_left and x_center >= x_right,
            window=[-1, 0, 1],
            extend=True,
            default=False,
        )

    def mark_local_min(self):
        return self.apply_window_func(
            lambda x_left, x_center, x_right: x_center < x_left and x_center <= x_right,
            window=[-1, 0, 1],
            extend=True,
            default=False,
        )

    def simple_smooth(self, window_len=3, exclude_center=False):
        center = int((window_len - 1) / 2)
        count = self.get_count()
        result = self.new()
        for n in self.get_range_numbers():
            is_edge = n < center or n >= count - center
            if is_edge:
                result.append(self.get_item_no(n))
            else:
                sub_series = self.slice(n - center, n + center + 1)
                if exclude_center:
                    sub_series = sub_series.drop_item_no(center)
                result.append(sub_series.get_mean())
        return result

    def smooth(self, window=[-1, 0, 1]):
        return self.apply_window_func(
            lambda s: s.get_mean(),
            window=window, extend=True, default=None,
            as_series=True,
        )

    def deviation_from_neighbors(self, window=[-1, 1]):
        return self.subtract(
            self.smooth(window)
        )

    def mark_spikes(self, threshold):
        return self.deviation_from_neighbors().map_zip_values(
            lambda x, m: x if m else None,
            self.mark_local_max(),
        ).map_values(
            lambda x: x > threshold,
        )

