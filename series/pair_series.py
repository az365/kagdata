try:  # Assume we're a sub-module in a package.
    from series.any_series import (
        AnySeries,
        is_defined,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..series.any_series import (
        AnySeries,
        is_defined,
    )


class PairSeries(AnySeries):
    def __init__(
            self,
            keys=list(),
            values=list(),
            is_sorted=False,
    ):
        self.keys = keys.get_values() if isinstance(keys, AnySeries) else list(keys)
        self.is_sorted = is_sorted
        super().__init__(
            values=values,
        )

    @classmethod
    def from_items(cls, items, is_sorted=False):
        series = cls()
        for i in items:
            series.append_pair(*i)
        series.is_sorted = is_sorted
        return series

    @classmethod
    def from_dict(cls, my_dict):
        series = cls()
        for k in sorted(my_dict):
            series.append_pair(k, my_dict[k])
        series.is_sorted = True
        return series

    def new(self):
        return self.__class__(is_sorted=self.is_sorted)

    def key_series(self):
        return AnySeries(self.keys)

    def value_series(self):
        return AnySeries(self.values)

    def get_keys(self):
        return self.keys

    def get_items(self):
        return zip(self.keys, self.values)

    def get_dict(self):
        return dict(self.get_items())

    def append(self, item, return_series=False):
        assert len(item) == 2, 'Len of pair mus be 2 (got {})'.format(item)
        key, value = item
        return self.append_pair(key, value, return_series)

    def append_pair(self, key, value, return_series=False):
        self.keys.append(key)
        self.values.append(value)
        if return_series:
            return self

    def add(self, pair_series, return_series=True):
        for i in pair_series.get_items():
            self.append_pair(*i)
        if return_series:
            return self

    def filter_pairs(self, function):
        keys, values = list(), list()
        for k, v in self.get_items():
            if function(k, v):
                keys.append(k)
                values.append(v)
        return __class__(keys, values, is_sorted=self.is_sorted)

    def filter_keys(self, function):
        return self.filter_pairs(lambda k, v: function(k))

    def filter_values(self, function):
        return self.filter_pairs(lambda k, v: function(v))

    def filter_values_defined(self):
        return self.filter_values(is_defined)

    def filter_keys_defined(self):
        return self.filter_keys(is_defined)

    def filter_keys_between(self, key_min, key_max):
        return self.filter_keys(lambda k: key_min <= k <= key_max)

    def map_keys(self, function, sorting_changed=False):
        for n in self.get_range_numbers():
            self.keys[n] = function(self.keys[n])
        if sorting_changed:
            self.is_sorted = False
        return self

    def sort_by_keys(self, reverse=False):
        return __class__().from_items(
            sorted(self.get_items(), reverse=reverse),
        )

    def group_by_keys(self):
        dict_groups = dict()
        for k, v in self.get_items():
            dict_groups[k] = dict_groups.get(k, []) + [v]
        return __class__().from_dict(dict_groups)

    def sum_by_keys(self):
        self.group_by_keys().map(sum)

    def mean_by_keys(self):
        self.group_by_keys().map(lambda a: AnySeries(a).filter_values_defined().mean_value())
