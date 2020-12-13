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
    ):
        self.keys = keys
        super().__init__(
            values=values,
        )

    @classmethod
    def from_dict(cls, my_dict, sort=True):
        pair_series = cls()
        if sort:
            for k in sorted(my_dict):
                pair_series.append_pair(k, my_dict[k])
        else:
            for i in my_dict.items():
                pair_series.append_pair(*i)

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
        return self.append(key, value, return_series)

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
            return PairSeries(keys, values)

    def filter_keys(self, function):
        return self.filter_pairs(lambda k, v: function(k))

    def filter_values(self, function):
        return self.filter_pairs(lambda k, v: function(v))

    def filter_keys_defined(self):
        return self.filter_keys(is_defined)

    def filter_keys_between(self, key_min, key_max):
        return self.filter_keys(lambda k: key_min <= k <= key_max)
