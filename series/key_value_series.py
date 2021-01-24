try:  # Assume we're a sub-module in a package.
    import series_classes as sc
    from utils import numeric as nm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import series_classes as sc
    from ..utils import numeric as nm


class KeyValueSeries(sc.AnySeries):
    def __init__(
            self,
            keys=[],
            values=[],
            validate=True,
    ):
        self.keys = list(keys)
        super().__init__(
            values=values,
            validate=validate,
        )

    def get_errors(self):
        yield from super().get_errors()
        if not self.has_valid_counts():
            yield 'Keys and values count for {} must be similar'.format(self.get_class_name())

    def has_valid_counts(self):
        return len(self.get_keys()) == len(self.get_values())

    @classmethod
    def get_data_fields(cls):
        return ['keys', 'values']

    @classmethod
    def from_items(cls, items):
        series = cls()
        for k, v in items:
            series.get_keys().append(k)
            series.get_values().append(v)
        return series

    @classmethod
    def from_dict(cls, my_dict):
        series = cls()
        for k in sorted(my_dict):
            series.append_pair(k, my_dict[k], inplace=True)
        return series.assume_sorted()

    def key_series(self):
        return sc.AnySeries(self.get_keys())

    def value_series(self):
        return sc.AnySeries(self.get_values())

    def get_value(self, key, default=None):
        return self.get_dict().get(key, default)

    def get_keys(self):
        return self.keys

    def set_keys(self, keys):
        return self.new(
            keys=keys,
            values=self.get_values(),
        )

    def get_items(self):
        return zip(self.get_keys(), self.get_values())

    def set_items(self, items):
        return self.from_items(items)

    def get_dict(self):
        return dict(self.get_items())

    def get_arg_min(self):
        min_value = None
        key_for_min_value = None
        for k, v in self.get_items():
            if min_value is None or v < min_value:
                min_value = v
                key_for_min_value = k
        return key_for_min_value

    def get_arg_max(self):
        max_value = None
        key_for_max_value = None
        for k, v in self.get_items():
            if max_value is None or v > max_value:
                max_value = v
                key_for_max_value = k
        return key_for_max_value

    def append(self, item, inplace):
        assert len(item) == 2, 'Len of pair mus be 2 (got {})'.format(item)
        key, value = item
        return self.append_pair(key, value, inplace)

    def append_pair(self, key, value, inplace):
        if inplace:
            self.get_keys().append(key)
            self.get_values().append(value)
        else:
            new = self.copy()
            new.get_keys().append(key)
            new.get_values().append(value)
            return new

    def add(self, key_value_series, to_the_begin=False):
        assert isinstance(key_value_series, sc.KeyValueSeries)
        if to_the_begin:
            keys = key_value_series.get_keys() + self.get_keys()
            values = key_value_series.get_values() + self.get_values()
        else:
            keys = self.get_keys() + key_value_series.get_keys()
            values = self.get_values() + key_value_series.get_values()
        return self.new(
            keys=keys,
            values=values,
            save_meta=True,
        )

    def filter_pairs(self, function):
        keys, values = list(), list()
        for k, v in self.get_items():
            if function(k, v):
                keys.append(k)
                values.append(v)
        return self.new(
            keys=keys,
            values=values,
        )

    def filter_keys(self, function):
        return self.filter_pairs(lambda k, v: function(k))

    def filter_values(self, function):
        return self.filter_pairs(lambda k, v: function(v))

    def filter_values_defined(self):
        return self.filter_values(nm.is_defined)

    def filter_keys_defined(self):
        return self.filter_keys(nm.is_defined)

    def filter_keys_between(self, key_min, key_max):
        return self.filter_keys(lambda k: key_min <= k <= key_max)

    def map_keys(self, function, sorting_changed=False):
        return self.set_keys(
            self.key_series().map(function),
        )

    def assume_date_numeric(self):
        return sc.DateNumericSeries(
            **self.get_properties()
        )

    def assume_sorted(self):
        return sc.SortedKeyValueSeries(
            **self.get_properties()
        )

    def is_sorted(self, check=True):
        return self.key_series().is_sorted(check=check)

    def sort_by_keys(self, reverse=False, inplace=False):
        if inplace:
            items = sorted(zip(self.get_keys(), self.get_values()), reverse=reverse)
            self.keys = [k for k, v in items]
            self.values = [v for k, v in items]
        else:
            result = self.__class__.from_items(
                sorted(self.get_items(), reverse=reverse),
            )
            if reverse:
                return result
            else:
                return result.assume_sorted()

    def group_by_keys(self):
        dict_groups = dict()
        for k, v in self.get_items():
            dict_groups[k] = dict_groups.get(k, []) + [v]
        return __class__().from_dict(dict_groups)

    def sum_by_keys(self):
        return self.group_by_keys().map(sum)

    def mean_by_keys(self):
        return self.group_by_keys().map(
            lambda a: sc.AnySeries(a).filter_values_defined().get_mean(),
        )

    @staticmethod
    def get_names():
        return 'key', 'value'

    def get_dataframe(self):
        return nm.get_dataframe(
            data=self.get_list(),
            columns=self.get_names(),
        )

    def plot(self, fmt='-'):
        nm.plot(self.get_keys(), self.get_values(), fmt=fmt)
