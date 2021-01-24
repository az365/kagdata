try:  # Assume we're a sub-module in a package.
    import series_classes as sc
    from utils import numeric as nm
    from utils import dates as dt
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import series_classes as sc
    from ..utils import numeric as nm
    from ..utils import dates as dt


class SortedKeyValueSeries(sc.KeyValueSeries, sc.SortedSeries):
    def __init__(
            self,
            keys=[],
            values=[],
            validate=False,
            sort_items=True,
    ):
        super().__init__(
            keys=keys,
            values=values,
            validate=False,
        )
        if sort_items:
            self.sort_by_keys(inplace=True)
        if validate:
            self.validate()

    def get_errors(self):
        yield from super().get_errors()
        if not self.is_sorted(check=True):
            yield 'Keys of {} must be sorted'.format(self.get_class_name())

    @classmethod
    def get_meta_fields(cls):
        return list(super().get_meta_fields()) + ['cached_spline']

    def key_series(self):
        return sc.SortedSeries(self.get_keys())

    def has_key_in_range(self, key):
        return self.get_first_key() <= key <= self.get_last_key()

    def get_first_key(self):
        if self.get_count():
            return self.get_keys()[0]

    def get_last_key(self):
        if self.get_count():
            return self.get_keys()[-1]

    def get_first_item(self):
        return self.get_first_key(), self.get_first_value()

    def get_last_item(self):
        return self.get_last_key(), self.get_last_value()

    def get_border_keys(self):
        return [self.get_first_key(), self.get_last_key()]

    def get_mutual_border_keys(self, other):
        assert isinstance(other, sc.SortedKeyValueSeries)
        first_key = max(self.get_first_key(), other.get_first_key())
        last_key = min(self.get_last_key(), other.get_last_key())
        if first_key < last_key:
            return [first_key, last_key]

    def assume_sorted(self):
        return self

    def assume_unsorted(self):
        return sc.KeyValueSeries(
            **self.get_data()
        )

    def assume_dates(self, validate=False):
        return sc.DateNumericSeries(
            validate=validate,
            **self.get_data()
        )

    def assume_numeric(self, validate=False):
        return sc.SortedNumericKeyValueSeries(
            validate=validate,
            **self.get_data()
        )

    def to_numeric(self, sort_items=True):
        series = self.map_keys_and_values(float, float, sorting_changed=False)
        if sort_items:
            series = series.sort()
        return series.assert_numeric()

    def copy(self):
        return self.new(
            keys=self.get_keys().copy(),
            values=self.get_values().copy(),
            sort_items=False,
            validate=False,
        )

    def map_keys(self, function, sorting_changed=True):
        result = self.set_keys(
            self.key_series().map(function),
        )
        if sorting_changed:
            result = result.assume_unsorted()
        return result

    def map_keys_and_values(self, key_function, value_function, sorting_changed=False):
        return self.map_keys(key_function, sorting_changed).map_values(value_function)

    def exclude(self, first_key, last_key):
        return self.filter_keys(lambda k: k < first_key or k > last_key)

    def span(self, first_key, last_key):
        return self.filter_keys(lambda k: first_key <= k <= last_key)
