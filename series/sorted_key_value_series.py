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
            # keys=keys if isinstance(keys, sc.SortedSeries) else sc.SortedSeries(
            #     keys, validate=False, sort_items=False,
            # ),
            # keys=keys.get_values() if isinstance(keys, sc.SortedSeries) else keys,
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
        return 'cached_spline'

    def key_series(self):
        return sc.SortedSeries(self.get_keys())

    # def get_nearest_key(self, key, distance_func):
    #     return self.key_series().get_nearest_value(key, distance_func)
    #
    # def get_nearest_item(self, key, distance_func):
    #     nearest_key = self.get_nearest_key(key, distance_func)
    #     return nearest_key, self.get_value(nearest_key)

    def has_key_in_range(self, key):
        return self.get_first_key() <= key <= self.get_last_key()

    def get_first_key(self):
        if self.get_count():
            return self.get_keys()[0]

    def get_last_key(self):
        if self.get_count():
            return self.get_keys()[-1]

    def get_first_value(self):
        if self.get_count():
            return self.get_values()[0]

    def get_last_value(self):
        if self.get_count():
            return self.get_values()[-1]

    def get_first_item(self):
        return self.get_first_key(), self.get_first_value()

    def get_last_item(self):
        return self.get_last_key(), self.get_last_value()

    def borders(self):
        return self.__class__.from_items(
            *self.get_border_keys()
        )

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

    # # def slice(self, n_start, n_end):
    # #     return self.filter_keys(lambda k: n_start <= k < n_end)
    #
    # def derivative(self):
    #     return self.new(
    #         keys=self.get_keys(),
    #         values=self.value_series().assume_numeric().derivative(extend=True).get_values(),
    #         sort_items=False, validate=False, save_meta=True,
    #     )
    #
    # def get_spline_function(self, from_cache=True, to_cache=True):
    #     if from_cache and self.cached_spline:
    #         spline_function = self.cached_spline
    #     else:
    #         spline_function = nm.spline_interpolate(
    #             self.get_keys(),
    #             self.get_values(),
    #         )
    #         if to_cache:
    #             self.cached_spline = spline_function
    #     return spline_function
    #
    # def get_spline_interpolated_value(self, key, default=None):
    #     if self.has_key_in_range(key):
    #         spline_function = self.get_spline_function(from_cache=True, to_cache=True)
    #         return spline_function
    #     else:
    #         return default
    #
    # def assume_numeric(self, validate=False):
    #     return sc.SortedNumericKeyValueSeries(
    #         keys=self.key_series().assume_numeric(),
    #         values=self.value_series().assume_numeric(),
    #     )
