try:  # Assume we're a sub-module in a package.
    import series_classes as sc
    from utils import (
        numeric as nm,
        dates as dt,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import series_classes as sc
    from ..utils import (
        numeric as nm,
        dates as dt,
    )

DEFAULT_NUMERIC = True
DEFAULT_SORTED = True


class SortedNumericSeries(sc.SortedSeries, sc.NumericSeries):
    def __init__(
            self,
            values=[],
            validate=False,
    ):
        super().__init__(
            values=values,
            validate=validate,
        )

    def get_errors(self):
        if not self.is_sorted(check=True):
            yield 'Values of {} must be sorted'.format(self.get_class_name())
        if not self.has_valid_items():
            yield 'Values of {} must be numeric'.format(self.get_class_name())

    def assume_unsorted(self):
        return sc.NumericSeries(
            validate=False,
            **self.get_data()
        )

    def to_dates(self, as_iso_date=False, from_scale='days'):
        return self.map(
            function=lambda d: dt.get_date_from_numeric(d, from_scale=from_scale),
            sorting_changed=False,
        ).assume_dates()

    def get_range_len(self):
        return self.get_distance_func()(
            *self.get_borders()
        )

    def distance(self, v, take_abs=True):
        got_one_value = isinstance(v, (int, float))
        if got_one_value:
            return self.distance_for_value(v, take_abs=take_abs)
        else:
            series = self.new(v, validate=False, sort_items=True)
            distance_series = sc.SortedNumericKeyValueSeries(
                self.get_values(),
                self.value_series().map(lambda i: series.get_distance_for_nearest_value(i, take_abs)),
                sort_items=False, validate=False,
            )
            return distance_series

    def distance_for_value(self, value, take_abs=True):
        distance_series = sc.SortedNumericKeyValueSeries(
            self.get_values(),
            self.value_series().map(lambda d: self.get_distance_func()(value, d, take_abs)),
        )
        return distance_series

    def get_distance_for_nearest_value(self, value, take_abs=True):
        nearest_value = self.get_nearest_value(value)
        return self.value_series().get_distance_func()(value, nearest_value, take_abs)

    def get_nearest_value(self, value, distance_func=None):
        distance_func = distance_func or self.get_distance_func()
        return super().get_nearest_value(value, distance_func)
