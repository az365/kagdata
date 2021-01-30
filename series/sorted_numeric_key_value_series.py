try:  # Assume we're a sub-module in a package.
    import series_classes as sc
    from utils import numeric as nm
    from utils import dates as dt
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import series_classes as sc
    from ..utils import numeric as nm
    from ..utils import dates as dt


class SortedNumericKeyValueSeries(sc.SortedKeyValueSeries, sc.SortedNumericSeries):
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
            validate=validate,
            sort_items=sort_items,
        )
        self.cached_spline = None

    def get_errors(self):
        yield from super().get_errors()
        if not self.key_series().assume_numeric().has_valid_items():
            yield 'Keys of {} must be int of float'.format(self.get_class_name())
        if not self.value_series().has_valid_items():
            yield 'Values of {} must be int of float'.format(self.get_class_name())

    @staticmethod
    def get_distance_func():
        return sc.NumericSeries.get_distance_func()

    @classmethod
    def get_meta_fields(cls):
        return list(super().get_meta_fields()) + ['cached_spline']

    def set_meta(self, dict_meta, inplace=False):
        if inplace:
            for k, v in dict_meta.items():
                if hasattr(v, 'copy') and k != 'cached_spline':
                    v = v.copy()
                self.__dict__[k] = v
        else:
            return super().set_meta(dict_meta, inplace=inplace)

    def key_series(self):
        return sc.SortedNumericSeries(self.get_keys())

    def value_series(self):
        return sc.NumericSeries(self.get_values())

    def get_numeric_keys(self):
        return self.get_keys()

    def assume_numeric(self, validate=False):
        return self.validate() if validate else self

    def assume_not_numeric(self, validate=False):
        return sc.SortedKeyValueSeries(
            validate=validate,
            **self.get_data()
        )

    def to_dates(self, as_iso_date=False, from_scale='days'):
        return self.map_keys(
            function=lambda d: dt.get_date_from_numeric(d, from_scale=from_scale),
            sorting_changed=False,
        ).assume_dates()

    def get_range_len(self):
        return self.get_distance_func()(
            *self.key_series().get_borders()
        )

    def distance(self, v, take_abs=True):
        return self.key_series().distance(v, take_abs)

    def get_nearest_key(self, key):
        return self.key_series().get_nearest_value(
            key,
            distance_func=self.get_distance_func(),
        )

    def get_nearest_item(self, key):
        nearest_key = self.get_nearest_key(key)
        return nearest_key, self.get_value(nearest_key)

    def get_two_nearest_keys(self, key):
        if self.get_count() < 2:
            return None
        else:
            distance_series = self.distance(key, take_abs=False)
            date_a = distance_series.filter_values(lambda v: v < 0).get_arg_max()
            date_b = distance_series.filter_values(lambda v: v >= 0).get_arg_min()
            return date_a, date_b

    def get_segment(self, key):
        nearest_keys = [i for i in self.get_two_nearest_keys(key) if i]
        return self.new().from_items(
            [(d, self.get_value(d)) for d in nearest_keys],
        )

    def derivative(self, extend=False, default=0):
        dx = self.key_series().derivative(extend=extend)
        dy = self.value_series().derivative(extend=extend)
        derivative = dy.divide(dx, default=default)
        return self.new(
            keys=self.get_numeric_keys(),
            values=derivative.get_values(),
            sort_items=False, validate=False, save_meta=True,
        )

    def get_spline_function(self, from_cache=True, to_cache=True):
        if from_cache and self.cached_spline:
            spline_function = self.cached_spline
        else:
            spline_function = nm.spline_interpolate(
                self.get_numeric_keys(),
                self.get_values(),
            )
            if to_cache:
                self.cached_spline = spline_function
        return spline_function

    def get_spline_interpolated_value(self, key, default=None):
        if self.has_key_in_range(key):
            spline_function = self.get_spline_function(from_cache=True, to_cache=True)
            return float(spline_function(key))
        else:
            return default

    def get_linear_interpolated_value(self, key, near_for_outside=True):
        segment = self.get_segment(key)
        if segment.get_count() == 1:
            if near_for_outside:
                return segment.get_first_value()
        elif segment.get_count() == 2:
            [(key_a, value_a), (key_b, value_b)] = segment.get_list()
            segment_days = segment.get_range_len()
            distance_days = self.get_distance_func()(key_a, key)
            interpolated_value = value_a + (value_b - value_a) * distance_days / segment_days
            return interpolated_value

    def get_interpolated_value(self, key, how='linear', *args, **kwargs):
        method_name = 'get_{}_interpolated_value'.format(how)
        interpolation_method = self.__getattribute__(method_name)
        return interpolation_method(key, *args, **kwargs)

    def interpolate(self, keys, how='linear', *args, **kwargs):
        method_name = '{}_interpolation'.format(how)
        interpolation_method = self.__getattribute__(method_name)
        return interpolation_method(keys, *args, **kwargs)

    def linear_interpolation(self, keys, near_for_outside=True):
        result = self.new(save_meta=True)
        for k in keys:
            result.append_pair(k, self.get_linear_interpolated_value(k, near_for_outside), inplace=True)
        return result

    def spline_interpolation(self, keys):
        spline_function = self.get_spline_function(from_cache=True, to_cache=True)
        result = self.new(
            keys=keys,
            values=spline_function(list(keys)),
            save_meta=True,
        )
        return result
