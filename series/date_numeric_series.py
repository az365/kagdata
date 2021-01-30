try:  # Assume we're a sub-module in a package.
    import series_classes as sc
    from utils import dates as dt
    from utils import numeric as nm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import series_classes as sc
    from ..utils import dates as dt
    from ..utils import numeric as nm


WINDOW_WEEKLY_DEFAULT = (-7, 0, 7)


class DateNumericSeries(sc.SortedNumericKeyValueSeries, sc.DateSeries):
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
            sort_items=sort_items,
            validate=validate,
        )
        self.cached_yoy = None

    def get_errors(self):
        yield from self.assume_not_numeric().get_errors()
        if not self.date_series().is_valid():
            yield 'Keys of {} must be sorted dates'.format(self.get_class_name())
        if not self.value_series().is_valid():
            yield 'Values of {} must be numeric'.format(self.get_class_name())

    @staticmethod
    def get_distance_func():
        return sc.DateSeries.get_distance_func()

    def assume_numeric(self, validate=False):
        return sc.SortedNumericKeyValueSeries(
            validate=validate,
            **self.get_data()
        )

    def get_dates(self, as_date_type=None):
        if as_date_type:
            return self.date_series().map(dt.to_date).get_values()
        else:
            return self.get_keys()

    def set_dates(self, dates):
        return self.new(
            keys=dates,
            values=self.get_values(),
            save_meta=True,
            sort_items=False,
            validate=False,
        )

    def filter_dates(self, function):
        return self.filter_keys(function)

    def get_numeric_keys(self):
        return self.to_days().get_keys()

    def numeric_key_series(self):
        return self.to_days().key_series()

    def key_series(self):
        return self.date_series()

    def value_series(self):
        return super().value_series().assume_numeric()

    def round_to_weeks(self):
        return self.map_dates(dt.get_monday_date).mean_by_keys()

    def round_to_months(self):
        return self.map_dates(dt.get_month_first_date).mean_by_keys()

    def get_segment(self, date):
        nearest_dates = [i for i in self.get_two_nearest_dates(date) if i]
        return self.new().from_items(
            [(d, self.get_value(d)) for d in nearest_dates],
        )

    def get_nearest_value(self, value, distance_func=None):
        return self.value_series().sort().get_nearest_value(value, distance_func)

    def get_interpolated_value(self, date, how='linear', *args, **kwargs):
        method_name = 'get_{}_interpolated_value'.format(how)
        requires_numeric_keys = how in ('linear', 'spline')
        if requires_numeric_keys:
            numeric_series = self.to_days()
            interpolation_method = numeric_series.__getattribute__(method_name)
            numeric_key = dt.get_day_abs_from_date(date)
            return interpolation_method(numeric_key, *args, **kwargs)
        else:
            interpolation_method = self.__getattribute__(method_name)
            return interpolation_method(date, *args, **kwargs)

    def interpolate(self, dates, how='linear', *args, **kwargs):
        method_name = '{}_interpolation'.format(how)
        requires_numeric_keys = how in ('linear', 'spline')
        if requires_numeric_keys:
            interpolation_method = self.to_days().__getattribute__(method_name)
            numeric_keys = sc.DateSeries(dates, sort_items=True).to_days()
            return interpolation_method(numeric_keys, *args, **kwargs).to_dates()
        else:
            interpolation_method = self.__getattribute__(method_name)
            return interpolation_method(dates, *args, **kwargs)

    def weighted_interpolation(self, dates, weight_benchmark, internal='linear'):
        assert isinstance(weight_benchmark, (DateNumericSeries, sc.DateNumericSeries))
        list_dates = dates.get_dates() if isinstance(dates, (sc.DateNumericSeries, sc.DateNumericSeries)) else dates
        border_dates = self.get_mutual_border_dates(weight_benchmark)
        result = self.new(save_meta=True)
        for d in list_dates:
            yearly_dates = dt.get_yearly_dates(d, *border_dates)
            if yearly_dates:
                yearly_primary = self.interpolate(yearly_dates, how=internal)
                yearly_benchmark = weight_benchmark.interpolate(yearly_dates, how=internal)
                weight = yearly_benchmark.divide(yearly_primary).get_mean()
                interpolated_value = self.get_interpolated_value(d, how=internal) * weight
                result.append_pair(d, interpolated_value, inplace=True)
        return result

    def interpolate_to_weeks(self, how='spline', *args, **kwargs):
        monday_dates = dt.get_weeks_range(self.get_first_date(), self.get_last_date())
        return self.interpolate(monday_dates, how=how, *args, **kwargs)

    def apply_window_series_function(
            self,
            window_days_count,
            function,
            input_as_dict=False,
            for_full_window_only=False,
    ):
        half_window_days = window_days_count / 2
        int_half_window_days = int(half_window_days)
        window_days_is_even = half_window_days == int_half_window_days
        left_days = int_half_window_days
        right_days = int_half_window_days if window_days_is_even else int_half_window_days + 1
        result = self.new(save_meta=True)
        if for_full_window_only:
            dates = self.crop(left_days, right_days).get_dates()
        else:
            dates = self.get_dates()
        for center_date in dates:
            window = self.span(
                dt.get_shifted_date(center_date, -left_days),
                dt.get_shifted_date(center_date, right_days),
            )
            if input_as_dict:
                window = window.get_dict()
            result.append_pair(center_date, function(window), inplace=True)
        return result

    def apply_interpolated_window_series_function(
            self,
            window_days_list,
            function,
            input_as_list=False,
            for_full_window_only=False,
    ):
        result = self.new(save_meta=True)
        left_days = min(window_days_list)
        right_days = max(window_days_list)
        if for_full_window_only:
            dates = self.crop(left_days, right_days).get_dates()
        else:
            dates = self.get_dates()
        for d in dates:
            window_dates = [dt.get_shifted_date(d, days) for days in window_days_list]
            window = self.interpolate(window_dates)
            window_values = window.get_values()
            if None not in window_values or not for_full_window_only:
                if input_as_list:
                    window = window_values
                result.append_pair(d, function(window), inplace=True)
        return result

    def smooth_linear_by_days(self, window_days_list=WINDOW_WEEKLY_DEFAULT):
        return self.apply_interpolated_window_series_function(
            window_days_list=window_days_list,
            function=lambda s: s.get_mean(),
            input_as_list=False,
            for_full_window_only=False,
        )

    def math(self, series, function, use_spline=False):
        assert isinstance(series, sc.DateNumericSeries)
        result = self.new(save_meta=True)
        for d, v in self.get_items():
            if v is not None:
                v0 = series.get_interpolated_value(d, use_spline=use_spline)
                if v0 is not None:
                    result.append_pair(d, function(v, v0), inplace=True)
        return result

    def yoy(self, use_spline=False):
        yearly_shifted = self.yearly_shift()
        return self.math(
            yearly_shifted,
            function=lambda a, b: (a - b) / b if b else None,
            use_spline=use_spline,
        )

    def get_yoy_for_date(self, date, use_spline=False):
        if not self.cached_yoy:
            self.cached_yoy = self.yoy(use_spline=use_spline)
        if date in self.cached_yoy:
            return self.get_value(date)
        elif date < self.get_first_date():
            return self.first_year().get_mean()
        elif date > self.get_last_date():
            return self.last_year().get_mean()
        else:
            return self.get_interpolated_value(date, use_spline=use_spline)

    def extrapolate_by_yoy(self, dates, yoy, smooth_kwargs=None, use_spline=False):
        if not yoy:
            yoy = self.yoy()
        if smooth_kwargs is not None:
            yoy = yoy.smooth(**smooth_kwargs)
        result = self.new()
        for d in dates:
            if self.has_date_in_range(d):
                cur_value = self.get_interpolated_value(d, use_spline=use_spline)
            else:
                if d > self.get_last_date():
                    increment = int(dt.get_days_between(self.get_last_date(), d) / dt.DAYS_IN_YEAR) + 1
                else:
                    increment = int(dt.get_days_between(self.get_first_date(), d) / dt.DAYS_IN_YEAR) + 1
                base_date = dt.get_next_year_date(d, increment=-increment)
                base_value = self.get_interpolated_value(base_date)
                cur_yoy = yoy.get_interpolated_value(d, use_spline=use_spline)
                cur_value = base_value * (1 + cur_yoy) ** increment
            result.append_pair(d, cur_value, inplace=True)
        return result

    def extrapolate_by_stl(self, dates):
        raise NotImplemented

    def extrapolate(self, how='by_yoy', *args, **kwargs):
        method_name = 'extrapolate_{}'.format(how)
        extrapolation_method = self.__getattribute__(method_name)
        return extrapolation_method(*args, **kwargs)

    def derivative(self, extend=False, default=0):
        return self.new(
            keys=self.get_keys(),
            values=self.value_series().derivative(extend=extend, default=default).get_values(),
            sort_items=False, validate=False, save_meta=True,
        )

    @staticmethod
    def get_names():
        return ['date', 'value']

    def plot(self, fmt='-'):
        nm.plot(self.get_keys(), self.get_values(), fmt=fmt)
