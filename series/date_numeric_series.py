try:  # Assume we're a sub-module in a package.
    import series_classes as sc
    from utils import dates as dt
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import series_classes as sc
    from ..utils import dates as dt


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
            sort_items=False,
            validate=False,
        )
        self.cached_yoy = None
        if sort_items:
            self.sort_by_keys(inplace=True)
        if validate:
            self.validate()

    def get_errors(self):
        yield from self.assume_not_numeric().get_errors()
        if not self.date_series().is_valid():
            yield 'Keys of {} must be sorted dates'.format(self.get_class_name())
        if not self.value_series().is_valid():
            yield 'Values of {} must be numeric'.format(self.get_class_name())

    @staticmethod
    def get_distance_func():
        return sc.DateSeries.get_distance_func()

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

    def get_segment_for_date(self, date):
        nearest_dates = [i for i in self.get_two_nearest_dates(date) if i]
        return self.new().from_items(
            [(d, self.get_value(d)) for d in nearest_dates],
        )

    def get_nearest_value(self, value, distance_func=None):
        return self.value_series().get_nearest_value(value, distance_func)

    def get_linear_interpolated_value(self, date, near_for_outside=True):
        segment = self.get_segment_for_date(date)
        if segment.get_count() == 1:
            if near_for_outside:
                return segment.get_first_value()
        elif segment.get_count() == 2:
            [(date_a, value_a), (date_b, value_b)] = segment.get_list()
            segment_days = segment.get_period_days()
            distance_days = self.get_distance_func()(date_a, date)
            interpolated_value = value_a + (value_b - value_a) * distance_days / segment_days
            return interpolated_value

    def get_spline_interpolated_value(self, date):
        raise NotImplemented

    def get_interpolated_value(self, date, use_spline=False):
        value = self.get_value(date)
        if value:
            return value
        if use_spline:
            return self.get_spline_interpolated_value(date)
        else:
            return self.get_linear_interpolated_value(date)

    def interpolate(self, dates, use_spline=False):
        result = self.new(save_meta=True)
        for d in dates:
            result.append_pair(d, self.get_interpolated_value(d, use_spline=use_spline))
        return result

    def interpolate_to_weeks(self, use_spline=True):
        monday_dates = dt.get_weeks_range(self.get_first_date(), self.get_last_date())
        return self.interpolate(monday_dates, use_spline=use_spline)

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
            result.append_pair(center_date, function(window))
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
                result.append_pair(d, function(window))
        return result

    def smooth(self, window_days_list=[-7, 0, 7]):
        return self.apply_interpolated_window_series_function(
            window_days_list=window_days_list,
            function=lambda s: s.get_mean(),
            input_as_list=False,
            for_full_window_only=False,
        )

    def math(self, series, function, use_spline=False):
        assert isinstance(series, sc.DateSeries)
        result = self.new(save_meta=True)
        for d, v in self.get_items():
            if v is not None:
                v0 = series.get_interpolated_value(d, use_spline=use_spline)
                if v0 is not None:
                    result.append(d, function(v, v0))
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

    def extrapolate_by_yoy(self, date_min, date_max):
        raise NotImplemented

    def extrapolate(self, date_min, date_max):
        raise NotImplemented
