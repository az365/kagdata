try:  # Assume we're a sub-module in a package.
    from series.any_series import AnySeries
    from series.key_value_series import KeyValueSeries
    from utils import dates as dt
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..series.any_series import AnySeries
    from ..series.key_value_series import KeyValueSeries
    from ..utils import dates as dt


class DateNumericSeries(KeyValueSeries):
    def __init__(
            self,
            dates=[],
            values=[],
            is_sorted=False,
    ):
        super().__init__(
            keys=dates,
            values=values,
            is_sorted=is_sorted,
        )
        self.cached_yoy = None

    def get_dates(self, as_date_type=None):
        if as_date_type:
            return self.key_series().map(dt.to_date).get_values()
        else:
            return self.get_keys()

    def get_first_date(self):
        if self.is_sorted:
            return self.get_first_key()
        else:
            return min(self.get_dates())

    def get_last_date(self):
        if self.is_sorted:
            return self.get_last_key()
        else:
            return max(self.get_dates())

    def get_period_days(self):
        return dt.get_days_between(
            self.get_first_date(),
            self.get_last_date(),
        )

    def exclude(self, first_date, last_date):
        return self.filter_keys(lambda d: d < first_date or d > last_date)

    def period(self, first_date, last_date):
        return self.filter_keys(lambda d: first_date <= d <= last_date)

    def cropped(self, left_days, right_days):
        return self.period(
            dt.get_shifted_date(self.get_first_date(), days=abs(left_days)),
            dt.get_shifted_date(self.get_last_date(), days=-abs(right_days)),
        )

    def first_year(self):
        date_a = self.get_first_date()
        date_b = dt.get_next_year_date(date_a)
        return self.period(date_a, date_b)

    def last_year(self):
        date_b = self.get_last_date()
        date_a = dt.get_next_year_date(date_b, increment=-1)
        return self.period(date_a, date_b)

    def shift(self, distance):
        return self.shift_dates(distance)

    def shift_dates(self, distance):
        return self.map_keys(lambda d: dt.get_shifted_date(d, days=distance))

    def yearly_shift(self):
        return self.map_keys(dt.get_next_year_date())

    def round_to_months(self):
        self.map_keys(dt.get_month_first_date).mean_by_keys()

    def get_distance_series(self, date, take_abs=True):
        distance_series = self.__class__(
            self.key_series(),
            self.key_series().map(lambda d: dt.get_days_between(date, d, take_abs)),
            is_sorted=self.is_sorted,
        )
        return distance_series

    def get_distance_for_nearest_date(self, date, take_abs=True):
        nearest_date = self.get_nearest_date(date)
        return dt.get_days_between(date, nearest_date, take_abs)

    def get_nearest_date(self, date):
        if self.get_count() == 0:
            return None
        elif self.get_count() == 1:
            return self.get_first_key()
        elif self.is_sorted:
            prev_date = None
            prev_distance = None
            for cur_date in self.get_keys():
                if cur_date == date:
                    return cur_date
                else:
                    cur_distance = abs(dt.get_days_between(cur_date, date))
                    if prev_distance is not None:
                        if cur_distance > prev_distance:
                            return prev_date
                    prev_date = cur_date
                    prev_distance = cur_distance
            return cur_date
        else:
            return self.get_distance_series(date, take_abs=True).get_arg_min()

    def get_two_nearest_dates(self, date):
        if self.get_count() < 2:
            return None
        else:
            distance_series = self.get_distance_series(date, take_abs=False)
            if not distance_series.is_sorted:
                distance_series = distance_series.sort_by_keys()
            date_a = distance_series.filter_values(lambda v: v < 0).get_arg_max()
            date_b = distance_series.filter_values(lambda v: v >= 0).get_arg_min()
            return date_a, date_b

    def get_segment_for_date(self, date):
        nearest_dates = [i for i in self.get_two_nearest_dates(date) if i]
        return self.new().from_items(
            [(d, self.get_value(d)) for d in nearest_dates],
            is_sorted=True,
        )

    def get_linear_interpolated_value(self, date, near_for_outside=True):
        segment = self.get_segment_for_date(date)
        if segment.get_count() == 1:
            if near_for_outside:
                return segment.get_first_value()
        elif segment.get_count() == 2:
            [(date_a, value_a), (date_b, value_b)] = segment.get_list()
            segment_days = segment.get_period_days()
            distance_days = dt.get_days_between(date_a, date)
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
        result = self.save_meta()
        for d in dates:
            result.append_pair(d, self.get_interpolated_value(d, use_spline=use_spline))
        if dates == sorted(dates):
            result.is_sorted = True
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
        result = self.save_meta()
        if for_full_window_only:
            dates = self.cropped(left_days, right_days).get_dates()
        else:
            dates = self.get_dates()
        for center_date in dates:
            window - self.period(
                dt.get_shifted_date(d, -left_days),
                dt.get_shifted_date(d, right_days),
            )
            if input_as_dict:
                window = window.get_dict()
            result.append_pair(d, function(window))
        return result

    def apply_interpolated_window_series_function(
            self,
            window_days_list,
            function,
            input_as_list=False,
            for_full_window_only=False,
    ):
        result = self.save_meta()
        left_days = min(window_days_list)
        right_days = max(window_days_list)
        if for_full_window_only:
            dates = self.cropped(left_days, right_days).get_dates()
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
        assert isinstance(series, DateNumericSeries)
        result = self.save_meta()
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
