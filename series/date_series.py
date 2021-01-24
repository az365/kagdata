try:  # Assume we're a sub-module in a package.
    import series_classes as sc
    from utils import dates as dt
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import series_classes as sc
    from ..utils import dates as dt


DEFAULT_NUMERIC = False
DEFAULT_SORTED = True


class DateSeries(sc.SortedSeries):
    def __init__(
            self,
            values=[],
            validate=True,
            sort_items=False,
    ):
        super().__init__(
            values=values,
            validate=validate,
            sort_items=sort_items,
        )

    def get_errors(self):
        yield from super().get_errors()
        if not self.has_valid_dates():
            yield 'Values of {} must be python-dates or iso-dates'.format(self.get_class_name())

    def has_valid_dates(self):
        for d in self.get_dates():
            if not self.is_valid_date(d):
                return False
        return True

    @staticmethod
    def is_valid_date(date):
        return isinstance(date, dt.date) or dt.check_iso_date(date)

    @staticmethod
    def get_distance_func():
        return dt.get_days_between

    @staticmethod
    def is_numeric(*_):
        return DEFAULT_NUMERIC

    def is_dates(self, check=False):
        if check:
            return self.has_valid_dates()
        else:
            return True

    def get_dates(self, as_date_type=False):
        if as_date_type:
            return self.map_values(dt.to_date).get_values()
        else:
            return self.get_values()

    def set_dates(self, dates):
        new = self.new(save_meta=True)
        new.values = dates
        return new

    def to_dates(self, as_iso_date=False):
        return self.map_dates(
            lambda i: dt.to_date(i, as_iso_date=as_iso_date),
        )

    def to_days(self):
        return self.map_dates(dt.get_day_abs_from_date).assume_numeric()

    def date_series(self):
        return DateSeries(
            self.get_dates(),
            sort_items=False,
            validate=False,
        )

    def get_first_date(self):
        if self.get_count():
            return self.get_dates()[0]

    def get_last_date(self):
        if self.get_count():
            return self.get_dates()[-1]

    def get_border_dates(self):
        return [self.get_first_date(), self.get_last_date()]

    def get_mutual_border_dates(self, other):
        assert isinstance(other, (sc.DateSeries, sc.DateNumericSeries))
        first_date = max(self.get_first_date(), other.get_first_date())
        last_date = min(self.get_last_date(), other.get_last_date())
        if first_date < last_date:
            return [first_date, last_date]

    def border_dates(self, other=None):
        if other:
            return DateSeries(self.get_mutual_border_dates(other))
        else:
            return DateSeries(self.get_border_dates())

    def get_range_len(self):
        return self.get_distance_func()(
            *self.get_border_dates()
        )

    def has_date_in_range(self, date):
        return self.get_first_date() <= date <= self.get_last_date()

    def map_dates(self, function):
        return self.set_dates(
            map(function, self.get_dates()),
        )

    def filter_dates(self, function):
        return self.filter(function)

    def exclude(self, first_date, last_date):
        return self.filter_dates(lambda d: d < first_date or d > last_date)

    def period(self, first_date, last_date):
        return self.filter_dates(lambda d: first_date <= d <= last_date)

    def crop(self, left_days, right_days):
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
        return self.map_dates(lambda d: dt.get_shifted_date(d, days=distance))

    def yearly_shift(self):
        return self.map_dates(dt.get_next_year_date)

    def round_to_weeks(self):
        return self.map_dates(dt.get_monday_date).uniq()

    def round_to_months(self):
        return self.map_dates(dt.get_month_first_date).uniq()

    def distance(self, d, take_abs=True):
        if isinstance(d, (str, dt.date)):
            distance_series = self.distance_for_date(d, take_abs=take_abs)
        elif isinstance(d, sc.DateSeries):
            date_series = self.new(d, validate=False, sort_items=False)
            distance_series = sc.DateNumericSeries(
                self.get_dates(),
                self.date_series().map(lambda i: date_series.get_distance_for_nearest_date(i, take_abs)),
                sort_items=False, validate=False,
            )
        else:
            raise TypeError('d-argument for distance-method must be date or DateSeries (got {}: {})'.format(type(d), d))
        return distance_series

    def distance_for_date(self, date, take_abs=True):
        return sc.DateNumericSeries(
            self.get_dates(),
            self.date_series().map(lambda d: self.get_distance_func()(date, d, take_abs)),
            sort_items=False, validate=False,
        )

    def get_distance_for_nearest_date(self, date, take_abs=True):
        nearest_date = self.get_nearest_date(date)
        return self.get_distance_func()(date, nearest_date, take_abs)

    def get_nearest_date(self, date, distance_func=None):
        return self.date_series().get_nearest_value(
            date,
            distance_func=distance_func or self.get_distance_func(),
        )

    def get_two_nearest_dates(self, date):
        if self.get_count() < 2:
            return None
        else:
            distance_series = self.date_series().distance(date, take_abs=False)
            date_a = distance_series.filter_values(lambda v: v < 0).get_arg_max()
            date_b = distance_series.filter_values(lambda v: v >= 0).get_arg_min()
            return date_a, date_b

    def get_segment(self, date):
        nearest_dates = [i for i in self.get_two_nearest_dates(date) if i]
        return self.new(nearest_dates)

    def interpolate_to_weeks(self):
        monday_dates = dt.get_weeks_range(self.get_first_date(), self.get_last_date())
        return self.new(monday_dates)
