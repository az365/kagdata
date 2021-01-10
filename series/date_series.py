try:  # Assume we're a sub-module in a package.
    from series.any_series import AnySeries
    from series.pair_series import PairSeries
    from utils import dates as dt
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..series.any_series import AnySeries
    from ..series.pair_series import PairSeries
    from ..utils import dates as dt


class DateSeries(PairSeries):
    def __init__(
            self,
            dates,
            values,
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
        raise NotImplemented

    def exclude(self, first_date, last_date):
        raise NotImplemented

    def period(self, first_date, last_date):
        raise NotImplemented

    def cropped(self, left_days, right_days):
        raise NotImplemented

    def first_year(self):
        raise NotImplemented

    def last_year(self):
        raise NotImplemented

    def shift(self, distance):
        raise NotImplemented

    def yearly_shift(self, return_series=True):
        for n in range(self.get_count()):
            self.values[n] = dt.get_next_year_date(self.values[n])
        if return_series:
            return self

    def round_to_months(self):
        self.map_keys(dt.get_month_first_date).mean_by_keys()

    def get_distance_series(self, date, take_abs=True):
        raise NotImplemented

    def get_nearest_date(self, date):
        raise NotImplemented

