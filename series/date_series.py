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
    ):
        super().__init__(
            keys=dates,
            values=values,
        )

    def plus_year(self, return_series=True):
        for n in range(self.get_count()):
            self.values[n] = dt.get_next_year_date(self.values[n])
        if return_series:
            return self

    def round_to_months(self):
        pass

    def yoy(self):
        pass

    def get_monthly_yoy(self):
        pass
