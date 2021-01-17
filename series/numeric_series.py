try:  # Assume we're a sub-module in a package.
    from series.any_series import (
        AnySeries,
        is_defined,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..series.any_series import (
        AnySeries,
        is_defined,
    )


class NumericSeries(AnySeries):
    def __init__(
            self,
            values=list(),
            validate=False,
    ):
        super().__init__(
            values=values,
        )
        if validate:
            self.validate()

    @staticmethod
    def get_validation_error():
        return TypeError('values items must be int of float')

    def is_valid(self):
        for v in self.get_values():
            if not isinstance(v, (int, float)):
                return False
        return True

    def get_sum(self):
        return sum(
            self.filter_values_defined().get_values(),
        )

    def get_mean(self):
        values_defined = self.filter_values_defined().get_values()
        return sum(values_defined) / len(values_defined)

    def norm(self, rate=None, default=None):
        if rate is None:
            rate = self.get_mean()
        return self.map_values(lambda v: v / rate if rate else default)

    def divide(self, series, default=None, extend=False):
        return self.map_optionally_extend_zip_values(
            lambda x, y: x / y if y else default,
            extend,
            series,
        )

    def subtract(self, series, default=None, extend=False):
        return self.map_optionally_extend_zip_values(
            lambda x, y: x - y if y is not None else default,
            extend,
            series,
        )

    def derivative(self, extend=False, default=None):
        if extend:
            return self.preface(default).subtract(
                self,
            )
        else:
            return self.slice(0, -1).subtract(
                self.shift(-1)
            )

    def get_sliding_window(self, window=[-1, 0, 1], extend=True, default=None, as_series=True):
        if extend:
            n_min = 0
            n_max = self.get_count()
        else:
            n_min = - min(window)
            n_max = self.get_count() - max(window)
        for center in range(n_min, n_max):
            sliding_window = [center + n for n in window]
            if as_series:
                yield self.items_no(sliding_window, extend=extend, default=default)
            else:
                yield self.get_item_no(sliding_window, extend=extend, default=default)

    def apply_window_func(self, function, window=[-1, 0, 1], extend=True, default=None, as_series=False):
        return self.new().set_items(
            map(function, self.get_sliding_window(window, extend=extend, default=default, as_series=as_series))
        )

    def mark_local_max(self):
        return self.apply_window_func(
            lambda x_left, x_center, x_right: x_center > x_left and x_center >= x_right,
            window=[-1, 0, 1],
            extend=True,
            default=False,
        )

    def mark_local_min(self):
        return self.apply_window_func(
            lambda x_left, x_center, x_right: x_center < x_left and x_center <= x_right,
            window=[-1, 0, 1],
            extend=True,
            default=False,
        )

    def simple_smooth(self, window_len=3, exclude_center=False):
        center = int((window_len - 1) / 2)
        count = self.get_count()
        result = self.new()
        for n in self.get_range_numbers():
            is_edge = n < center or n >= count - center
            if is_edge:
                result.append(self.get_item_no(n))
            else:
                sub_series = self.slice(n - center, n + center + 1)
                if exclude_center:
                    sub_series = sub_series.drop_item_no(center)
                result.append(sub_series.get_mean())
        return result

    def smooth(self, window=[-1, 0, 1]):
        return self.apply_window_func(
            lambda s: s.get_mean(),
            window=window, extend=True, default=None,
            as_series=True,
        )

    def deviation_from_neighbors(self, window=[-1, 1]):
        return self.subtract(
            self.smooth(window)
        )

    def mark_spikes(self, threshold):
        return self.deviation_from_neighbors().map_zip_values(
            lambda x, m: x if m else None,
            self.mark_local_max(),
        ).map_values(
            lambda x: x > threshold,
        )
