try:  # Assume we're a sub-module in a package.
    import series_classes as sc
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import series_classes as sc


DEFAULT_NUMERIC = False
DEFAULT_SORTED = True


class SortedSeries(sc.AnySeries):
    def __init__(
            self,
            values=[],
            validate=True,
            sort_items=False,
    ):
        if sort_items:
            values = sorted(values)
        super().__init__(
            values=values,
            validate=validate,
        )

    def get_errors(self):
        yield from super().get_errors()
        if not self.is_sorted(check=True):
            yield 'Values of {} must be sorted'.format(self.get_class_name())

    def is_sorted(self, check=False):
        if check:
            return super().is_sorted(check=True)
        else:
            return DEFAULT_SORTED

    def copy(self):
        return self.new(
            validate=False,
        )

    def assume_numeric(self, validate=False):
        return sc.SortedNumericSeries(
            validate=validate,
            **self.get_data()
        )

    def assume_sorted(self):
        return self

    def assume_unsorted(self):
        return sc.AnySeries(**self.get_data())

    def uniq(self):
        series = self.new(save_meta=True)
        prev = None
        for item in self.get_items():
            if prev is None or item != prev:
                series.append(item, inplace=True)
            if hasattr(item, 'copy'):
                prev = item.copy()
            else:
                prev = item
        return series

    def get_nearest_value(self, value, distance_func):
        if self.get_count() == 0:
            return None
        elif self.get_count() == 1:
            return self.get_first_value()
        else:
            prev_value = None
            prev_distance = None
            for cur_value in self.get_values():
                if cur_value == value:
                    return cur_value
                else:
                    cur_distance = abs(distance_func(cur_value, value))
                    if prev_distance is not None:
                        if cur_distance > prev_distance:
                            return prev_value
                    prev_value = cur_value
                    prev_distance = cur_distance
            return cur_value

    def get_two_nearest_values(self, value):
        if self.get_count() < 2:
            return None
        else:
            distance_series = self.distance(value, take_abs=False)
            date_a = distance_series.filter_values(lambda v: v < 0).get_arg_max()
            date_b = distance_series.filter_values(lambda v: v >= 0).get_arg_min()
            return date_a, date_b

    def get_first_value(self):
        if self.get_count():
            return self.get_values()[0]

    def get_last_value(self):
        if self.get_count():
            return self.get_values()[-1]

    def get_first_item(self):
        return self.get_first_value()

    def get_last_item(self):
        return self.get_last_value()

    def get_borders(self):
        return self.get_first_item(), self.get_last_item()

    def get_mutual_borders(self, other):
        assert isinstance(other, (sc.SortedSeries, sc.SortedKeyValueSeries))
        first_item = max(self.get_first_item(), other.get_first_item())
        last_item = min(self.get_last_item(), other.get_last_item())
        if first_item < last_item:
            return [first_item, last_item]

    def borders(self, other=None):
        if other:
            return self.new(self.get_mutual_borders(other))
        else:
            return self.new(self.get_borders())
