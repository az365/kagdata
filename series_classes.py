try:  # Assume we're a sub-module in a package.
    from series.abstract_series import AbstractSeries
    from series.any_series import AnySeries
    from series.numeric_series import NumericSeries
    from series.sorted_series import SortedSeries
    from series.sorted_numeric_series import SortedNumericSeries
    from series.date_series import DateSeries
    from series.key_value_series import KeyValueSeries
    from series.sorted_key_value_series import SortedKeyValueSeries
    from series.sorted_numeric_key_value_series import SortedNumericKeyValueSeries
    from series.date_numeric_series import DateNumericSeries
    from utils import numeric as nm
    from utils import dates as dt
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .series.abstract_series import AbstractSeries
    from .series.any_series import AnySeries
    from .series.numeric_series import NumericSeries
    from .series.sorted_series import SortedSeries
    from .series.sorted_numeric_series import SortedNumericSeries
    from .series.date_series import DateSeries
    from .series.key_value_series import KeyValueSeries
    from .series.sorted_key_value_series import SortedKeyValueSeries
    from .series.sorted_numeric_key_value_series import SortedNumericKeyValueSeries
    from .series.date_numeric_series import DateNumericSeries
    from .utils import numeric as nm
    from .utils import dates as dt
