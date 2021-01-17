try:  # Assume we're a sub-module in a package.
    from series.any_series import AnySeries
    from series.date_series import DateSeries
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..series.any_series import AnySeries
    from ..series.date_series import DateSeries


def test_smooth():
    data = [2, 5, 2]
    expected = [2, 3.0, 2]
    received = AnySeries(data).smooth(3).get_list()
    assert received == expected


def test_get_nearest_date():
    data = {'2020-01-01': 10, '2021-01-01': 20}
    cases = ['2019-12-01', '2020-02-01', '2020-12-01', '2021-12-02']
    expected = ['2020-01-01', '2020-01-01', '2021-01-01', '2021-01-01']
    received = [DateSeries.from_dict(data).get_nearest_date(d) for d in cases]
    assert received == expected


def test_get_segment_for_date():
    data = {'2020-01-01': 10, '2021-01-01': 20, '2022-01-01': 30}
    cases = ['2019-12-01', '2020-02-01', '2021-02-01']
    expected = [
        [('2020-01-01', 10)],
        [('2020-01-01', 10), ('2021-01-01', 20)],
        [('2021-01-01', 20), ('2022-01-01', 30)],
    ]
    received = [DateSeries.from_dict(data).get_segment_for_date(d).get_list() for d in cases]
    assert received == expected


def test_get_interpolated_value():
    data = {'2019-01-01': 375, '2020-01-01': 10}
    cases = ['2018-12-01', '2019-02-01', '2019-12-01', '2020-12-02']
    expected = [375, 344, 41, 10]
    received = [DateSeries.from_dict(data).get_interpolated_value(d) for d in cases]
    assert received == expected


if __name__ == '__main__':
    test_smooth()
    test_get_nearest_date()
    test_get_segment_for_date()
    test_get_interpolated_value()
