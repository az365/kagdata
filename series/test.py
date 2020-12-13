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


def test_smooth():
    data = [2, 5, 2]
    expected = [2, 3.0, 2]
    received = AnySeries(data).smooth(3).get_list()
    assert received == expected


if __name__ == '__main__':
    test_smooth()
