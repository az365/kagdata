try:  # Assume we're a sub-module in a package.
    from . import flux
    from . import readers
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    import flux
    import readers


TEST_INT_SEQUENCE = [1, 3, 5, 7, 9, 2, 4, 6, 8]


def test_map():
    expected = [-i for i in TEST_INT_SEQUENCE]
    received = readers.from_list(
        TEST_INT_SEQUENCE,
    ).map(
        lambda i: -i,
    ).to_list()
    assert received == expected


def test_filter():
    expected = [7, 9, 6, 8]
    received = readers.from_list(
        TEST_INT_SEQUENCE,
    ).filter(
        lambda i: i > 5,
    ).to_list()
    assert received == expected


def test_take():
    expected = [1, 3, 5, 7, 9]
    received = readers.from_list(
        TEST_INT_SEQUENCE,
    ).take(
        5,
    ).to_list()
    assert received == expected


def test_map_filter_take():
    expected = [-1, -3, -5]
    received = readers.from_list(
        TEST_INT_SEQUENCE,
    ).map(
        lambda i: -i,
    ).filter(
        lambda i: i % 2,
    ).take(
        3,
    ).to_list()
    assert expected == received


if __name__ == '__main__':
    test_map()
    test_filter()
    test_take()
    test_map_filter_take()
