try:  # Assume we're a sub-module in a package.
    from . import flux
    from . import readers
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    import flux
    import readers


EXAMPLE_FILENAME = 'test_file.tmp'
EXAMPLE_INT_SEQUENCE = [1, 3, 5, 7, 9, 2, 4, 6, 8]
EXAMPLE_CSV_ROWS = [
    'a,1',
    'b,"2,22"',
    'c,3',
]


def test_map():
    expected = [-i for i in EXAMPLE_INT_SEQUENCE]
    received = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).map(
        lambda i: -i,
    ).to_list()
    assert received == expected


def test_filter():
    expected = [7, 9, 6, 8]
    received = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).filter(
        lambda i: i > 5,
    ).to_list()
    assert received == expected


def test_take():
    expected = [1, 3, 5, 7, 9]
    received = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).take(
        5,
    ).to_list()
    assert received == expected


def test_skip():
    expected = [2, 4, 6, 8]
    received = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).skip(
        5,
    ).to_list()
    assert received == expected


def test_map_filter_take():
    expected = [-1, -3, -5]
    received = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).map(
        lambda i: -i,
    ).filter(
        lambda i: i % 2,
    ).take(
        3,
    ).to_list()
    assert received == expected


def test_enumerated():
    expected = list(enumerate(EXAMPLE_INT_SEQUENCE))
    received = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).enumerate().to_list()
    assert received == expected


def test_save_and_read():
    expected = [str(i) for i in EXAMPLE_INT_SEQUENCE]
    received_0 = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).save(
        EXAMPLE_FILENAME,
    ).map(
        str,
    ).to_list()
    received_1 = readers.from_file(
        EXAMPLE_FILENAME
    ).to_list()
    assert received_0 == expected, 'test case 0 failed'
    assert received_1 == expected, 'test case 1 failed'
    readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).to_file(
        EXAMPLE_FILENAME,
    )
    received_2 = readers.from_file(
        EXAMPLE_FILENAME,
    ).to_list()
    assert received_2 == expected, 'test case 2 failed'


def test_add():
    addition = list(reversed(EXAMPLE_INT_SEQUENCE))
    expected_1 = EXAMPLE_INT_SEQUENCE + addition
    expected_2 = addition + EXAMPLE_INT_SEQUENCE
    received_1i = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).add(
        addition
    ).to_list()
    assert received_1i == expected_1
    received_2i = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).add(
        addition,
        before=True,
    ).to_list()
    assert received_2i == expected_2
    received_1f = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).add(
        readers.from_list(addition),
    ).to_list()
    assert received_1f == expected_1
    received_2f = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).add(
        readers.from_list(addition),
        before=True,
    ).to_list()
    assert received_2f == expected_2


def test_separate_first():
    expected = [EXAMPLE_INT_SEQUENCE[0], EXAMPLE_INT_SEQUENCE[1:]]
    received = list(
        readers.from_list(
            EXAMPLE_INT_SEQUENCE,
        ).separate_first()
    )
    received[1] = received[1].to_list()
    assert received == expected


def test_to_rows():
    expected = [['a', '1'], ['b', '2,22'], ['c', '3']]
    received = readers.from_list(
        EXAMPLE_CSV_ROWS,
    ).to_rows(
        ',',
    ).to_list()
    assert received == expected


if __name__ == '__main__':
    test_map()
    test_filter()
    test_take()
    test_skip()
    test_map_filter_take()
    test_enumerated()
    test_save_and_read()
    test_add()
    test_separate_first()
    test_to_rows()
