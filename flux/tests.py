try:  # Assume we're a sub-module in a package.
    from . import readers
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
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
    ).flat_map(
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
    ).to_lines(
    ).save(
        EXAMPLE_FILENAME,
    ).to_list()
    received_1 = readers.from_file(
        EXAMPLE_FILENAME
    ).to_list()
    assert received_0 == expected, 'test case 0'
    assert received_1 == expected, 'test case 1'
    readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).to_lines(
    ).to_file(
        EXAMPLE_FILENAME,
    )
    received_2 = readers.from_file(
        EXAMPLE_FILENAME,
    ).to_list()
    assert received_2 == expected, 'test case 2'


def test_add():
    addition = list(reversed(EXAMPLE_INT_SEQUENCE))
    expected_1 = EXAMPLE_INT_SEQUENCE + addition
    expected_2 = addition + EXAMPLE_INT_SEQUENCE
    received_1i = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).add(
        addition
    ).to_list()
    assert received_1i == expected_1, 'test case 1i'
    received_2i = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).add(
        addition,
        before=True,
    ).to_list()
    assert received_2i == expected_2, 'test case 2i'
    received_1f = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).add(
        readers.from_list(addition),
    ).to_list()
    assert received_1f == expected_1, 'test case 1f'
    received_2f = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).add(
        readers.from_list(addition),
        before=True,
    ).to_list()
    assert received_2f == expected_2, 'test case 2f'


def test_add_records():
    addition = list(reversed(EXAMPLE_INT_SEQUENCE))
    expected_1 = list(map(lambda v: dict(item=v), EXAMPLE_INT_SEQUENCE + addition))
    expected_2 = list(map(lambda v: dict(item=v), addition + EXAMPLE_INT_SEQUENCE))
    received_1 = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).to_records(
        lambda i: dict(item=i),
    ).add(
        readers.from_list(addition).to_records(),
    ).to_list()
    print(received_1)
    assert received_1 == expected_1, 'test case 1i'
    received_2 = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).to_records(
    ).add(
        readers.from_list(addition).to_records(),
        before=True,
    ).to_list()
    print(received_2)
    assert received_2 == expected_2, 'test case 2i'


def test_separate_first():
    expected = [EXAMPLE_INT_SEQUENCE[0], EXAMPLE_INT_SEQUENCE[1:]]
    received = list(
        readers.from_list(
            EXAMPLE_INT_SEQUENCE,
        ).separate_first()
    )
    received[1] = received[1].to_list()
    assert received == expected


def test_split_by_pos():
    pos_1, pos_2 = 3, 5
    expected_1 = EXAMPLE_INT_SEQUENCE[:pos_1], EXAMPLE_INT_SEQUENCE[pos_1:]
    a, b = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).split(
        pos_1,
    )
    received_1 = a.to_list(), b.to_list()
    assert received_1 == expected_1, 'test case 1'
    expected_2 = (
        [pos_1] + EXAMPLE_INT_SEQUENCE[:pos_1],
        [pos_2 - pos_1] + EXAMPLE_INT_SEQUENCE[pos_1:pos_2],
        [len(EXAMPLE_INT_SEQUENCE) - pos_2] + EXAMPLE_INT_SEQUENCE[pos_2:],
    )
    a, b, c = readers.from_list(
        EXAMPLE_INT_SEQUENCE,
    ).split(
        (pos_1, pos_2),
    )
    received_2 = a.count_to_items().to_list(), b.count_to_items().to_list(), c.count_to_items().to_list()
    assert received_2 == expected_2, 'test case 2'


def test_split_by_func():
    expected = [1, 3, 2, 4], [5, 7, 9, 6, 8]
    a, b = readers.from_list(
        EXAMPLE_INT_SEQUENCE
    ).split(
        lambda i: i >= 5,
    )
    received = a.to_list(), b.to_list()
    assert received == expected


def test_to_rows():
    expected = [['a', '1'], ['b', '2,22'], ['c', '3']]
    received = readers.from_list(
        EXAMPLE_CSV_ROWS,
    ).to_lines(
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
    test_add_records()
    test_separate_first()
    test_split_by_pos()
    test_split_by_func()
    test_to_rows()
