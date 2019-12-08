import pandas as pd
import datetime as dt

try:  # Assume we're a sub-module in a package.
    from . import eda_tools as eda
    from . import feature_engineering as fe
    from . import records_and_sessions as rs
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    import eda_tools as eda
    import feature_engineering as fe
    import records_and_sessions as rs


AGG_SESSION_FROM_VIEWS = (  # aggregation config (field_out, agg_method, field_in),
    ('user_id', 'first', 'user_id'),
    ('date', 'first', 'date'),
    ('entry_point', 'first', 'page_id'),
    ('finish_point', 'last', 'page_id'),
    ('fav_page_id', 'mode', 'page_id'),
    ('fav_service_id', 'dim', 'service_id'),
    ('fav_page_time_spent', 'main', 'time_spent'),
    ('fav_page_clicks_cnt', 'main', 'clicks_cnt'),
    ('views_cnt', 'count', '*'),
    ('time_spent', 'sum', 'time_spent'),
    ('clicks_cnt', 'sum', 'clicks_cnt'),
    ('avg_clicks_per_view', 'avg', 'clicks_cnt'),
)
RECS_EXAMPLE_VIEWS = (
    {'user_id': 123, 'date': '2019-02-01', 'service_id': 32, 'page_id': 1, 'clicks_cnt': 5, 'time_spent': 5.5},
    {'user_id': 123, 'date': '2019-02-01', 'service_id': 32, 'page_id': 1, 'clicks_cnt': 3, 'time_spent': 4.5},
    {'user_id': 123, 'date': '2019-02-01', 'service_id': 32, 'page_id': 2, 'clicks_cnt': 1, 'time_spent': 0.5},
    {'user_id': 123, 'date': '2019-02-02', 'service_id': 64, 'page_id': 3, 'clicks_cnt': 7, 'time_spent': 100},
)
EXPECTED_SESSIONS_FROM_VIEWS = (
    {
        'user_id': 123, 'date': '2019-02-01',
        'entry_point': 1, 'finish_point': 2,
        'fav_page_id': 1, 'fav_service_id': 32,
        'fav_page_time_spent': 10.0, 'fav_page_clicks_cnt': 8,
        'views_cnt': 3, 'time_spent': 10.5, 'clicks_cnt': 9,
        'avg_clicks_per_view': 3.0,
    },
    {
        'user_id': 123, 'date': '2019-02-02',
        'entry_point': 3, 'finish_point': 3,
        'fav_page_id': 3, 'fav_service_id': 64,
        'fav_page_time_spent': 100, 'fav_page_clicks_cnt': 7,
        'views_cnt': 1, 'time_spent': 100, 'clicks_cnt': 7,
        'avg_clicks_per_view': 7.0,
    },
)
AGG_USER_FROM_SESSIONS = (
    ('user_id', 'first', 'user_id'),
    ('first_date', 'first', 'date'),
    ('first_service_id', 'first', 'fav_service_id'),
    ('first_page_id', 'first', 'fav_page_id'),
    ('first_page_time_spent', 'first', 'fav_time_spent'),
    ('fav_service_id', 'dim', 'fav_service_id'),
    ('fav_page_id', 'dimension', 'fav_page_id'),
    ('max_page_time_spent', 'main', 'fav_time_spent'),
    ('time_spent', 'sum', 'time_spent'),
    ('sessions_cnt', 'count', 'session'),
)
RECS_EXAMPLE_SESSIONS = (
    {'user_id': 3, 'date': '2019-02-01', 'fav_service_id': 2, 'fav_page_id': 9, 'fav_time_spent': 5, 'time_spent': 10},
    {'user_id': 3, 'date': '2019-02-02', 'fav_service_id': 2, 'fav_page_id': 8, 'fav_time_spent': 6, 'time_spent': 10},
    {'user_id': 3, 'date': '2019-02-02', 'fav_service_id': 2, 'fav_page_id': 8, 'fav_time_spent': 6, 'time_spent': 10},
    {'user_id': 3, 'date': '2019-02-03', 'fav_service_id': 2, 'fav_page_id': 9, 'fav_time_spent': 7, 'time_spent': 10},
    {'user_id': 3, 'date': '2019-02-03', 'fav_service_id': 2, 'fav_page_id': 9, 'fav_time_spent': 7, 'time_spent': 10},
)
EXPECTED_USER_FROM_SESSIONS = (
    {
        'user_id': 3,
        'first_date': '2019-02-01',
        'first_service_id': 2,
        'first_page_id': 9,
        'first_page_time_spent': 5,
        'fav_service_id': 2,
        'fav_page_id': 8,
        'max_page_time_spent': 12,
        'time_spent': 50,
        'sessions_cnt': 5,
    },
)


def test_agg_reducer():
    expected = EXPECTED_SESSIONS_FROM_VIEWS[0:1]
    received = rs.agg_reducer(
        RECS_EXAMPLE_VIEWS[0:3],
        agg=AGG_SESSION_FROM_VIEWS,
        skip_first_from_main=False,
    )
    assert list(received) == list(expected), 'test case {}, received {} instead of {}'.format(0, received, expected)

    expected = EXPECTED_SESSIONS_FROM_VIEWS[1:2]
    received = rs.agg_reducer(
        RECS_EXAMPLE_VIEWS[3:4],
        agg=AGG_SESSION_FROM_VIEWS,
        skip_first_from_main=False,
    )
    assert list(received) == list(expected), 'test case {}, received {} instead of {}'.format(1, received, expected)

    expected = EXPECTED_USER_FROM_SESSIONS
    received = rs.agg_reducer(
        RECS_EXAMPLE_SESSIONS[:],
        agg=AGG_USER_FROM_SESSIONS,
        skip_first_from_main=True,
    )
    assert list(received) == list(expected), 'test case {}, received {} instead of {}'.format(2, received, expected)


def test_sorted_reduce():
    expected = EXPECTED_SESSIONS_FROM_VIEWS
    received = rs.sorted_reduce(
        RECS_EXAMPLE_VIEWS,
        key=['user_id', 'date'],
        reducer=lambda r: rs.agg_reducer(
            r,
            agg=AGG_SESSION_FROM_VIEWS,
            skip_first_from_main=False,
        ),
    )
    assert list(received) == list(expected), 'test case {}, received {} instead of {}'.format(0, received, expected)

    expected = EXPECTED_USER_FROM_SESSIONS
    received = rs.sorted_reduce(
        RECS_EXAMPLE_SESSIONS,
        key='user_id',
        reducer=lambda r: rs.agg_reducer(
            r,
            agg=AGG_USER_FROM_SESSIONS,
            skip_first_from_main=True,
        ),
    )
    assert list(received) == list(expected), 'test case {}, received {} instead of {}'.format(1, received, expected)


def test_sorted_groupby_aggregate():
    expected = EXPECTED_SESSIONS_FROM_VIEWS
    received = rs.sorted_groupby_aggregate(
        RECS_EXAMPLE_VIEWS,
        key=['user_id', 'date'],
        agg=AGG_SESSION_FROM_VIEWS,
        skip_first_from_main=False,
    )
    assert list(received) == list(expected), 'test case {}, received {} instead of {}'.format(0, received, expected)

    expected = EXPECTED_USER_FROM_SESSIONS
    received = rs.sorted_groupby_aggregate(
        RECS_EXAMPLE_SESSIONS,
        key='user_id',
        agg=AGG_USER_FROM_SESSIONS,
        skip_first_from_main=True,
    )
    assert list(received) == list(expected), 'test case {}, received {} instead of {}'.format(1, received, expected)


def test_enumerate_sessions():
    events = [
        {'user_id': 999, 'timestamp': 1500000000, 'dt': '2017-07-14 02:40:00', 'expected_s_no': 1},
        {'user_id': 999, 'timestamp': 1500000600, 'dt': '2017-07-14 02:50:00', 'expected_s_no': 1},
        {'user_id': 999, 'timestamp': 1500001200, 'dt': '2017-07-14 03:00:00', 'expected_s_no': 1},
        {'user_id': 999, 'timestamp': 1500003600, 'dt': '2017-07-14 03:40:00', 'expected_s_no': 2},
        {'user_id': 999, 'timestamp': 1500004200, 'dt': '2017-07-14 03:50:00', 'expected_s_no': 2},
        {'user_id': 999, 'timestamp': 1500005400, 'dt': '2017-07-14 04:10:00', 'expected_s_no': 2},
        {'user_id': 999, 'timestamp': 1500006600, 'dt': '2017-07-14 04:30:00', 'expected_s_no': 2},
        {'user_id': 999, 'timestamp': 1500007800, 'dt': '2017-07-14 04:50:00', 'expected_s_no': None},
        {'user_id': 999, 'timestamp': 1500010200, 'dt': '2017-07-14 05:30:00', 'expected_s_no': 3},
    ]
    result = list(
        rs.enumerate_sessions(
            events,
            time_field='timestamp', time_format='int',
            timeout=30 * 60, timebound=1 * 60 * 60,
            session_field='received_s_no',
        )
    )
    expected = [i.get('expected_s_no') for i in events]
    received = [i.get('received_s_no') for i in result]
    assert list(received) == list(expected), 'test case {}, received {} instead of {}'.format(0, received, expected)

    result = list(
        rs.enumerate_sessions(
            events,
            time_field='dt', time_format='iso',
            timeout=dt.timedelta(minutes=30), timebound=dt.timedelta(hours=1),
            session_field='received_s_no',
        )
    )
    expected = [i.get('expected_s_no') for i in events]
    received = [i.get('received_s_no') for i in result]
    assert list(received) == list(expected), 'test case {}, received {} instead of {}'.format(1, received, expected)


def test_reduce_session():
    # events = [
    #     {'user_id': 999, 'timestamp': 1500000000, 'dt': '2017-07-14 02:40:00', 'expected_s_no': 1},
    #     {'user_id': 999, 'timestamp': 1500000600, 'dt': '2017-07-14 02:50:00', 'expected_s_no': 1},
    #     {'user_id': 999, 'timestamp': 1500001200, 'dt': '2017-07-14 03:00:00', 'expected_s_no': 1},
    #     {'user_id': 999, 'timestamp': 1500003600, 'dt': '2017-07-14 03:40:00', 'expected_s_no': 2},
    #     {'user_id': 999, 'timestamp': 1500004200, 'dt': '2017-07-14 03:50:00', 'expected_s_no': 2},
    #     {'user_id': 999, 'timestamp': 1500005400, 'dt': '2017-07-14 04:10:00', 'expected_s_no': 2},
    #     {'user_id': 999, 'timestamp': 1500006600, 'dt': '2017-07-14 04:30:00', 'expected_s_no': 2},
    #     {'user_id': 999, 'timestamp': 1500007800, 'dt': '2017-07-14 04:50:00', 'expected_s_no': None},
    #     {'user_id': 999, 'timestamp': 1500010200, 'dt': '2017-07-14 05:30:00', 'expected_s_no': 3},
    # ]
    # received = rs.reduce_sessions(events, SESSION_CONFIG, AGG_SESSION_FROM_EVENTS)
    session_config_for_views = {
        'time_field': 'date',
        'time_format': 'iso',
        'timeout': dt.timedelta(days=1),
        'timebound': dt.timedelta(days=1),
        'session_field': 'session_no',
        'first_session_no': 1,
        'event_timeout_field': 'event_timeout',
    }
    received = list(rs.reduce_sessions(RECS_EXAMPLE_VIEWS, session_config_for_views, AGG_SESSION_FROM_VIEWS))
    expected = list(EXPECTED_SESSIONS_FROM_VIEWS)
    assert received == expected, 'test case {}, received {} instead of {}'.format(0, received, expected)


def test_get_bin_by_value():
    list_bounds = (0, 100, 1000, 10000, 100000)
    values_and_bounds = (
        (-5, '000: 0-', None),
        (50, '001: 0..100', 0),
        (500, '002: 100..1000', 100),
        (50000, '004: 10000..100000', 10000),
        (100500, '006: 100000+', 100000),
    )
    for n, (value, interval_name, bound) in enumerate(values_and_bounds):
        received = eda.get_bin_by_value(value, list_bounds, '{:03}: {}', True)
        expected = (interval_name, bound)
        assert received == expected, 'test case {}, received {} instead of {}'.format(n, received, expected)


def test_add_bin_fields():
    list_bounds = (0, 5, 10, 50, 100)
    fields_to_bin = ('clicks_cnt', 'time_spent')
    processed_records = rs.add_bin_fields(RECS_EXAMPLE_VIEWS, fields_to_bin, list_bounds)
    received = [
        [r.get('{}_bound'.format(f)) for f in fields_to_bin]
        for r in processed_records
    ]
    expected = [[5, 5], [0, 0], [0, 0], [5, 100]]
    assert received == expected, 'test case {}, received {} instead of {}'.format(0, received, expected)


def test_add_secondary_fields():
    secondary_measures = [
        ('SpU', lambda s, u: s / u, 'sessions', 'users'),
        ('RpS', lambda r, s: r / s, 'requests', 'sessions'),
        ('rows', lambda: 1),
        ('lines', lambda a: 1, '*'),
    ]
    test_records = [
        {'requests': 10, 'sessions': 5, 'users': 2},
        {'requests': 30, 'sessions': 12, 'users': 3},
    ]
    received = list(rs.add_secondary_fields(test_records, secondary_measures))
    expected = [
        {'requests': 10, 'sessions': 5, 'users': 2, 'SpU': 2.5, 'RpS': 2.0, 'rows': 1, 'lines': 1},
        {'requests': 30, 'sessions': 12, 'users': 3, 'SpU': 4.0, 'RpS': 2.5, 'rows': 1, 'lines': 1},
    ]
    assert received == expected, 'test case {}, received {} instead of {}'.format(0, received, expected)


def get_rising_synth_sample(date_cnt=5, shop_cnt=2, item_cnt=3, item2cat={0: 1, 1: 1, 2: 2}, k=10):
    def get_rising_synth_data(date_cnt, shop_cnt, item_cnt, item2cat, k):
        for cur_date in range(date_cnt):
            for cur_shop in range(shop_cnt):
                for cur_item in range(item_cnt):
                    cnt = (cur_shop * item_cnt + item_cnt + 1) * k + cur_date
                    cur_cat = item2cat.get(cur_item, 0)
                    yield (cur_date, cur_shop, cur_cat, cur_item, cnt)
    return pd.DataFrame(
        get_rising_synth_data(date_cnt, shop_cnt, item_cnt, item2cat, k),
        columns=['date_m_no', 'shop_id', 'cat_id', 'item_id', 'cnt']
    )


def test_add_rolling_features(verbose=True):
    synth_df = get_rising_synth_sample(34, k=100)
    rolled_synth_train_df = fe.add_rolling_features(
        synth_df, synth_df,
        ['cnt'], ['shop_id', 'cat_id', 'item_id'], 'date_m_no',
        discard_rows_without_new_features=True,
        verbose=verbose,
    )
    rolled_synth_test_df = fe.add_rolling_features(
        synth_df[synth_df.date_m_no == synth_df.date_m_no.max()], synth_df,
        ['cnt'], ['shop_id', 'cat_id', 'item_id'], 'date_m_no',
        discard_rows_without_new_features=True,
        verbose=verbose,
    )
    assert rolled_synth_train_df.date_m_no.min(), rolled_synth_train_df.date_m_no.max() == (23, 33)
    assert list(rolled_synth_test_df.date_m_no.unique()) == [33]


if __name__ == '__main__':
    test_agg_reducer()
    test_sorted_groupby_aggregate()
    test_sorted_reduce()
    test_enumerate_sessions()
    test_reduce_session()
    test_get_bin_by_value()
    test_add_bin_fields()
    test_add_rolling_features()
