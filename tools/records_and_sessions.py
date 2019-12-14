import pandas as pd

try:  # Assume we're a sub-module in a package.
    from . import process_csv as pcsv
    from . import eda_tools as eda
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    import process_csv as pcsv
    import eda_tools as eda


AVAILABLE_AGG_METHODS = (
    'first', 'last',
    'sum', 'avg', 'cnt', 'count',
    'main', 'mode', 'dim', 'dimension',
    'const',
)
AGG_EXAMPLE = (  # aggregation config with columns: (field_out, agg_method, field_in)
    ('user_id', 'first', 'user_id'),
    ('date', 'first', 'date'),
    ('fav_page_id', 'mode', 'page_id'),
    ('fav_time_spent', 'main', 'time_spent'),
    ('sum_time_spent', 'sum', 'time_spent'),
    ('avg_time_spent', 'avg', 'time_spent'),
    ('views_cnt', 'count', '*'),
)
DEFAULT_KEY_FIELDS = ('user_id', 'date')
SCHEME = (
    ('user_id', 'id'),
    ('date', 'date', 'iso'),
    ('page_id', 'cat'),
    ('time_spent', 'float'),
    ('clicks_cnt', 'int'),
)
TIME_FIELD = 'timestamp'
TIME_FORMAT = 'int'
EVENT_FIELD = 'event_no'
SESSION_FIELD = 'session_no'
SESSION_TIMEOUT = 30 * 60
SESSION_TIMEBOUND = 1 * 60 * 60
SESSION_CONFIG = {
    'time_field': TIME_FIELD,
    'time_format': TIME_FORMAT,
    'timeout': SESSION_TIMEOUT,
    'timebound': SESSION_TIMEBOUND,
    'session_field': SESSION_FIELD,
    'first_session_no': 1,
    'event_timeout_field': 'event_timeout',
}


def initialize_histograms(agg):
    measures = list()
    dimensions = list()
    fields_checked = list()
    histograms = None
    for field_out, agg_method, field_in in agg:
        if agg_method == 'main':  # extract main value (frequency of statistical mode) from histogram
            measures.append(field_in)
        elif agg_method in ('dim', 'dimension', 'mode'):  # extract main key (statistical mode) from histogram
            dimensions.append(field_in)
        elif agg_method not in AVAILABLE_AGG_METHODS:
            raise ValueError('Unknown aggregate method: {} (available: {})'.format(agg_method, AVAILABLE_AGG_METHODS))
        if field_out in fields_checked:
            raise ValueError('Output field name is repeated: {}'.format(field_out))
        else:
            fields_checked.append(field_out)
    build_histogram = bool(measures)
    if build_histogram:
        assert dimensions, 'dimensions must be specified when main-aggregator is used'
        histograms = [dict() for _ in measures]  # {dimensions: sum(main_measure), }
    first_key, main_key = None, None
    return [histograms, dimensions, measures, first_key, main_key]


def get_main_key_from_histogram(histogram, key_field_no=0, value_field_no=1):
    main_key = max(
        histogram.items(),
        key=lambda a: a[value_field_no]
    )[key_field_no]
    return main_key


def get_current_key_from_record(record, dimensions):
    return tuple(
        [record.get(d) for d in dimensions]
    )


def increment_histograms(record, histograms, dimensions, measures, inplace=True):
    if inplace:
        hist_dicts = histograms
    else:
        hist_dicts = histograms.copy()
    cur_hist_key = get_current_key_from_record(record, dimensions)
    for n, m in enumerate(measures):
        if m is None:
            cur_increment = 1
        else:
            cur_increment = record.get(m, 0)
        assert isinstance(
            cur_increment, (int, float),
        ), 'unsupported measure type for increment: {} (int of float needed), value={}'.format(
            type(cur_increment), cur_increment,
        )
        histograms[n][cur_hist_key] = histograms[n].get(cur_hist_key, 0) + cur_increment
    if not inplace:
        return hist_dicts, cur_hist_key


def update_aggregate(aggregated_record, source, field_description, fillna=None, inplace=True):
    # source can be exact value or record as dict
    field_out, agg_method, field_in = field_description
    record_out = aggregated_record if inplace else aggregated_record.copy()
    value_in = source.get(field_in) if isinstance(source, dict) else source
    if (value_in is None) and (agg_method not in ('cnt', 'count')):
        if fillna is not None:
            value_in = fillna
        else:
            raise ValueError('{} value is None and fillna option not used'.format(field_in))
    if agg_method == 'first':
        if record_out.get(field_out) is None:
            record_out[field_out] = value_in
    elif agg_method == 'last':
        record_out[field_out] = value_in
    elif agg_method in ('sum', 'avg'):
        record_out[field_out] = record_out.get(field_out, 0) + value_in
    if not inplace:
        return record_out


def postprocess_aggregate(aggregated_record, field_description, records_count, histogram_objects, inplace=True):
    record_out = aggregated_record if inplace else aggregated_record.copy()
    field_out, agg_method, field_in = field_description
    histograms, hist_dimensions, hist_measures, first_hist_key, main_hist_key = histogram_objects
    if agg_method == 'avg':
        record_out[field_out] = record_out.get(field_out, 0) / records_count
    elif agg_method in ('count', 'cnt'):
        record_out[field_out] = records_count
    elif agg_method == 'main':
        field_no = hist_measures.index(field_in)
        record_out[field_out] = histograms[field_no][main_hist_key]
    elif agg_method in ('dim', 'dimension', 'mode'):
        field_no = hist_dimensions.index(field_in)
        record_out[field_out] = main_hist_key[field_no]
    elif agg_method == 'const':
        record_out[field_out] = field_in
    if not inplace:
        return record_out


def add_bin_fields(records, fields_for_bins=('time_spent', 'clicks_cnt'), bounds=eda.DEFAULT_BOUNDS, fillna=0):
    for r in records:
        for field_in in fields_for_bins:
            field_bin = '{}_bin'.format(field_in)
            field_bound = '{}_bound'.format(field_in)
            value_in = r.get(field_in, fillna)
            r[field_bin], r[field_bound] = eda.get_bin_by_value(
                value_in,
                bounds,
                output_bound=True,
            )
        yield r


def extract_secondary_value(record, function, *input_fields):
    input_values = list()
    for cur_field in input_fields:
        input_values.append(
            record.get(cur_field)
        )
    return function(*input_values)


def add_secondary_fields(records, extractors):
    for r in records:
        for e in extractors:
            field_out, function = e[:2]
            fields_in = e[2:]
            r[field_out] = extract_secondary_value(r, function, *fields_in)
        yield r


def filter_records_by_function(records, function, *fields):
    for r in records:
        take_this = extract_secondary_value(r, function, *fields)
        if take_this:
            yield r


def filter_records_by_values(records, fields, list_values):
    for r in records:
        if isinstance(fields, str):
            value = r.get(fields)
        else:
            value = [r.get(f) for f in fields]
        take_this = value in list_values
        if take_this:
            yield r


def apply_dict(records, field_in, field_out, dict_to_apply, default_value=None):
    for r in records:
        value_in = r.get(field_in)
        value_out = dict_to_apply.get(value_in, default_value)
        if value_out:
            r[field_out] = value_out
            yield r


def sort_records(records, by):
    if isinstance(by, (set, list, tuple)):
        sorted_records = sorted(
            list(records),
            key=lambda r: [r.get(f) for f in by]
        )
    else:
        sorted_records = sorted(
            list(records),
            key=lambda r: r.get(by)
        )
    return sorted_records


def agg_reducer(records, agg=AGG_EXAMPLE, skip_first_from_main=False, fillna=None):
    histogram_objects = initialize_histograms(agg)
    histograms, hist_dimensions, hist_measures, first_hist_key, main_hist_key = histogram_objects
    record_out = dict()
    records_count = 0
    for cur_record in records:
        records_count += 1
        if histograms:
            increment_histograms(cur_record, histograms, hist_dimensions, hist_measures, inplace=True)
            first_hist_key = first_hist_key or get_current_key_from_record(cur_record, hist_dimensions)
        for field_description in agg:
            update_aggregate(record_out, cur_record, field_description, fillna, inplace=True)
    if records_count:
        if histograms:
            if skip_first_from_main and len(histograms[0]) > 1:
                for n, m in enumerate(hist_measures):
                    histograms[n][first_hist_key] = 0
            main_measure_no = 0
            main_object_no = 4
            main_hist_key = get_main_key_from_histogram(histograms[main_measure_no])
            histogram_objects[main_object_no] = main_hist_key
        for field_description in agg:
            postprocess_aggregate(record_out, field_description, records_count, histogram_objects)
    return [record_out]


def enumerate_sessions(
        records,
        time_field=TIME_FIELD, time_format=TIME_FORMAT,
        timeout=SESSION_TIMEOUT, timebound=SESSION_TIMEBOUND,
        session_field=SESSION_FIELD,
        first_session_no=1,
        event_field=EVENT_FIELD,
        first_event_no=1,
        event_timeout_field='event_timeout',
):
    start_time = None
    prev_time = None
    timebound_exceeded = False
    session_no = first_session_no
    event_no = first_event_no
    for record_in in records:
        record_out = record_in.copy()
        cur_time = record_in.get(time_field)
        if time_format not in ('int', int, 'float', float, 'timestamp', 'ts'):
            cur_time = pcsv.string_to_date(cur_time, time_format)
        if start_time is None:
            start_time = cur_time
        if prev_time is None:
            prev_time = cur_time
        cur_timelen = cur_time - start_time
        cur_timeout = cur_time - prev_time
        if cur_timelen > timebound:
            timebound_exceeded = True
        if cur_timeout >= timeout:
            session_no += 1
            event_no = first_event_no
            start_time = cur_time
            timebound_exceeded = False
        if not timebound_exceeded:
            record_out[session_field] = session_no
            record_out[event_field] = event_no
        if event_timeout_field:
            record_out[event_timeout_field] = cur_timeout
        event_no += 1
        prev_time = cur_time
        yield record_out


def add_last_dummy_record(records, key=DEFAULT_KEY_FIELDS, dummy_value=None):
    for record in records:
        yield record
    key_fields = [key] if isinstance(key, str) else key
    dummy_record = {k: dummy_value for k in key_fields}
    yield dummy_record


def sorted_reduce(records, key=DEFAULT_KEY_FIELDS, reducer=agg_reducer):
    # input records must be sorted by key
    key_fields = [key] if isinstance(key, str) else key
    records_group = list()
    is_first_record = True
    for n, cur_record in enumerate(add_last_dummy_record(records, key)):
        cur_key = [cur_record.get(f) for f in key_fields]
        if is_first_record:
            prev_key = cur_key
            is_first_record = False
        if prev_key != cur_key:
            for record_out in reducer(records_group):
                yield record_out
            prev_key = cur_key
            records_group = list()
        records_group.append(cur_record)


def reduce(records, key=DEFAULT_KEY_FIELDS, reducer=agg_reducer):
    sorted_records = sort_records(records, by=key)
    processed_records = sorted_reduce(sorted_records, key, reducer)
    return processed_records


def sorted_groupby_aggregate(records, key=DEFAULT_KEY_FIELDS, agg=AGG_EXAMPLE, skip_first_from_main=False):
    for cur_record in sorted_reduce(
        records,
        key=key,
        reducer=lambda r: agg_reducer(
            r,
            agg=agg,
            skip_first_from_main=skip_first_from_main,
            fillna=0,
        ),
    ):
        yield cur_record


def reduce_sessions(records, session_config=SESSION_CONFIG, agg_or_reducer=AGG_EXAMPLE):
    # input records must be from one user
    time_field = session_config.get('time_field', TIME_FIELD)
    session_field = session_config.get('session_field', SESSION_FIELD)
    sorted_records = sort_records(records, by=time_field)
    marked_records = enumerate_sessions(sorted_records, **session_config)
    if isinstance(agg_or_reducer, (set, list, tuple)):
        aggregation_config = agg_or_reducer
        sessions = sorted_groupby_aggregate(
            marked_records, key=session_field,
            agg=aggregation_config, skip_first_from_main=False,
        )
    else:
        process_session_reducer = agg_or_reducer
        sessions = sorted_reduce(
            marked_records, key=session_field,
            reducer=process_session_reducer,
        )
    return sessions


def get_composite_key(record, fields):
    if isinstance(fields, (str, int, float, bool)):
        list_fields = [fields]
    elif isinstance(fields, (set, list, tuple)):
        list_fields = fields
    else:
        raise ValueError('unsupported argument type: {} instead of str, list, ...'.format(type(fields)))
    list_values = [record.get(f) for f in list_fields]
    return tuple(list_values)


def map_side_join(left, right, by, filter_left_by_right=False):
    # by-argument can be one field or list of fields
    if isinstance(right, dict):
        dict_right = right
    else:
        dict_right = {get_composite_key(r, by): r for r in right}
    for r in left:
        key = get_composite_key(r, by)
        right_part = dict_right.get(key)
        if right_part:
            r.update(right_part)
        if right_part or not filter_left_by_right:
            yield r


def get_records_from_reader(reader, scheme=SCHEME, skip_first_row=True, max_n=None, expected_n=None, step_n=10000):
    records_count = max_n if (max_n or 0) > (expected_n or 0) else expected_n
    for n, row in enumerate(reader):
        if records_count:
            if (n % step_n == 0) or (n >= records_count):
                print('{}% ({}/{}) lines processed'.format(int(100 * n / records_count), n, records_count), end='\r')
            if max_n and n >= max_n:
                break
        if skip_first_row and n == 0:
            continue
        record = dict()
        for field_description, value in zip(scheme, row):
            field_name, field_type = field_description[:2]
            if field_type == 'int':
                value = int(value)
            elif field_type == 'float':
                value = float(value)

            record[field_name] = value
        yield record


def get_dataframe_from_records(records, columns=None):
    rows = list()
    for r in records:
        if not columns:
            columns = r.keys()
        rows.append(
            [r.get(c) for c in columns]
        )
    return pd.DataFrame(rows, columns=columns)
