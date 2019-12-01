import pandas as pd


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


def get_main_key(histogram, key_field_no=0, value_field_no=1):
    main_key = max(
        histogram.items(),
        key=lambda a: a[value_field_no]
    )[key_field_no]
    return main_key


def agg_reducer(
        records,  # provided records must be from one key
        agg=AGG_EXAMPLE,  # aggregators config as list of tuples: (field_out, agg_method, field_in)
        skip_first_from_main=False,
        fillna=None,
):
    hist_measures = list()
    hist_dimensions = list()
    fields_checked = list()
    for field_out, agg_method, field_in in agg:
        if agg_method == 'main':  # extract main value (frequency of statistical mode) from histogram
            hist_measures.append(field_in)
        elif agg_method in ('dim', 'dimension', 'mode'):  # extract main key (statistical mode) from histogram
            hist_dimensions.append(field_in)
        elif agg_method not in AVAILABLE_AGG_METHODS:
            raise ValueError('Unknown aggregate method: {} (available: {})'.format(agg_method, AVAILABLE_AGG_METHODS))
        if field_out in fields_checked:
            raise ValueError('Output field name is repeated: {}'.format(field_out))
        else:
            fields_checked.append(field_out)
    build_histogram = bool(hist_measures)
    if build_histogram:
        assert hist_dimensions, 'dimensions must be specified when main-aggregator is used'
        first_hist_key = None
    record_out = dict()  # {field_name: field_value, }
    hist_dicts = [dict() for m in hist_measures]  # {dimensions: sum(main_measure), }
    records_count = 0
    for cur_record in records:
        records_count += 1
        if build_histogram:
            cur_hist_key = tuple([cur_record.get(d) for d in hist_dimensions])
            first_hist_key = first_hist_key or cur_hist_key
            for n, m in enumerate(hist_measures):
                if m is None:
                    hist_increment = 1
                else:
                    hist_increment = cur_record.get(m, 0)
                assert isinstance(
                    hist_increment, (int, float),
                ), 'unsupported measure type: {} (int of float needed), value={}'.format(
                    type(hist_increment), hist_increment,
                )
                hist_dicts[n][cur_hist_key] = hist_dicts[n].get(cur_hist_key, 0) + hist_increment
        for field_out, agg_method, field_in in agg:
            value_in = cur_record.get(field_in)
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
    if records_count > 0:
        if build_histogram:
            if skip_first_from_main and len(hist_dicts[0]) > 1:
                for n, m in enumerate(hist_measures):
                    hist_dicts[n][first_hist_key] = 0
            main_measure_no = 0
            main_hist_key = get_main_key(hist_dicts[main_measure_no])
        for field_out, agg_method, field_in in agg:
            if agg_method == 'avg':
                record_out[field_out] = record_out.get(field_out, 0) / records_count
            elif agg_method in ('count', 'cnt'):
                record_out[field_out] = records_count
            elif agg_method == 'main':
                field_no = hist_measures.index(field_in)
                record_out[field_out] = hist_dicts[field_no][main_hist_key]
            elif agg_method in ('dim', 'dimension', 'mode'):
                field_no = hist_dimensions.index(field_in)
                record_out[field_out] = main_hist_key[field_no]
            elif agg_method == 'const':
                record_out[field_out] = field_in
    return [record_out]


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
    prev_key = None
    for n, cur_record in enumerate(add_last_dummy_record(records, key)):
        cur_key = [cur_record.get(f) for f in key_fields]
        if prev_key is None:
            prev_key = cur_key
        if prev_key != cur_key:
            for record_out in reducer(records_group):
                yield record_out
            prev_key = cur_key
            records_group = list()
        records_group.append(cur_record)


def sorted_groupby_aggregate(records, key=DEFAULT_KEY_FIELDS, agg=AGG_EXAMPLE, skip_first_from_main=False):
    for cur_record in sorted_reduce(
        records,
        key=key,
        reducer=lambda r: agg_reducer(
            r,
            agg=agg,
            skip_first_from_main=skip_first_from_main,
        ),
    ):
        yield cur_record


def get_records_from_reader(reader, scheme=SCHEME, skip_first_row=True, max_n=None, expected_n=None, step_n=10000):
    records_count = max_n if (max_n or 0) > (expected_n or 0) else expected_n
    for n, row in enumerate(reader):
        if records_count > 0:
            if (n % step_n == 0) or (n >= records_count):
                print('{}% ({}/{}) lines processed'.format(100 * n / records_count, n, records_count), end='\r')
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
            [r[c] for c in columns]
        )
    return pd.DataFrame(rows, columns=columns)
