import csv
import gzip
import datetime as dt
import pandas as pd


SCHEME = [  # (name, type, subtype)
    ('date', 'date', 'iso'),
    ('shop_id', 'id'),
    ('cat_id', 'cat'),
    ('item_id', 'id'),
    ('cnt', 'int', 'add'),  # additive
    ('price', 'float', 'rel'),  # relation
    ('revenue', 'float', 'add'),
]
NUM_TYPES = ('int', 'float', 'num', 'date')
DELIMITER = ','  # '\t'
SECONDARY_MEASURES = [
    ('price', lambda c, r: r / c, 'cnt', 'revenue'),
]


def get_csv_rows(filename, encoding, delimiter, gz=False):
    if gz:
        fileholder = gzip.open(filename, 'r')
    else:
        fileholder = open(filename, 'r', encoding=encoding)
    reader = csv.reader(fileholder, delimiter=delimiter)
    return fileholder, reader


def form_dataframe(fields_and_value_lists):
    columns = [i[0] for i in fields_and_value_lists]
    dict_values = {k: v for k, v in fields_and_value_lists}
    return pd.DataFrame(data=dict_values, columns=columns)


def get_counts(distincts):
    counts = list()
    for cur_distinct in distincts:
        counts += [len(cur_distinct)]
    return counts


def string_to_date(date_as_string, date_format):
    if date_format == 'iso':
        return dt.datetime.fromisoformat(date_as_string)
    elif date_format == 'gost':
        day, month, year = date_as_string.split('.')
        return dt.datetime(int(year), int(month), int(day))
    else:
        return dt.datetime.strptime(date_as_string, date_format)


def plus_one(key, histogram_as_dict):
    # for using as reducer_function in simple_reduce()
    histogram_as_dict[key] = histogram_as_dict.get(key, 0) + 1
    return histogram_as_dict


def first_one(key):
    # for using as init_function in simple_reduce()
    histogram_as_dict = {key: 1}
    return histogram_as_dict


def simple_reduce(current_value, accumulative_value, reducer_function, init_function=lambda a: a, args_as_list=True):
    if accumulative_value is None:
        return init_function(current_value)
    elif args_as_list:
        return reducer_function((current_value, accumulative_value))
    else:
        return reducer_function(current_value, accumulative_value)


def histogram_reduce(current_value, histogram_as_dict):
    return simple_reduce(
        current_value=current_value,
        accumulative_value=histogram_as_dict,
        reducer_function=plus_one,
        init_function=first_one,
        args_as_list=False,
    )


def columns_stats(
        filename, encoding='utf-8', delimiter=DELIMITER, gz=False,
        scheme=SCHEME,
        head=None,
        skip_first_row=False,
        skip_errors=True,
        output_errors=True,
        return_dataframe=False,
):
    TITLE = 'columns_stats()'
    parsed_rows_count = 0
    unparsed_rows_count = 0
    columns_count = len(scheme)
    column_numbers = range(columns_count)
    non_zero_counts = [0] * columns_count
    min_values = [None] * columns_count
    max_values = [None] * columns_count
    sum_values = [None] * columns_count
    histograms = [None] * columns_count

    fileholder, rows = get_csv_rows(filename, encoding, delimiter, gz)
    for n, row in enumerate(rows):
        if skip_first_row and n == 0:
            continue
        if head:
            if n >= head:
                break
        actual_row_len = len(row)
        if actual_row_len == columns_count:
            parsed_rows_count += 1
            for col_no, field_description, cur_value in zip(column_numbers, scheme, row):
                field_name, field_type = field_description[:2]
                field_subtype = field_description[2] if len(field_description) > 2 else None
                if field_type == 'date':
                    cur_value = string_to_date(cur_value, field_subtype)
                elif field_type == 'int':
                    cur_value = int(cur_value)
                elif field_type == 'float':
                    cur_value = float(cur_value)
                if cur_value:
                    non_zero_counts[col_no] += 1
                if field_type in NUM_TYPES:
                    min_values[col_no] = simple_reduce(cur_value, min_values[col_no], min)
                    max_values[col_no] = simple_reduce(cur_value, max_values[col_no], max)
                    if field_type != 'date':
                        sum_values[col_no] = simple_reduce(cur_value, sum_values[col_no], sum)
                elif field_type == 'cat':
                    histograms[col_no] = histogram_reduce(cur_value, histograms[col_no])
                elif field_type == 'id':
                    pass
                else:
                    error_message = 'Unsupported column type: {}'.format(field_type)
                    if not skip_errors:
                        raise ValueError(error_message)
                    if output_errors:
                        print(error_message)
        else:
            error_message = 'Incorrect columns count: {} instead of {} in line {}'.format(
                actual_row_len, columns_count, n
            )
            if skip_errors:
                unparsed_rows_count += 1
            else:
                raise ValueError(error_message)
            if output_errors:
                print(error_message, end='')
                line_begin = delimiter.join(row)
                if len(line_begin) > 40:
                    line_begin = line_begin[:40] + '...'
                print(':', line_begin)

    fileholder.close()

    distincts = [(f.keys() if isinstance(f, dict) else []) for f in histograms]
    counts = get_counts(distincts)
    avg_values = [i for i in map(lambda v: float(v or 0) / parsed_rows_count, sum_values)] if parsed_rows_count else []
    result = [
        ('field', [f[0] for f in scheme]),
        ('min_value', min_values),
        ('max_value', max_values),
        ('sum_value', sum_values),
        ('avg_value', avg_values),
        ('count', counts),
        ('distinct', distincts),
        ('histogram', histograms),
    ]
    if return_dataframe:
        if unparsed_rows_count:
            print(TITLE, ':', unparsed_rows_count, 'unparsed rows')
        return form_dataframe(result)
    else:
        return result, unparsed_rows_count
