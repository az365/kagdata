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


def read_several_files(prefix, suffix, batch_names, file_template, delimiter=None, skip_first_line=False, verbose=True):
    for batch_name in batch_names:
        filename = file_template.format(prefix, batch_name, suffix)
        if verbose:
            print('Reading file:', filename)
        fileholder = open(filename, 'r')
        is_first_row = True
        reader = csv.reader(fileholder, delimiter=delimiter) if delimiter else csv.reader(fileholder)
        for row in reader:
            if skip_first_line and is_first_row:
                is_first_row = False
                continue
            yield row
        fileholder.close()


def form_dataframe(fields_and_value_lists):
    columns = [i[0] for i in fields_and_value_lists]
    dict_values = {k: v for k, v in fields_and_value_lists}
    return pd.DataFrame(data=dict_values, columns=columns)


def get_counts(distincts, by_items=False):
    counts = list()
    if by_items:
        for distincts_by_items in distincts:
            counts_by_items = dict()
            if distincts_by_items:
                for cur_item, cur_distinct in distincts_by_items.items():
                    counts_by_items[cur_item] = len(cur_distinct)
            counts += [counts_by_items.copy()]
    else:
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
    result = histogram_as_dict.copy()
    result[key] = result.get(key, 0) + 1
    return result


def first_one(key):
    # for using as init_function in simple_reduce()
    histogram_as_dict = {key: 1}
    return histogram_as_dict


def simple_reduce(
        current_value, accumulative_value,
        reducer_function, init_function=lambda a: a,
        args_as_list=True,
):
    if accumulative_value is None:
        return init_function(current_value)
    elif args_as_list:
        return reducer_function((current_value, accumulative_value))
    else:
        return reducer_function(current_value, accumulative_value)


def item_reduce(
        current_value, dict_accumulative_values,
        item,
        reducer_function, init_function=lambda a: a,
        args_as_list=True,
):
    accumulative_value_for_item = dict_accumulative_values.get(
        item,
        dict_accumulative_values.get(None)
    )
    result = dict_accumulative_values.copy()
    if accumulative_value_for_item is None:
        result[item] = init_function(current_value)
    elif args_as_list:
        result[item] = reducer_function((current_value, accumulative_value_for_item))
    else:
        result[item] = reducer_function(current_value, accumulative_value_for_item)
    return result


def histogram_reduce(current_value, histogram_as_dict, item=None):
    if item:
        return item_reduce(
            current_value=current_value,
            dict_accumulative_values=histogram_as_dict,
            item=item,
            reducer_function=plus_one,
            init_function=first_one,
            args_as_list=False
        )
    else:
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
        verbose=True,
        expected_lines_cnt=0,
        return_dataframe=False,
):
    title = 'columns_stats()'
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
        if verbose and ((n % 10000 == 0) or (expected_lines_cnt and (n >= expected_lines_cnt - 1))):
            if expected_lines_cnt:
                print(
                    '{}% ({}/{}) lines processed'.format(int(100 * n / expected_lines_cnt), n, expected_lines_cnt),
                    end='\r',
                )
            else:
                print('{} lines processed'.format(n), end='\r')
    fileholder.close()

    distincts = [(f.keys() if isinstance(f, dict) else []) for f in histograms]
    counts = get_counts(distincts)
    avg_values = [i for i in map(lambda v: float(v or 0) / parsed_rows_count, sum_values)] if parsed_rows_count else []
    result = [
        ('field', [f[0] for f in scheme]),
        ('non_zero_count', non_zero_counts),
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
            print(title, ':', unparsed_rows_count, 'unparsed rows')
        return form_dataframe(result)
    else:
        return result, unparsed_rows_count


def items_stats(
        filename, encoding='utf-8', delimiter=DELIMITER, gz=False,
        scheme=SCHEME,
        key='item_id',
        head=None,
        skip_first_row=False,
        skip_errors=True,
        output_errors=True,
        verbose=True,
        expected_lines_cnt=0,
        return_dataframe=False,
):
    title = 'items_stats()'
    parsed_items = list()
    parsed_rows_count = 0
    unparsed_rows_count = 0
    columns_count = len(scheme)
    column_numbers = range(columns_count)
    column_names = [f[0] for f in scheme]
    if isinstance(key, int):
        key_col_no = key
    else:
        key_col_no = column_names.index(key)
    non_zero_counts = [{None: 0}] * columns_count
    min_values = [dict() for i in range(columns_count)]
    max_values = [dict() for i in range(columns_count)]
    sum_values = [dict() for i in range(columns_count)]
    avg_values = [dict() for i in range(columns_count)]
    histograms = [dict() for i in range(columns_count)]

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
            item = row[key_col_no]
            if item not in parsed_items:
                parsed_items.append(item)
            for col_no, field_description, cur_value in zip(column_numbers, scheme, row):
                field_name, field_type = field_description[:2]
                field_subtype = field_description[2] if len(field_description) > 2 else None
                if field_name != key:
                    if field_type == 'date':
                        cur_value = string_to_date(cur_value, field_subtype)
                    elif field_type == 'int':
                        cur_value = int(cur_value)
                    elif field_type == 'float':
                        cur_value = float(cur_value)
                    if cur_value:
                        non_zero_counts[col_no] = item_reduce(1, non_zero_counts[col_no], item, sum)
                    if field_type in NUM_TYPES:
                        min_values[col_no] = item_reduce(cur_value, min_values[col_no], item, min)
                        max_values[col_no] = item_reduce(cur_value, max_values[col_no], item, max)
                        if field_type != 'date':
                            sum_values[col_no] = item_reduce(cur_value, sum_values[col_no], item, sum)
                    elif field_type == 'cat':
                        histograms[col_no] = histogram_reduce(cur_value, histograms[col_no], item=item)
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
                if len(line_begin) > 140:
                    line_begin = line_begin[:140] + '...'
                print(':', line_begin)
        if verbose and ((n % 10000 == 0) or (expected_lines_cnt and (n >= expected_lines_cnt - 1))):
            if expected_lines_cnt:
                print(
                    '{}% ({}/{}) lines processed'.format(int(100 * n / expected_lines_cnt), n, expected_lines_cnt),
                    end='\r',
                )
            else:
                print('{} lines processed'.format(n), end='\r')
    fileholder.close()

    distincts = list()
    for histograms_by_items in histograms:
        distincts_by_items = dict()
        for cur_item, cur_histogram in histograms_by_items.items():
            cur_distinct = cur_histogram.keys()
            distincts_by_items[cur_item] = [v for v in cur_distinct]
        distincts.append(distincts_by_items)
    counts = get_counts(distincts, by_items=True)
    for col_no in range(columns_count):
        if sum_values[col_no]:
            for item in sum_values[col_no].keys():
                avg_values[col_no][item] = float(sum_values[col_no][item] or 0) / parsed_rows_count

    print('parsed_items:', parsed_items)
    aggregates = [
        ('non_zero_count', non_zero_counts),
        ('min_value', min_values),
        ('max_value', max_values),
        ('sum_value', sum_values),
        ('avg_value', avg_values),
        ('count', counts),
        ('distinct', distincts),
        ('histogram', histograms),
    ]
    title_row = ['item', 'field'] + [a[0] for a in aggregates]
    result = [title_row]
    for item in parsed_items:
        for col_no in range(columns_count):
            field = scheme[col_no][0]
            out_row = [item, field]
            for agg_name, agg_values in aggregates:
                cell_value = agg_values[col_no].get(item)
                out_row.append(cell_value)
            result.append(out_row.copy())
    if return_dataframe:
        if unparsed_rows_count:
            print(title, ':', unparsed_rows_count, 'unparsed rows')
        return pd.DataFrame.from_records(result[1:], columns=result[0])
    else:
        return result, unparsed_rows_count
