import gc
import numpy as np
import pandas as pd


TIME_FIELD = 'date_m_no'
MIN_TIME_VALUE, MONTH_LEN, YEAR_LEN, TEST_LEN = 0, 1, 12, 1
TIME_RANGE = [1, 2, 3, 6, 12]
REPRESENTATIVE_TIME_LEN = 2 * YEAR_LEN
TARGET = 'target'
MEASURES = [TARGET]
DIMENSIONS = ['key']
SENSIBLE_DIMENSION_COMBINATIONS = [DIMENSIONS]
KEY_FIELDS = [TIME_FIELD] + DIMENSIONS


def add_more_date_fields(dataframe, date_field='date'):
    dataframe[['date_d', 'date_m', 'date_y']] = dataframe[date_field].str.split('.', expand=True).astype('int')
    dataframe['date_ym_str'] = dataframe.date_y.map(str) + '-' + dataframe.date_m.map(lambda m: '{:02}'.format(m))
    dataframe['date_ymd_str'] = dataframe['date_ym_str'] + '-' + dataframe.date_d.map(lambda d: '{:02}'.format(d))
    dataframe['date_ymd_dt'] = pd.to_datetime(dataframe.date_ymd_str)
    dataframe['date_wd'] = dataframe.date_ymd_dt.dt.weekday
    dataframe['date_w'] = dataframe.date_ymd_dt.dt.week
    dataframe['date_w_no'] = (dataframe.date_y - 2013) * 53 + dataframe.date_w
    dataframe['date_ym_float'] = dataframe.date_y + dataframe.date_m/12
    dataframe['date_yw_float'] = dataframe.date_y + dataframe.date_w/53
    dataframe['date_ymd_float'] = dataframe.date_y + dataframe.date_m/12 + dataframe.date_d/372
    dataframe['date_m_no'] = (dataframe.date_y - 2013) * 12 + (dataframe.date_m - 1) * 1
    return dataframe


def add_agg_by_slices(
    dataframe, train_dataframe=None,
    measures=MEASURES, dimensions=DIMENSIONS, 
    by=DIMENSIONS[0], 
    sum_field='{}_sum_by_{}', share_field='{}_share_from_{}', mean_field='{}_mean_by_{}', size_field='size_by_{}',
    mean_only=False,
    group_size=None,
    fillna=0,
    verbose=True,
):
    TITLE = 'add_sums_by_slices(by={}):'.format(by)
    if train_dataframe is None:
        train_dataframe = dataframe
    if isinstance(by, str):
        by_set = {by}
        by_str = by
    elif isinstance(by, (set, list, tuple)):
        by_set = set(by)
        by_str = '_and_'.join(by)
    common_dimensions = list(set(dimensions) - by_set)
    if verbose:
        print(TITLE, 'Computing sums...', end='\r')

    sum_data = train_dataframe.groupby(
        common_dimensions,
        as_index=False
    ).agg(
        {m: 'sum' for m in measures}
    ).rename(
        columns={m: sum_field.format(m, by_str) for m in measures}
    )
    if verbose:
        print(TITLE, 'Merging...', end='\r')
    result = dataframe.merge(
        sum_data,
        on=common_dimensions,
        how='left', 
    )
    
    if group_size:  # months count, i.e.
        result[size_field.format(by_str)] = group_size
    else:
        if verbose:
            print(TITLE, 'Computing group sizes...', end='\r')
        size_data = train_dataframe[common_dimensions + ['cnt']].groupby(
            common_dimensions,
            as_index=False
        ).count().rename(
            columns={'cnt': size_field.format(by_str)}
        )
        result = result.merge(
            size_data,
            on=common_dimensions,
            how='left', 
        )
    if verbose:
        print(TITLE, 'Computing mean...', ' '*20, end='\r')
    for m in measures:
        sum_column = sum_field.format(m, by_str)
        mean_column = mean_field.format(m, by_str)
        share_column = share_field.format(m, by_str)
        size_column = size_field.format(by_str)
        result[mean_column] = result[sum_column] / result[size_column]
        if mean_only:
            result.drop(sum_column, 1, inplace=True)
    if mean_only:
        result.drop(size_column, 1, inplace=True)
    if fillna is not None:
        result = result.fillna(fillna)
    if verbose:
        print(TITLE, 'Done.', ' ' * 50)
    return result


def add_agg_features(
    dataframe, train_dataframe,
    measures=MEASURES, dimensions=DIMENSIONS, time_field=TIME_FIELD,
    dimension_combinations=SENSIBLE_DIMENSION_COMBINATIONS,
    take_last_times=REPRESENTATIVE_TIME_LEN,
    test_len=TEST_LEN,
    verbose=True,
):
    TITLE = 'add_agg_features():'
    max_time = dataframe[time_field].max() - test_len  # exclude test dates from counters
    min_time = max_time - take_last_times + 1
    last_time_dataframe = train_dataframe[
        (train_dataframe[time_field] >= min_time) &
        (train_dataframe[time_field] <= max_time)
    ]
    result = dataframe.copy()
    for sum_by in dimension_combinations:
        if verbose:
            print(TITLE, 'Adding sum by {}...'.format(sum_by), end='\r')
        result = add_agg_by_slices(
            result, last_time_dataframe, 
            measures, dimensions, 
            sum_by,
            group_size=take_last_times,
            mean_only=True,
            verbose=verbose,
        )
    if verbose:
        print(TITLE, 'Done.', ' ' * 50)
    return result


def add_lag_features(
    dataframe, train_dataframe,
    fields_for_lag=MEASURES, key_fields=KEY_FIELDS, time_field=TIME_FIELD,
    lag_range=TIME_RANGE, 
    discard_rows_witout_new_features=True,
    verbose=True,
):
    TITLE = 'add_lag_features():'
    for time_lag in lag_range:
        if verbose:
            print(TITLE, 'Processing time-lag {}...'.format(time_lag), end='\r')
        sales_shift = train_dataframe[key_fields + fields_for_lag].copy()
        sales_shift[time_field] = sales_shift[time_field] + time_lag
        rename_fields = lambda x: '{}_lag_{}'.format(x, time_lag) if x in fields_for_lag else x
        sales_shift = sales_shift.rename(columns=rename_fields)
        dataframe = dataframe.merge(sales_shift, on=key_fields, how='left').fillna(0)
        gc.collect();
    if discard_rows_witout_new_features:  # don't use old data without shifted dates
        dataframe = dataframe[dataframe[time_field] >= MIN_TIME_VALUE + max(lag_range)] 
    if verbose:
        print(TITLE, 'Done.', ' ' * 50)
        fit_cols = [col for col in dataframe.columns if col[-1] in [str(item) for item in lag_range]] 
        print(TITLE, 'fit_cols = ', fit_cols)  # list of all lagged features
        to_drop_cols = list(set(list(dataframe.columns)) - (set(fit_cols)|set(key_fields))) + [time_field] 
        print(TITLE, 'to_drop_cols = ', to_drop_cols)  # We will drop these at fitting stage
    gc.collect()
    return dataframe


def add_xox_features(
    dataframe,  # train or test
    lag_dataframe,  # lag_dataframe must include lag-features from add_lag_features()
    train_dataframe,  # train_dataframe must include target-field (cnt, i.e.)
    measure=TARGET, dimensions=KEY_FIELDS, time_field=TIME_FIELD, 
    lags=[('yoy', YEAR_LEN, TIME_RANGE[:3]), ('mom', MONTH_LEN, TIME_RANGE[:4])],  # (x_name, x_len, lag_range)
    na_value=None,  # YoY value in cases when item not exists in both lag- and train-dataframes
    inf_value=None,  # YoY value in cases when any new item just appeared (division by zero)
    discard_rows_witout_new_features=True,
    verbose=True,
):
    # For every month point adding YoYs (Year over Year), MoMs, etc. from previous months.
    # Using fields added in add_lag_features(), so it should run after that.
    TITLE = 'add_xox_features():'
    fit_cols = list()
    max_time_offset = 0
    cur_period_field = measure
    for x_name, x_len, lag_range in lags:
        prev_period_field = '{}_lag_{}'.format(cur_period_field, x_len)  # field produced in add_lag_features()
        x_base = lag_dataframe[dimensions + [prev_period_field]].merge(
            train_dataframe[dimensions + [cur_period_field]],
            on=dimensions,
            how='left',
        )
        cur_series = x_base[cur_period_field]
        prev_series = x_base[prev_period_field]
        x_base[x_name] = (cur_series - prev_series) / prev_series
        x_base = x_base.drop(
            columns=[cur_period_field, prev_period_field]
        )
        if na_value is not None:
            x_base = x_base.fillna(0)
        if inf_value is not None:
            x_base = x_base.replace([-np.inf, np.inf], [-inf_value, inf_value])
        for time_lag in lag_range:
            if verbose:
                print(TITLE, 'Processing {} for lag {}...'.format(x_name, time_lag), end='\r')
            lag_delta_field = '{}_{}_{}'.format(measure, x_name, time_lag)
            shifted_data = x_base.copy()
            shifted_data[time_field] = shifted_data[time_field] + time_lag
            shifted_data = shifted_data.rename(columns={x_name: lag_delta_field})
            dataframe = dataframe.merge(shifted_data, on=dimensions, how='left').fillna(0)
            max_time_offset = max(max_time_offset, x_len + time_lag)
            fit_cols.append(lag_delta_field)
            gc.collect();
    if discard_rows_witout_new_features:
        dataframe = dataframe[dataframe[time_field] >= MIN_TIME_VALUE + max_time_offset]
    to_drop_cols = list(set(list(dataframe.columns)) - (set(fit_cols)|set(dimensions))) + [time_field] 
    if verbose:
        print(TITLE, 'Done.', ' ' * 50)
        print(TITLE, 'fit_cols = ', fit_cols)  # list of all added features
        print(TITLE, 'to_drop_cols = ', to_drop_cols)  # we will drop these at fitting stage
    gc.collect()
    return dataframe


def limit_one_value(x, min_value=0, max_value=20):
    if x > max_value:
        return max_value
    elif x > min_value:
        return x
    else:
        return min_value


def limit_array_values(array, min_value=0, max_value=20):
    return np.array([limit_one_value(i, min_value, max_value) for i in array])
