import gc
import numpy as np
import pandas as pd


TIME_FIELD = 'date_m_no'
MIN_TIME_VALUE, MONTH_LEN, YEAR_LEN, TEST_LEN = 0, 1, 12, 1
TIME_RANGE = [1, 2, 3, 6, 12]
REPRESENTATIVE_TIME_LEN = 2 * YEAR_LEN
MIN_YEAR = 2013
TARGET = 'target'
MEASURES = [TARGET]
DIMENSIONS = ['key']
SENSIBLE_DIMENSION_COMBINATIONS = [DIMENSIONS]
KEY_FIELDS = [TIME_FIELD] + DIMENSIONS


def add_more_date_fields(dataframe, date_field='date', iso_mode=False, min_year=MIN_YEAR):
    if iso_mode:
        dataframe[['date_y', 'date_m', 'date_d']] = dataframe[date_field].str.split('-', expand=True).astype('int')
    else:  # gost_mode
        dataframe[['date_d', 'date_m', 'date_y']] = dataframe[date_field].str.split('.', expand=True).astype('int')
    dataframe['date_ym_str'] = dataframe.date_y.map(str) + '-' + dataframe.date_m.map(lambda m: '{:02}'.format(m))
    dataframe['date_ymd_str'] = dataframe['date_ym_str'] + '-' + dataframe.date_d.map(lambda d: '{:02}'.format(d))
    dataframe['date_ymd_dt'] = pd.to_datetime(dataframe.date_ymd_str)
    dataframe['date_wd'] = dataframe.date_ymd_dt.dt.weekday
    dataframe['date_w'] = dataframe.date_ymd_dt.dt.week
    dataframe['date_w_no'] = (dataframe.date_y - min_year) * 53 + dataframe.date_w
    dataframe['date_ym_float'] = dataframe.date_y + dataframe.date_m/12
    dataframe['date_yw_float'] = dataframe.date_y + dataframe.date_w/53
    dataframe['date_ymd_float'] = dataframe.date_y + dataframe.date_m/12 + dataframe.date_d/372
    dataframe['date_m_no'] = (dataframe.date_y - min_year) * 12 + (dataframe.date_m - 1) * 1
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
    title = 'add_agg_by_slices(by={}):'.format(by)
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
        print(title, 'Computing sums...', end='\r')

    sum_data = train_dataframe.groupby(
        common_dimensions,
        as_index=False
    ).agg(
        {m: 'sum' for m in measures}
    ).rename(
        columns={m: sum_field.format(m, by_str) for m in measures}
    )
    if verbose:
        print(title, 'Merging...', end='\r')
    result = dataframe.merge(
        sum_data,
        on=common_dimensions,
        how='left', 
    )
    
    if group_size:  # months count, i.e.
        result[size_field.format(by_str)] = group_size
    else:
        if verbose:
            print(title, 'Computing group sizes...', end='\r')
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
        print(title, 'Computing mean...', ' '*20, end='\r')
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
        print(title, 'Done.', ' ' * 50)
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
        discard_rows_without_new_features=True,
        verbose=True,
):
    title = 'add_lag_features():'
    for time_lag in lag_range:
        if verbose:
            print(title, 'Processing time-lag {}...'.format(time_lag), end='\r')
        sales_shift = train_dataframe[key_fields + fields_for_lag].copy()
        sales_shift[time_field] = sales_shift[time_field] + time_lag
        rename_fields = lambda x: '{}_lag_{}'.format(x, time_lag) if x in fields_for_lag else x
        sales_shift = sales_shift.rename(columns=rename_fields)
        dataframe = dataframe.merge(sales_shift, on=key_fields, how='left').fillna(0)
        gc.collect();
    if discard_rows_without_new_features:  # don't use old data without shifted dates
        dataframe = dataframe[dataframe[time_field] >= MIN_TIME_VALUE + max(lag_range)] 
    if verbose:
        print(title, 'Done.', ' ' * 50)
        fit_cols = [col for col in dataframe.columns if col[-1] in [str(item) for item in lag_range]] 
        print(title, 'fit_cols = ', fit_cols)  # list of all lagged features
        to_drop_cols = list(set(list(dataframe.columns)) - (set(fit_cols)|set(key_fields))) + [time_field] 
        print(title, 'to_drop_cols = ', to_drop_cols)  # We will drop these at fitting stage
    gc.collect()
    return dataframe


def add_rolling_features(
        dataframe,
        train_dataframe,
        measures=MEASURES, dimensions=DIMENSIONS, time_field=TIME_FIELD,
        lag_range=TIME_RANGE, len_range=TIME_RANGE,
        field_name_template='{1}_lag{2}_{0}{3}',
        add_sums=True, add_means=True,
        discard_rows_without_new_features=True,
        verbose=True,
):
    # Generalization of add_lag_features() and add_agg_features()
    # Dimensions must not include time-field
    title = 'add_rolling_features():'
    sum_field = field_name_template.format('sum', '{0}', '{1}', '{2}')
    avg_field = field_name_template.format('avg', '{0}', '{1}', '{2}')
    result = dataframe.copy()
    target_times = dataframe[time_field].unique()
    available_times = train_dataframe[time_field].unique()
    min_target_time, max_target_time = target_times.min(), target_times.max()
    min_available_time, max_available_time = available_times.min(), available_times.max()
    for time_window_len in len_range:
        if verbose:
            print(title, 'Processing len {}...'.format(time_window_len), end='\r')
        grid = list()
        cropped_dataframe = train_dataframe[
            (train_dataframe[time_field] >= min_target_time - max(lag_range) - time_window_len + 1) &
            (train_dataframe[time_field] <= max_target_time - min(lag_range))
        ]
        cropped_dataframe = cropped_dataframe[measures + dimensions + [time_field]]

        for cur_time in available_times:
            if verbose:
                print(title, 'Processing len {}, time {}...'.format(time_window_len, cur_time), end='\r')
            filtered_dataframe = cropped_dataframe[
                (cropped_dataframe[time_field] >= cur_time - time_window_len + 1) &
                (cropped_dataframe[time_field] <= cur_time)
            ]
            sum_dataframe = filtered_dataframe.groupby(
                dimensions,
                as_index=False,
            ).agg(
                {m: 'sum' for m in measures}
            ).rename(
                columns={m: sum_field.format(m, 0, time_window_len) for m in measures}
            )
            sum_dataframe[time_field] = cur_time
            grid.append(sum_dataframe.copy())
            gc.collect()
        grid = pd.DataFrame(np.vstack(grid), columns=sum_dataframe.columns, dtype=np.int32)
        cropped_dataframe = cropped_dataframe.merge(
            grid,
            on=dimensions + [time_field],
            how='left',
        )
        gc.collect()

        for time_lag in lag_range:
            if verbose:
                print(title, 'Shifting lag {}, len {}...'.format(time_lag, time_window_len), end='\r')
            shifted_dataframe = cropped_dataframe.drop(columns=measures)
            shifted_dataframe[time_field] = shifted_dataframe[time_field] + time_lag
            rename_fields = {
                sum_field.format(m, 0, time_window_len):
                sum_field.format(m, time_lag, time_window_len) 
                for m in measures
            }
            shifted_dataframe = shifted_dataframe.rename(columns=rename_fields)
            if verbose:
                print(title, 'Merging lag {}, len {}...'.format(time_lag, time_window_len), ' ' * 30, end='\r')
            result = result.merge(
                shifted_dataframe, 
                on=dimensions + [time_field], 
                how='left'
            ).fillna(0)
            gc.collect()

    for m in measures:
        for time_window_len in len_range:
            for time_lag in lag_range:
                triple = (m, time_lag, time_window_len)
                if add_means:
                    result[avg_field.format(*triple)] = result[sum_field.format(*triple)] / time_window_len
                if not add_sums:
                    result.drop(columns=sum_field.format(*triple))

    if discard_rows_without_new_features:  # and without incomplete sums
        result = result[
            (result[time_field] >= min_available_time + max(lag_range) + max(len_range) - 1) &
            (result[time_field] <= max_available_time + min(lag_range))
        ]
    if verbose:
        print(title, 'Done.', ' ' * 50)
    gc.collect()
    return result


def add_xox_features(
        dataframe,  # train or test
        lag_dataframe,  # lag_dataframe must include lag-features from add_lag_features()
        train_dataframe,  # train_dataframe must include target-field (cnt, i.e.)
        measure=TARGET, dimensions=KEY_FIELDS, time_field=TIME_FIELD,
        lags=[('yoy', YEAR_LEN, TIME_RANGE[:3]), ('mom', MONTH_LEN, TIME_RANGE[:4])],  # (x_name, x_len, lag_range)
        na_value=None,  # YoY value in cases when item not exists in both lag- and train-dataframes
        inf_value=None,  # YoY value in cases when any new item just appeared (division by zero)
        discard_rows_without_new_features=True,
        verbose=True,
):
    # For every month point adding YoYs (Year over Year), MoMs, etc. from previous months.
    # Using fields added in add_lag_features(), so it should run after that.
    title = 'add_xox_features():'
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
                print(title, 'Processing {} for lag {}...'.format(x_name, time_lag), end='\r')
            lag_delta_field = '{}_{}_{}'.format(measure, x_name, time_lag)
            shifted_data = x_base.copy()
            shifted_data[time_field] = shifted_data[time_field] + time_lag
            shifted_data = shifted_data.rename(columns={x_name: lag_delta_field})
            dataframe = dataframe.merge(shifted_data, on=dimensions, how='left').fillna(0)
            max_time_offset = max(max_time_offset, x_len + time_lag)
            fit_cols.append(lag_delta_field)
            gc.collect();
    if discard_rows_without_new_features:
        dataframe = dataframe[dataframe[time_field] >= MIN_TIME_VALUE + max_time_offset]
    to_drop_cols = list(set(list(dataframe.columns)) - (set(fit_cols)|set(dimensions))) + [time_field] 
    if verbose:
        print(title, 'Done.', ' ' * 50)
        print(title, 'fit_cols = ', fit_cols)  # list of all added features
        print(title, 'to_drop_cols = ', to_drop_cols)  # we will drop these at fitting stage
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
