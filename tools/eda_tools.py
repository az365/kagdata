import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import random
from itertools import product
import gc


def get_aggregate(data, dimensions, measures=['cnt', 'revenue'], aggregator='sum', relation_field='price', add_x=-1):
    result = data.groupby(
        dimensions, 
        as_index=False,
    ).agg(
        {f: aggregator for f in measures}
    ).sort_values(
        dimensions,
    )
    if relation_field:
        assert len(measures) >= 2
        result[relation_field] = result[measures[1]] / result[measures[0]]
    if add_x is not None:
        result['x'] = result[dimensions[add_x]]
    return result


def get_splited_aggregate(data, by, values=None):
    splitted_aggregate = list()
    if by:
        if not values:
            values = data[by].unique()
        for cur_value in values:
            splitted_aggregate.append(
                data[data[by] == cur_value]
            )
    else:  # if by is None
        splitted_aggregate.append(data)
    return splitted_aggregate


def get_vstack_dataset(datasets):
    stack = list()
    new_columns = ['dataset']
    for i in datasets:
        for c in i.columns:
            if c not in new_columns:
                new_columns.append(c)
    for no, dataset in enumerate(datasets):
        new_part = dataset.copy()
        new_part['dataset'] = no
        for c in new_columns:
            if c not in new_part.columns:
                new_part[c] = 0
        stack.append(
            new_part[new_columns]
        )
    return pd.DataFrame(np.vstack(stack), columns=new_columns)    


def get_unpivot(dataframe, fields_from=['cnt', 'revenue'], field_to='measure', value_to='value'):
    stack = list()
    new_columns = list()
    for cur_field in fields_from:
        new_part = dataframe.copy()
        new_part[field_to] = cur_field
        new_part[value_to] = new_part[cur_field]
        stack.append(new_part)
        if True:  # not new_columns:
            new_columns = new_part.columns
    return pd.DataFrame(np.vstack(stack), columns=new_columns)


def get_top_n_by(dataframe, field='cat_id', n=10, by='cnt'):
    if by:
        cat_sizes = dataframe.groupby(field).agg({by: 'sum'}).sort_values(by, ascending=False)
    else:
        cat_sizes = dataframe.groupby(field).size().sort_values(ascending=False)
    if (len(cat_sizes) > n):
        cat_sizes = cat_sizes[:n]
    return cat_sizes.index.tolist()


def convert_64_to_32(dataframe):
    float_columns = [c for c in dataframe if dataframe[c].dtype == "float64"]
    dataframe[float_columns] = dataframe[float_columns].astype(np.float32)
    int_columns = [c for c in dataframe if dataframe[c].dtype == "int64"]
    dataframe[int_columns] = dataframe[int_columns].astype(np.int32)
    return dataframe


def crop_value(x, min_value=0, max_value=20):
    if x > max_value:
        return max_value
    elif x > min_value:
        return x
    else:
        return min_value


def plot_single(
    data, x_field='x', y_field='y', 
    cat_field=None, cat_values=None,
    relative_y=False,
    stackplot=False,
    plot_legend=False, legend_location='best',
    plot=plt, 
):
    if relative_y:
        sum_y = data.groupby(x_field).agg({y_field: 'sum'})[y_field]
    if cat_field:
        if not cat_values:
            cat_values = data[cat_field].unique()
        for cur_cat_value in cat_values:
            filtred_data = data[data[cat_field] == cur_cat_value]
            x_values = filtred_data[x_field]
            y_values = filtred_data[y_field]
            if relative_y:
                y_values = y_values / sum_y
            if stackplot:
                plot.stackplot(
                    x_values.tolist(),
                    y_values.tolist(),
                )
            else:
                plot.plot(
                    x_values.tolist(),
                    y_values.tolist(),
                    label=cur_cat_value,
                )
        if plot_legend:
            plot.legend(loc=legend_location)  # loc: best, upper right, ...
    else:
        plot.plot(
            data[x_field].tolist(),
            data[y_field].tolist(),
        )


def plot_multiple(
    dataframe,
    x_range_field='shop_id', y_range_field='cat_id', x_range_values=None, y_range_values=None,
    x_axis_field='x', y_axis_field='cnt',
    cat_field=None, cat_values=None,
    stackplot=False,
    relative_y=False,
    max_cells_count=(16, 16),
    figsize=(15, 8),
    agg='sum',
    verbose=True,
):
    if agg:
        dimensions = {x_range_field, y_range_field, x_axis_field, cat_field} - {None}
        measures = {y_axis_field}
        data_agg = dataframe.groupby(list(dimensions), as_index=False).agg({f: agg for f in measures})
    else:
        data_agg = dataframe
    
    rows = get_splited_aggregate(data_agg, y_range_field, y_range_values)
    cols = get_splited_aggregate(rows[0], x_range_field, x_range_values)
    rows_count = len(rows)
    cols_count = len(cols)
    max_rows_count, max_cols_count = max_cells_count
    if rows_count > max_rows_count:
        rows_count = max_rows_count
    if cols_count > max_cols_count:
        cols_count = max_cols_count
    if verbose:
        print('Plotting rows: {} ({}), columns: {} ({})...'.format(y_range_field, rows_count, x_range_field, cols_count))
    fig, axis = plt.subplots(rows_count, cols_count, figsize=figsize)

    for row_no, row_data in enumerate(rows):
        cols = get_splited_aggregate(row_data, x_range_field, x_range_values)
        for col_no, col_data in enumerate(cols):
            if row_no < rows_count and col_no < cols_count:
                if cols_count > 1:
                    if rows_count > 1:
                        block = axis[row_no, col_no]
                    else:
                        block = axis[col_no]
                else:
                    block = axis[row_no]
                plot_single(
                    data=col_data,
                    x_field=x_axis_field,
                    y_field=y_axis_field,
                    cat_field=cat_field,
                    cat_values=cat_values,
                    relative_y=relative_y,
                    stackplot=stackplot,
                    plot=block,
                )
    if cat_field:
        block.legend(loc='best')


def plot_hist(series, log=False, bins=None, max_bins=75, default_bins=10, max_value=1e3):
    uniq_cnt = len(series.unique())
    if max_value is not None:
        filtered_series = series[series < max_value]
        filtred_cnt = len(filtered_series)
        print(uniq_cnt, '->', filtred_cnt)
        uniq_cnt = filtred_cnt
    else:
        print(uniq_cnt)
    filtered_series = series
    if bins:
        pass
    elif uniq_cnt < max_bins:
        bins = uniq_cnt
    else:
        bins = default_bins
    filtered_series.hist(log=log, bins=bins)


def get_tops(dataframe, fields=['shop', 'cat', 'item'], n=8, by='cnt'):
    result = list()
    for field in fields:
        id_field, name_field = '{}_id'.format(field), '{}_name'.format(field)
        top_ids = get_top_n_by(sales_d, field=id_field, n=n, by=by)
        result.append(top_ids.copy())
        cur_dict = DICT_IDS[field]
        print('Top {} {}s by {}:'.format(n, field, by))
        for i in top_ids:
            print('    {}: {}'.format(i, cur_dict[cur_dict[id_field] == i][name_field].values[0]))
    return result


def add_more_date_fields(dataframe, date_field='date'):
    dataframe[['date_d', 'date_m', 'date_y']] = dataframe[date_field].str.split('.', expand=True).astype('int')
    dataframe['date_ym_str'] = dataframe.date_y.map(str) + '-' + dataframe.date_m.map(lambda m: '{:02}'.format(m))
    dataframe['date_ymd_str'] = dataframe.date_y.map(str) + '-' + dataframe.date_m.map(lambda m: '{:02}'.format(m)) + '-' + dataframe.date_d.map(lambda d: '{:02}'.format(d))
    dataframe['date_ymd_dt'] = pd.to_datetime(dataframe.date_ymd_str)
    dataframe['date_wd'] = dataframe.date_ymd_dt.dt.weekday
    dataframe['date_w'] = dataframe.date_ymd_dt.dt.week
    dataframe['date_w_no'] = (dataframe.date_y - 2013) * 53 + dataframe.date_w
    dataframe['date_ym_float'] = dataframe.date_y + dataframe.date_m/12
    dataframe['date_yw_float'] = dataframe.date_y + dataframe.date_w/53
    dataframe['date_ymd_float'] = dataframe.date_y + dataframe.date_m/12 + sales_d.date_d/372
    dataframe['date_m_no'] = (dataframe.date_y - 2013) * 12 + (dataframe.date_m - 1) * 1
    return dataframe


def add_sum_data(data, by, dimensions, measures, sum_field='{}_sum_by_{}', share_field='{}_share_from_{}'):
    if isinstance(by, str):
        by_set = {by}
        by_str = by
    elif isinstance(by, (set, list, tuple)):
        by_set = set(by)
        by_str = '_and_'.join(by)
    common_dimensions = list(set(dimensions) - by_set)
    sum_data = data.groupby(
        common_dimensions,
        as_index=False
    ).agg(
        {m: 'sum' for m in measures}
    ).rename(
        columns={m: sum_field.format(m, by_str) for m in measures}
    )
    result = data.merge(
        sum_data,
        on=common_dimensions,
        how='left',
    )
    for m in measures:
        result[share_field.format(m, by_str)] = result[m] / result[sum_field.format(m, by_str)]
    return result
