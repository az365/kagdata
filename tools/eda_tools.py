from enum import Enum
import pandas as pd
import numpy as np
from matplotlib import (
    pyplot as plt,
    patches as mp,
)


DEFAULT_BOUNDS = (0, 1, 10, 100, 1000, 10000, 100000)


def get_aggregate(data, dimensions, measures=('cnt', 'revenue'), aggregator='sum', relation_field='price', add_x=-1):
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


def get_split_aggregate(data, by, values=None):
    split_aggregate = list()
    if by:
        if not values:
            values = data[by].unique()
        for cur_value in values:
            split_aggregate.append(
                data[data[by] == cur_value]
            )
    else:  # if by is None
        split_aggregate.append(data)
    return split_aggregate


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


def get_unpivot(dataframe, fields_from=('cnt', 'revenue'), field_to='measure', value_to='value'):
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
    if len(cat_sizes) > n:
        cat_sizes = cat_sizes[:n]
    return cat_sizes.index.tolist()


def get_tops(dataframe, fields=('shop', 'cat', 'item'), n=8, by='cnt', dict_ids={}, verbose=True):
    result = list()
    for field in fields:
        id_field, name_field = '{}_id'.format(field), '{}_name'.format(field)
        top_ids = get_top_n_by(dataframe, field=id_field, n=n, by=by)
        result.append(top_ids.copy())
        cur_dict = dict_ids[field]
        if verbose:
            print('Top {} {}s by {}:'.format(n, field, by))
            for i in top_ids:
                print('    {}: {}'.format(i, cur_dict[cur_dict[id_field] == i][name_field].values[0]))
    return result


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


def get_bin_by_value(value, bounds=DEFAULT_BOUNDS, bin_format='{:03}: {}', output_bound=False):
    bounds_cnt = len(bounds)
    assert bounds_cnt, 'bounds must be non-empty and sorted'
    bin_no = bounds_cnt + 1
    for cur_no, cur_value in enumerate(bounds):
        if value < cur_value:
            bin_no = cur_no
            break
    if bin_format or output_bound:
        if bin_no == 0:
            left_bound, right_bound = None, bounds[0]
        elif bin_no > bounds_cnt:
            left_bound, right_bound = bounds[-1], None
        else:
            left_bound, right_bound = bounds[bin_no - 1], bounds[bin_no]
    if bin_format:
        if left_bound is None:
            interval_name = '{}-'.format(right_bound)
        elif right_bound is None:
            interval_name = '{}+'.format(left_bound)
        else:
            interval_name = '{}..{}'.format(left_bound, right_bound)
        result = bin_format.format(bin_no, interval_name)
    else:
        result = bin_no
    if output_bound:
        result = result, left_bound
    return result


def meld_other(dataframe, cat_field, cat_values, minor_value='other', save_ones=False, sort_by_cat=True):
    actual_cats = dataframe[cat_field].unique()
    major_cats = [c for c in cat_values if c in actual_cats]
    minor_cats = [c for c in actual_cats if c not in major_cats]
    result = dataframe.copy()
    result[cat_field] = result[cat_field].apply(
        lambda c: c if c in major_cats else minor_value
    )
    if save_ones:  # preserve numbers of major cats for plot colors
        for c in minor_cats:
            result.append(
                dataframe[dataframe[cat_field] == c].head(1)
            )
    if sort_by_cat:
        result.sort_values(by=cat_field, inplace=True)
    return result


def get_brief_caption(value, max_len=10):
    if isinstance(value, (int, float)):
        if value > 10:
            value = int(value)
        else:
            value = round(3)
        if value > 10000000:
            value = '{}M'.format(round(value / 1000000, 1))
        elif value > 10000:
            value = '{}k'.format(round(value / 1000, 1))
    elif isinstance(value, str):
        if len(value) > max_len:
            value = value[:max_len - 1] + '_'
    return value


def get_cum_sum_for_stackplot(dataframe, x_field, y_field, cat_field, reverse_cat=True):
    data = dataframe.copy()
    x_values = data[x_field].unique()
    cat_values = data[cat_field].unique()
    for cur_x in x_values:
        cum_sum = 0.0
        for cur_cat in sorted(cat_values, reverse=reverse_cat):
            cur_mask = (
                (data[x_field] == cur_x) &
                (data[cat_field] == cur_cat)
            )
            cur_row = data[cur_mask]
            has_row = bool(cur_row.shape[0])
            if has_row:
                cur_index = cur_row.index[0]
                cur_value = cur_row[y_field].values[0]
                cum_sum = cum_sum + cur_value
                data.loc[cur_index, y_field] = cum_sum
    return data


def get_subplot_title(dataframe, x_range_field, y_range_field, title):
    if title:
        title_blocks = list()
        if title != 'auto':
            title_blocks.append(title)
        if y_range_field:
            cur_value = dataframe[y_range_field].array[0]
            title_blocks.append('{}={}'.format(y_range_field, cur_value))
        if x_range_field:
            cur_value = dataframe[x_range_field].array[0]
            title_blocks.append('{}={}'.format(x_range_field, cur_value))
        return ', '.join(title_blocks)


def process_lim(limit, series):
    if isinstance(limit, str):
        max_value = max(series)
        if limit[-1:] == '%':
            limit = max_value * float(limit[:-1]) / 100
        elif limit == 'max':
            limit = max_value
    if isinstance(limit, (int, float)):
        limit = (0, limit)
    return limit


class PlotType(Enum):
    line = 'line'
    plot = 'line'
    log = 'loglog'
    loglog = 'loglog'
    stackplot = 'stackplot'
    stack = 'stackplot'
    bar = 'bar'


def plot_series(x_values, y_values, plot=plt, plot_type=PlotType.line, **plot_kws):
    plot_xy = (
        list(x_values),
        list(y_values),
    )
    if plot_type == PlotType.line:
        plot.plot(*plot_xy, **plot_kws)
    elif plot_type == PlotType.loglog:
        plot.loglog(*plot_xy, **plot_kws)
    elif plot_type == PlotType.stackplot:
        plot.stackplot(*plot_xy, **plot_kws)
    elif plot_type == PlotType.bar:
        plot.bar(*plot_xy, **plot_kws)
    else:
        raise ValueError('Unsupported plot type: {}'.format(plot_type))


def plot_captions(plot, x_values, y_values, y_captions, y_offset_rate=40, y_min_size=25):
    max_y = max(y_values)
    offset_y = max_y / y_offset_rate
    for c, x, y in zip(y_captions, x_values, y_values):
        if isinstance(c, str):
            plot_caption = True
        else:
            plot_caption = (c > max_y / y_min_size)
        if plot_caption:
            c = get_brief_caption(c)
            plot.annotate(c, xy=(x, y - offset_y))


def plot_single(
        dataframe, x_field='x', y_field='y',
        relative_y=False, caption_field=None,
        cat_field=None, cat_values=None, cat_colors=None,
        plot_type=PlotType.line,
        plot_legend=False, legend_location='best',
        bbox_to_anchor=None,
        ylim=None,
        title=None,
        plot=plt,
):
    graph_kws = dict(plot=plot, plot_type=plot_type)
    if cat_field:
        if not cat_values:
            cat_values = dataframe[cat_field].unique()
        if relative_y:
            sum_y = dataframe.groupby(x_field).agg({y_field: 'sum'})[y_field]
        if plot_type in (PlotType.stackplot, PlotType.bar):
            data = dataframe.copy()
            if caption_field == y_field:
                caption_field = '{}_'.format(y_field)
                data[caption_field] = data[y_field]
            data = get_cum_sum_for_stackplot(data, x_field, y_field, cat_field, reverse_cat=True)
        else:
            data = dataframe
        for cur_cat_value in cat_values:
            filtered_data = data[data[cat_field] == cur_cat_value]
            x_values = filtered_data[x_field]
            y_values = filtered_data[y_field]
            if relative_y:
                y_values = y_values / sum_y
            if plot_type in (PlotType.line, PlotType.bar):
                graph_kws['label'] = cur_cat_value
            if cat_colors:
                color = cat_colors.get(cur_cat_value)
                if color:
                    graph_kws['color'] = color
            plot_series(x_values, y_values, **graph_kws)
            if caption_field:
                y_captions = filtered_data[caption_field]
                plot_captions(plot, x_values, y_values, y_captions)
        if plot_legend:
            if plot_type == PlotType.stackplot:
                if cat_colors:
                    rectangles = [mp.Rectangle((0, 0), 1, 1, fc=cat_colors.get(c)) for c in cat_values]
                    plot.legend(rectangles, cat_values, loc=legend_location, bbox_to_anchor=bbox_to_anchor)
            else:
                plot.legend(loc=legend_location, bbox_to_anchor=bbox_to_anchor)  # loc: best, upper right, ...
    else:
        x_values = dataframe[x_field]
        y_values = dataframe[y_field]
        plot_series(x_values, y_values, **graph_kws)
        if caption_field:
            y_captions = dataframe[caption_field]
            plot_captions(plot, x_values, y_values, y_captions)
    if ylim:
        plot.ylim(*ylim)
    if title:
        plot.title.set_text(title)


def plot_multiple(
        dataframe,
        x_range_field='shop_id', y_range_field='cat_id', x_range_values=None, y_range_values=None,
        x_axis_field='x', y_axis_field='cnt',
        cat_field=None, cat_values=None, cat_colors=None,
        y_caption_field=None,
        plot_type=PlotType.line,
        relative_y=False,
        xlim='max', ylim='max',
        max_cells_count=(16, 16),
        figsize=(15, 8),
        agg='sum',
        plot_legend=True,
        title='auto',
        verbose=True,
):
    if agg:
        dimensions = {x_range_field, y_range_field, x_axis_field, cat_field} - {None}
        measures = {y_axis_field}
        data_agg = dataframe.groupby(list(dimensions), as_index=False).agg({f: agg for f in measures})
    else:
        data_agg = dataframe
    assert data_agg.shape[0] > 0, 'dataframe must be non-empty'
    rows = get_split_aggregate(data_agg, y_range_field, y_range_values)
    cols = get_split_aggregate(rows[0], x_range_field, x_range_values)
    rows_count = len(rows)
    cols_count = len(cols)
    max_rows_count, max_cols_count = max_cells_count
    if rows_count > max_rows_count:
        rows_count = max_rows_count
    if cols_count > max_cols_count:
        cols_count = max_cols_count
    if verbose:
        print(
            'Plotting rows: {} ({}), columns: {} ({})...'.format(y_range_field, rows_count, x_range_field, cols_count),
            end='\r',
        )
    subplot_kw = dict()
    x_is_numeric = isinstance(list(data_agg[x_axis_field])[0], (int, float))
    if x_is_numeric:
        subplot_kw['xlim'] = process_lim(xlim, data_agg[x_axis_field])
    subplot_kw['ylim'] = process_lim(ylim, data_agg[y_axis_field])
    fig, axis = plt.subplots(rows_count, cols_count, figsize=figsize, subplot_kw=subplot_kw)

    for row_no, row_data in enumerate(rows):
        cols = get_split_aggregate(row_data, x_range_field, x_range_values)
        for col_no, subplot_data in enumerate(cols):
            if row_no < rows_count and col_no < cols_count:
                if cols_count > 1:
                    block = axis[row_no, col_no] if rows_count > 1 else axis[col_no]
                else:
                    block = axis[row_no] if rows_count > 1 else axis
                is_last_block = row_no == len(rows) - 1 and col_no == len(cols) - 1
                plot_legend_here = plot_legend and is_last_block
                subtitle = get_subplot_title(subplot_data, x_range_field, y_range_field, title)
                plot_single(
                    dataframe=subplot_data,
                    x_field=x_axis_field,
                    y_field=y_axis_field,
                    cat_field=cat_field,
                    cat_values=cat_values,
                    cat_colors=cat_colors,
                    caption_field=y_caption_field,
                    relative_y=relative_y,
                    plot_type=plot_type,
                    plot_legend=plot_legend_here,
                    title=subtitle,
                    plot=block,
                )
    if verbose:
        if cols_count > 1:
            x_range_values = x_range_values or data_agg[x_range_field].unique()
            print('{} {} in columns: {}'.format(
                cols_count, x_range_field, ', '.join([str(v) for v in x_range_values])
            ), ' ' * 25)
        if rows_count > 1:
            y_range_values = y_range_values or data_agg[y_range_field].unique()
            print('{} {} in rows: {}'.format(
                rows_count, y_range_field, ', '.join([str(v) for v in y_range_values])
            ))


def plot_hist(series, log=False, bins=None, max_bins=75, default_bins=10, max_value=1e3):
    uniq_cnt = len(series.unique())
    if max_value is not None:
        filtered_series = series[series <= max_value]
        filtered_cnt = len(filtered_series)
        print(uniq_cnt, '->', filtered_cnt)
        uniq_cnt = filtered_cnt
    else:
        filtered_series = series
        print(uniq_cnt)
    if bins:
        pass
    elif uniq_cnt < max_bins:
        bins = uniq_cnt
    else:
        bins = default_bins
    filtered_series.hist(log=log, bins=bins)


def add_more_date_fields(dataframe, date_field='date', iso_mode=False, min_year=2013):
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
