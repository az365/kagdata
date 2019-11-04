import gc
from itertools import product
import numpy as np
import pandas as pd

try:  # Assume we're a sub-module in a package.
    from . import feature_engineering as fe
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    import feature_engineering as fe

TARGET = 'cnt'
MEASURES = [TARGET]  # also can be ['cnt', 'price, 'revenue']
TIME_FIELD = 'date_m_no'
MIN_TIME_VALUE, MONTH_LEN, YEAR_LEN, TEST_LEN = 0, 1, 12, 1
TIME_RANGE = [1, 2, 3, 6, 12]
CAT_DIMENSIONS = ['shop_id', 'cat_id', 'item_id']
ALL_DIMENSIONS = [TIME_FIELD] + CAT_DIMENSIONS
MIN_DIMENSIONS = ['shop_id', 'item_id']  # cat_id can be derived from item_id
KEY_FIELDS = [TIME_FIELD] + MIN_DIMENSIONS
SENSIBLE_DIMENSION_COMBINATIONS = [  # taking into account the hierarchy cat_id/item_id
    ('shop_id', ),
    ('cat_id', ),
    ('item_id', ),
    ('shop_id', 'cat_id'), 
    ('shop_id', 'item_id'), 
]


def add_cat_id(sales_dataframe, items_dataframe):
    if 'cat_id' in items_dataframe.columns:
        items_prepared = items_dataframe[['item_id', 'cat_id']]
    elif 'item_category_id' in items_dataframe.columns:
        items_prepared = items_dataframe[
            ['item_id', 'item_category_id']
        ].rename(
            columns={'item_category_id': 'cat_id'}
        )
    return sales_dataframe.merge(
        items_prepared,
        on='item_id',
        how='left',
    )


def add_initial_features_to_kaggle_test(dataframe, items_dataframe, time_value):
    result = add_cat_id(dataframe, items_dataframe)
    result[TIME_FIELD] = time_value
    return result


def all_shops_and_items_by_times(
    dataframe, 
    verbose=True,
):
    TITLE = 'all_shops_and_items():'
    grid = list()
    shops_list = sorted(dataframe['shop_id'].unique())
    items_list = sorted(dataframe['item_id'].unique())
    times_list = sorted(dataframe[TIME_FIELD].unique())
    for cur_time in times_list:
        appending_block = np.array(list(product(*[shops_list, items_list, [cur_time]])), dtype='int32')
        if verbose:
            print(TITLE, 'Adding month {}...'.format(cur_time), end='\r')
        grid.append(appending_block)
    grid = pd.DataFrame(np.vstack(grid), columns=['shop_id', 'item_id', TIME_FIELD], dtype=np.int32)
    if verbose:
        print(TITLE, 'Done.', 'appending_block.shape={}, grid.shape={}'.format(appending_block.shape, grid.shape))
    return grid


def extend_train_by_zero_items(dataframe, key_fields=KEY_FIELDS, verbose=True):
    grid = all_shops_and_items_by_times(dataframe, verbose=verbose)
    dataframe['is_non_zero'] = 1
    if verbose:
        TITLE = 'extend_train_by_zero_items():'
        print(TITLE, 'Merging grid...', end='\r')
    result = grid.merge(
        dataframe, 
        on=key_fields,
        how='left', 
    ).fillna(0) 
    del grid
    if verbose:
        print(TITLE, 'Done.', ' ' * 50, end='\r')
    gc.collect()
    return result


def apply_zero_ones_to_dataframe(dataframe, zero_ones_model, target_field=TARGET, key_fields=CAT_DIMENSIONS[:2]):
    # When pair (shop, category) is constantly zero, we can fill target all relevant rows by zeroes
    non_zero = zero_ones_model.predict(dataset[key_fields].values)
    result = dataframe.copy()
    result[target_field] = result[target_field] * non_zero
    return result


def apply_zero_ones(predictions, features, zero_ones_model):
    non_zero = zero_ones_model.predict(features)
    result = predictions * non_zero
    return result


def prepare_lgbm_features(dataframe, train_dataframe=None):
    if train_dataframe is None:
        train_dataframe = dataframe
    safe_fields = ALL_DIMENSIONS.copy()
    for cur_measure in MEASURES:
        if cur_measure in dataframe.columns:  # measures must be presented in train and can be absent in test
            safe_fields.append(cur_measure)
    result = dataframe[
        safe_fields
    ]
    result = fe.add_lag_features(
        result, train_dataframe, 
        MEASURES, KEY_FIELDS, TIME_FIELD,
        lag_range=TIME_RANGE,
    )
    result = fe.add_xox_features(
        result, result, train_dataframe, 
        TARGET, KEY_FIELDS, TIME_FIELD, 
        lags=[('yoy', YEAR_LEN, TIME_RANGE[:3]), ('mom', MONTH_LEN, TIME_RANGE[:4])], 
        na_value=0, inf_value=99,
    )
    result = fe.add_agg_features(
        result, train_dataframe, 
        MEASURES, CAT_DIMENSIONS, TIME_FIELD, 
        dimension_combinations=SENSIBLE_DIMENSION_COMBINATIONS, 
        take_last_times=2 * YEAR_LEN, test_len=TEST_LEN,
    )
    return result


def form_kaggle_submission(
    predicts, test_df, 
    try_no=None, filename_template='submission{:0>3d}.csv', 
    return_df=False, verbose=True
):
    TITLE = 'form_kaggle_submission():'
    assert len(predicts) == test_df.shape[0]
    result = pd.DataFrame(data={'ID': test_df['ID'], 'item_cnt_month': predicts})  # ID,item_cnt_month
    save_df = try_no and filename_template
    if save_df:
        filename = filename_template.format(try_no)
        if verbose:
            print(TITLE, 'Saving {}...'.format(filename), end='\r')
        result.to_csv(filename, index=False)
        if verbose:
            print(TITLE, 'File {} saved.'.format(filename))
    if return_df:
        return result
