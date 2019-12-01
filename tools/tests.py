import pandas as pd

try:  # Assume we're a sub-module in a package.
    from . import feature_engineering as fe
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    import feature_engineering as fe


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
    test_add_rolling_features()
