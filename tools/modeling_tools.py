import gc
import numpy as np


def split_train_and_test_by_month(
    dataframe, 
    month_field='date_m_no', fold_no=0,
):
    months = sorted(dataframe[month_field].unique())
    test_month = months[-1 - fold_no]
    df_train = dataframe[dataframe[month_field] < test_month]
    df_test = dataframe[dataframe[month_field] == test_month]
    return df_train, df_test


def split_X_and_y(
    dataframe, 
    target_field='cnt', feature_fields=None, leaky_fields=['target', 'cnt', 'revenue', 'is_non_zero'],
    output_feature_list=False,
):
    y = dataframe[target_field]
    if feature_fields:
        features = feature_fields
        X = dataframe[features]
        for f in leaky_fields:
            if f in features:
                X[f] = 0
    else:
        features = dataframe.columns.tolist()
        for f in leaky_fields:
            if f in features:
                features.remove(f)
        X = dataframe[features]
    X = X.values
    y = y.values
    if output_feature_list:
        return X, y, features
    else:
        return X, y


class JustRemember:
    def __init__(self, zero_ones_only=False, remember_last=False, remember_max=False, default_value=1):
        self.zero_ones_only = zero_ones_only
        self.remember_last = remember_last
        self.remember_max = remember_max
        self.default_value = default_value
        self.remembered = dict()
    
    def fit(self, X_train, y_train):
        zero_groups = dict()
        for features, target in zip(X_train, y_train):
            key = tuple(features)
            if self.zero_ones_only:
                value = (1 if target else 0)
            else:
                value = target
            if self.remember_last or (key not in self.remembered):
                self.remembered[key] = value
            elif self.remember_max:
                if value > self.remembered.get(key):
                    self.remembered[key] = value
        
    def predict(self, X_test):
        y_test = list()
        for features in X_test:
            predict = self.remembered.get(tuple(features), self.default_value)
            y_test.append(predict)
        return np.array(y_test)
