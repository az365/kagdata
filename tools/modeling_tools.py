import gc
import numpy as np


def split_train_and_test_by_time(dataframe, time_field, fold_no=0, test_len=1):
    times = sorted(dataframe[time_field].unique())
    test_time = times[-1 - fold_no * test_len]
    train = dataframe[dataframe[time_field] < test_time]
    test = dataframe[(dataframe[time_field] >= test_time) & (dataframe[time_field] < test_time + test_len)]
    return train, test


def split_X_and_y(
        dataframe,
        target_field='cnt',
        feature_fields=None,
        leaky_fields=('target', 'cnt', 'revenue', 'is_non_zero'),
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
    def __init__(
            self,
            zero_ones_only=False, remember_last=False, remember_max=False, default_value=1,
    ):
        self.zero_ones_only = zero_ones_only
        self.remember_last = remember_last
        self.remember_max = remember_max
        self.default_value = default_value
        self.remembered = dict()
    
    def fit(self, X_train, y_train, verbose=True):
        title = 'JustRemember.fit():'
        count = len(y_train)
        for row_no, (features, target) in enumerate(zip(X_train, y_train)):
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
            if verbose:
                if row_no % 10000 == 0:
                    print(
                        title + 'Processing {}% ({} / {})...'.format(int(100 * row_no / count), row_no, count),
                        end='\r',
                    )
        if verbose:
            print(title, 'Done.', ' ' * 50)
        gc.collect()

    def predict(self, X_test, verbose=True):
        title = 'JustRemember.predict():'
        y_test = list()
        count = len(X_test)
        for row_no, features in enumerate(X_test):
            predict = self.remembered.get(tuple(features), self.default_value)
            y_test.append(predict)
            if verbose:
                if row_no % 10000 == 0:
                    print(
                        title, 'Processing {}% ({} / {})...'.format(int(100 * row_no / count), row_no, count),
                        end='\r',
                    )
        if verbose:
            print(title, 'Done.', ' ' * 50)
        gc.collect()
        return np.array(y_test)
