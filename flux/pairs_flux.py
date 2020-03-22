try:  # Assume we're a sub-module in a package.
    from . import fluxes as fx
    from . import readers
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    import fluxes as fx
    import readers


def is_pair(row):
    if isinstance(row, (list, tuple)):
        return len(row) == 2


def check_pairs(pairs, skip_errors=False):
    for i in pairs:
        if is_pair(i):
            pass
        elif skip_errors:
            continue
        else:
            raise TypeError('check_pairs(): this item is not pair: {}'.format(i))
        yield i


class PairsFlux(fx.RowsFlux):
    def __init__(self, items, count=None, check=True, secondary=None):
        super().__init__(
            items=check_pairs(items) if check else items,
            count=count,
            check=check,
        )
        if secondary is None:
            self.secondary = fx.FluxType.AnyFlux
        else:
            assert secondary in fx.FluxType
            self.secondary = secondary or fx.FluxType.AnyFlux

    def meta(self):
        return dict(
            count=self.count,
            check=self.check,
            secondary=self.secondary,
        )

    def is_valid_item(self, item):
        return is_pair(
            item,
        )

    def valid_items(self, items, skip_errors=False):
        return check_pairs(
            items,
            skip_errors,
        )

    def secondary_type(self):
        return self.secondary

    def secondary_flux(self):
        return fx.get_class(
            self.secondary,
        ).__init__(
            self.items,
        )

    def memory_sort_by_key(self):
        sorted_items = sorted(
            self.convert_to_list().items,
            key=lambda p: p[0],
        )
        return PairsFlux(
            sorted_items,
            **self.meta()
        )

    def sorted_group_by_keys(self):
        def get_groups():
            accumulated = list()
            prev_k = None
            for k, v in self.items:
                if (k != prev_k) and accumulated:
                    yield prev_k, accumulated
                    accumulated = list()
                prev_k = k
                accumulated.append(v)
            yield k, accumulated
        return fx.PairsFlux(
            get_groups(),
        )

    def keys(self):
        my_keys = list()
        for k, v in self.items:
            if k in my_keys:
                pass
            else:
                my_keys.append(k)
        return my_keys

    def extract_keys_in_memory(self):
        flux_for_keys, flux_for_items = self.tee(2)
        return (
            flux_for_keys.keys(),
            flux_for_items,
        )

    def extract_keys_on_disk(self, tmp_file_template, encoding='utf8'):
        filename = tmp_file_template.format('json') if '{}' in tmp_file_template else tmp_file_template
        self.to_records().to_json().to_file(
            filename,
            encoding=encoding,
        )
        return (
            readers.from_file(filename, encoding).to_records().map(lambda r: r.get('key')),
            readers.from_file(filename, encoding).to_records().to_pairs('key', 'value'),
        )

    def extract_keys(self, tmp_file_template=None):
        if tmp_file_template is None:
            return self.extract_keys_in_memory()
        else:
            return self.extract_keys_on_disk(tmp_file_template)

    def to_dict(self, of_lists=False):
        result = dict()
        if of_lists:
            for k, v in self.items:
                distinct = result.get(k, [])
                if v not in distinct:
                    result[k] = distinct + [v]
        else:
            for k, v in self.items:
                result[k] = v
        return result

    def to_records(self, key_field='key', value_field='value', **kwargs):
        records = map(
            lambda i: {key_field: i[0], value_field: i[1]},
            self.items,
        )
        return fx.RecordsFlux(
            records,
            count=self.count,
        )
