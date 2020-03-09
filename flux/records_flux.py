try:  # Assume we're a sub-module in a package.
    from . import fluxes as fx
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    import fluxes as fx


def is_record(item):
    return isinstance(item, dict)


def check_records(records, skip_errors=False):
    for r in records:
        if is_record(r):
            pass
        elif skip_errors:
            continue
        else:
            raise TypeError('check_records(): this item is not record: {}'.format(r))
        yield r


class RecordsFlux(fx.AnyFlux):
    def __init__(self, items, count=None, check=True):
        super().__init__(
            items=check_records(items) if check else items,
            count=count,
        )
        self.check = check

    def meta(self):
        return dict(
            count=self.count,
            check=self.check,
        )

    @staticmethod
    def is_valid_item(item):
        return is_record(item)

    @staticmethod
    def valid_items(items, skip_errors=False):
        return check_records(items, skip_errors)

    def set_count(self, count):
        return RecordsFlux(
            self.items,
            count=count,
            check=False,
        )

    def get_records(self, skip_errors=False, raise_errors=True):
        if skip_errors or raise_errors:
            return check_records(self.items, skip_errors)
        else:
            return self.items

    def to_lines(self, columns, add_title_row=False, delimiter='\t'):
        return fx.LinesFlux(
            self.to_rows(columns, add_title_row=add_title_row),
            self.count,
        ).map(
            delimiter.join,
        )

    def to_rows(self, columns, add_title_row=False):
        def get_rows(columns_list):
            if add_title_row:
                yield columns_list
            for r in self.items:
                yield [r.get(f) for f in columns_list]
        return fx.RowsFlux(
            get_rows(list(columns)),
            self.count + (1 if add_title_row else 0),
        )

    def schematize(self, schema, skip_errors=False):
        return fx.SchemaFlux(
            self.items,
            **self.meta()
        ).schematize(
            schema=schema,
            skip_errors=skip_errors,
        )

    def to_pairs(self, key_field, value_field=None):
        def get_pairs():
            for i in self.items:
                key = i.get(key_field)
                value = i if value_field is None else i.get(v)
                yield key, value
        return fx.PairsFlux(
            get_pairs(),
            count=self.count,
            secondary=fx.FluxType.RecordsFlux if value_field is None else fx.FluxType.AnyFlux,
        )

    def to_dict(self, key_field, value_field=None, of_lists=False):
        return self.to_pairs(
            key_field,
            value_field,
        ).to_dict(
            of_lists,
        )
