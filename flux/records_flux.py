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


def process_selector_description(d):
    if callable(d[0]):
        function, inputs = d[0], d[1:]
    elif callable(d[-1]):
        inputs, function = d[:-1], d[-1]
    else:
        inputs, function = d, lambda *a: tuple(a)
    return function, inputs


def select_value(record, selector):
    if callable(selector):
        return selector(record)
    elif isinstance(selector, (list, tuple)):
        function, fields = process_selector_description(selector)
        values = [record.get(f) for f in fields]
        return function(*values)
    else:
        return record.get(selector)


def select_fields(rec_in, *fields, **selectors):
    rec_out = dict()
    for f in fields:
        if f == '*':
            rec_out.update(rec_in)
        elif isinstance(f, (list, tuple)) and len(f) > 1:
            rec_out[f[0]] = select_value(rec_in, f[1:])
        else:
            rec_out[f] = rec_in.get(f)
    for f, e in selectors.items():
        if callable(e):
            rec_out[f] = e(rec_in)
        elif isinstance(e, (list, tuple)):
            rec_out[f] = select_value(rec_in, e)
        else:
            rec_out[f] = rec_in.get(f)
    return rec_out


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

    def select(self, *fields, **selectors):
        return self.flat_map(
            lambda r: select_fields(r, *fields, **selectors),
        )

    def filter(self, *fields):
        def filter_function(r):
            for f in fields:
                if not select_value(r, f):
                    return False
            return True
        props = self.meta()
        props.pop('count')
        filtered_items = filter(filter_function, self.items)
        if self.is_in_memory():
            filtered_items = list(filtered_items)
            props['count'] = len(filtered_items)
        return self.__class__(
            filtered_items,
            **props
        )

    def group_by(self, key, step=None, as_pairs=True, verbose=True):
        sorted_fx = self.sort(
            key,
            step=step,
            verbose=True
        )
        if as_pairs:
            sorted_fx = sorted_fx.to_pairs(key)
        grouped_fx = sorted_fx.sorted_group_by_key()
        if as_pairs:
            return grouped_fx
        else:
            return grouped_fx.values()

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
                value = i if value_field is None else i.get(value_field)
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
