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


def sort_by_dependencies(selectors):
    ordered_fields = list()
    for field, description in selectors.items():
        f_pos = 0
        if not callable(description):
            _, dependencies = fx.process_selector_description(description)
            for d in dependencies:
                if d in ordered_fields:
                    d_pos = ordered_fields.index(d)
                    if d_pos >= f_pos:
                        f_pos = d_pos + 1
        ordered_fields.insert(f_pos, field)
    print('ordered_fields:', ordered_fields)
    return [(f, selectors[f]) for f in ordered_fields]


def select_value(record, description):
    if callable(description):
        return description(record)
    elif isinstance(description, (list, tuple)):
        function, fields = fx.process_selector_description(description)
        values = [record.get(f) for f in fields]
        return function(*values)
    else:
        return record.get(description)


def select_fields(rec_in, *descriptions):
    record = rec_in.copy()
    fields_out = list()
    for desc in descriptions:
        if desc == '*':
            fields_out.append(rec_in.keys())
        elif isinstance(desc, (list, tuple)):
            if len(desc) > 1:
                f_out = desc[0]
                fs_in = desc[1] if len(desc) == 2 else desc[1:]
                record[f_out] = select_value(record, fs_in)
                fields_out.append(f_out)
            else:
                raise ValueError('incorrect selector: {}'.format(desc))
        else:
            if desc not in record:
                record[desc] = None
            fields_out.append(desc)
    return {f: record[f] for f in fields_out}


def get_key_function(descriptions):
    if len(descriptions) == 0:
        raise ValueError('key must be defined')
    elif len(descriptions) == 1:
        key_function = lambda r: select_value(r, descriptions[0])
    else:
        key_function = lambda r: tuple([select_value(r, d) for d in descriptions])
    return key_function


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

    def get_records(self, skip_errors=False, raise_errors=True):
        if skip_errors or raise_errors:
            return check_records(self.items, skip_errors)
        else:
            return self.items

    def enumerated_records(self, field='#', first=1):
        for n, r in enumerate(self.items):
            r[field] = n + first
            yield r

    def enumerate(self, native=False):
        props = self.meta()
        if native:
            target_class = self.__class__
            enumerated = self.enumerated_records()
        else:
            target_class = fx.PairsFlux
            enumerated = self.enumerated_items()
            props['secondary'] = fx.FluxType(self.class_name())
        return target_class(
            items=enumerated,
            **props
        )

    def select(self, *fields, **selectors):
        descriptions = list(fields)
        for k, v in sort_by_dependencies(selectors):
            if isinstance(v, (list, tuple)):
                descriptions.append([k] + list(v)),
            else:
                descriptions.append([k] + [v])
        print('select(descriptions=', descriptions, ')')
        return self.native_map(
            lambda r: select_fields(r, *descriptions),
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

    def sort(
            self,
            *keys,
            reverse=False,
            step=fx.MAX_ITEMS_IN_MEMORY, tmp_file_template='merge_sort_{}', encoding='utf8',
            verbose=True,
    ):
        key_function = get_key_function(keys)
        if self.is_in_memory() or (step is None) or (self.count is not None and self.count <= step):
            return self.memory_sort(key_function, reverse)
        else:
            return self.disk_sort(key_function, reverse, step, tmp_file_template, encoding, verbose)

    def group_by(self, *keys, step=None, as_pairs=True, verbose=True):
        sorted_fx = self.sort(
            *keys,
            step=step,
            verbose=verbose,
        )
        if as_pairs:
            sorted_fx = sorted_fx.to_pairs(
                key=get_key_function(keys),
            )
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

    def to_pairs(self, key, value=None):
        def get_pairs():
            for i in self.items:
                k = select_value(i, key)
                v = i if value is None else select_value(i, value)
                yield k, v
        return fx.PairsFlux(
            list(get_pairs()) if self.is_in_memory() else get_pairs(),
            count=self.count,
            secondary=fx.FluxType.RecordsFlux if value is None else fx.FluxType.AnyFlux,
        )

    def to_dict(self, key_field, value_field=None, of_lists=False):
        return self.to_pairs(
            key_field,
            value_field,
        ).to_dict(
            of_lists,
        )
