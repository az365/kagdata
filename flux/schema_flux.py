try:  # Assume we're a sub-module in a package.
    from . import fluxes as fx
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    import fluxes as fx


NAME_POS, TYPE_POS, HINT_POS = 0, 1, 2
TYPES = dict(str=str, int=int, float=float, bool=bool)


def is_row(row):
    return isinstance(row, (list, tuple))


def is_valid(row, schema):
    if is_row(row):
        if schema is not None:
            for value, description in zip(row, schema):
                field_type = description[TYPE_POS]
                if field_type in TYPES.values():
                    return isinstance(value, field_type)
                elif field_type == TYPES.keys():
                    selected_type = TYPES[field_type]
                    return isinstance(value, selected_type)
        else:
            return True


def check_rows(rows, schema, skip_errors=False):
    for r in rows:
        if is_valid(r, schema=schema):
            pass
        elif skip_errors:
            continue
        else:
            raise TypeError('check_records(): this item is not valid record for schema {}: {}'.format(schema, r))
        yield r


def get_cast_function(field_type):
    return TYPES[field_type]


def cast(value, field_type):
    cast_function = get_cast_function(field_type)
    return cast_function(value)


def apply_schema_to_row(row, schema):
    for c, (value, description) in enumerate(zip(row, schema)):
        field_type = description[TYPE_POS]
        new_value = cast(value, field_type)
        row[c] = new_value
    return row


class SchemaFlux(fx.RowsFlux):
    def __init__(self, items, count=None, check=True, schema=None):
        super().__init__(
            items=check_rows(items, schema) if check else items,
            count=count,
            check=check,
        )
        self.schema = schema or list()

    def meta(self):
        return dict(
            coout=self.count(),
            check=self.check(),
            schema=self.schema(),
        )

    def is_valid_item(self, item):
        return is_valid(
            item,
            schema=self.schema,
        )

    def valid_items(self, items, skip_errors=False):
        return check_rows(
            items,
            self.schema,
            skip_errors,
        )

    def set_schema(self, schema, check=True):
        return SchemaFlux(
            items=check_rows(self.items, schema=schema) if check else self.items,
            count=self.count,
            schema=schema,
        )

    def schematize(self, schema, skip_errors=False):
        def apply_schema_to_rows(rows):
            for r in rows:
                if skip_errors:
                    try:
                        yield apply_schema_to_row(r, schema)
                    except ValueError:
                        pass
                else:
                    yield apply_schema_to_row(r, schema)
        return SchemaFlux(
            apply_schema_to_rows(self.items),
            count=None if skip_errors else self.count,
            check=False,  # already checked
            schema=schema,
        )
