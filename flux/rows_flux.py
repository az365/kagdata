try:  # Assume we're a sub-module in a package.
    from . import fluxes as fx
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    import fluxes as fx


def is_row(row):
    return isinstance(row, (list, tuple))


def check_rows(rows, skip_errors=False):
    for i in rows:
        if is_row(i):
            pass
        elif skip_errors:
            continue
        else:
            raise TypeError('check_records(): this item is not record: {}'.format(i))
        yield i


class RowsFlux(fx.AnyFlux):
    def __init__(self, items, count=None, check=True):
        super().__init__(
            items=check_rows(items) if check else items,
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
        return is_row(item)

    @staticmethod
    def valid_items(items, skip_errors=False):
        return check_rows(items, skip_errors)

    def to_records(self, function=None, columns=[]):
        def get_records(rows, cols):
            for r in rows:
                yield {k: v for k, v in zip(cols, r)}
        if function:
            records = map(function, self.items)
        elif columns:
            records = get_records(self.items, columns)
        else:
            records = map(lambda r: dict(row=r), self.items)
        return fx.RecordsFlux(
            records,
            **self.meta()
        )
