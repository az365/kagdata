from itertools import chain
try:  # Assume we're a sub-module in a package.
    from .flux import Flux
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from flux import Flux


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


class RecordsFlux(Flux):
    def __init__(self, iterable, count=None, check=True):
        super().__init__(
            iterable=check_records(iterable) if check else iterable,
            count=count,
        )

    def set_count(self, count):
        return RecordsFlux(
            self.input_iterable,
            count=count,
            check=False,
        )

    def get_records(self, skip_errors=False, raise_errors=True):
        if skip_errors or raise_errors:
            return check_records(self.input_iterable, skip_errors)
        else:
            return self.input_iterable

    def add_records(self, records, before=False, skip_errors=False):
        new_records = check_records(records, skip_errors)
        old_records = self.get_records(skip_errors)
        if before:
            chain_records = chain(new_records, old_records)
        else:
            chain_records = chain(old_records, new_records)
        return RecordsFlux(
            chain_records,
            count=None,
        )

    def add_flux(self, flux, before=False, skip_errors=False):
        return self.add_records(
            flux.input_iterable,
            before=before,
            skip_errors=skip_errors,
        ).set_count(
            self.expected_count + flux.expected_count,
        )

    def to_rows(self, columns, add_title_row=False):
        def get_rows(columns_list):
            if add_title_row:
                yield columns_list
            for r in self.input_iterable:
                yield [r.get(f) for f in columns_list]
        return Flux(
            get_rows(list(columns)),
            self.expected_count + (1 if add_title_row else 0),
        )

    def to_lines(self, columns, add_title_row=False, delimiter='\t'):
        return Flux(
            self.to_rows(columns, add_title_row=add_title_row),
            self.expected_count,
        ).map(
            delimiter.join,
        )
