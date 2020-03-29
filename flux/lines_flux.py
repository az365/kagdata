import json
import csv

try:
    from . import fluxes as fx
    from . import readers
except ImportError:
    import fluxes as fx
    import readers


MAX_LINES_IN_MEMORY = 10000000


def is_line(line):
    return isinstance(line, str)


def check_lines(lines, skip_errors=False):
    for i in lines:
        if is_line(i):
            pass
        elif skip_errors:
            continue
        else:
            raise TypeError('check_records(): this item is not record: {}'.format(i))
        yield i


class LinesFlux(fx.AnyFlux):
    def __init__(self, items, count=None, check=True, source=None):
        super().__init__(
            check_lines(items) if check else items,
            count=count,
        )
        self.check = check
        self.source = source

    def meta(self):
        return dict(
            count=self.count,
            check=self.check,
            source=self.source,
        )

    @staticmethod
    def is_valid_item(item):
        return is_line(item)

    @staticmethod
    def valid_items(items, skip_errors=False):
        return check_lines(items, skip_errors)

    def parse_json(self, default_value=None):
        def json_loads(line):
            try:
                return json.loads(line)
            except json.JSONDecodeError as err:
                if default_value is not None:
                    return default_value
                else:
                    raise json.JSONDecodeError(err.msg, err.doc, err.pos)
        return self.to_records(
            json_loads,
        ).set_count(
            self.count,
        )

    def save(self, filename, encoding=None, end='\n', verbose=True, immediately=False):
        def write_and_yield(fh, lines):
            n = 0
            for n, i in enumerate(lines):
                if n > 0:
                    fileholder.write(end)
                fh.write(str(i))
                yield i
            fh.close()
            if verbose:
                print('Done. {} rows has written into {}'.format(n + 1, filename))
        if immediately:
            self.to_file(filename, encoding, end, verbose, return_flux=True)
        else:
            fileholder = open(filename, 'w', encoding=encoding) if encoding else open(filename, 'w')
            return LinesFlux(
                write_and_yield(fileholder, self.items),
                count=self.count,
            )

    def to_file(self, filename, encoding=None, end='\n', verbose=True, return_flux=True):
        saved_flux = self.save(filename, encoding, end, verbose, immediately=False)
        saved_flux.pass_items()
        if return_flux:
            return readers.from_file(
                filename,
                encoding=encoding,
                verbose=verbose,
            )

    def to_rows(self, delimiter=None):
        lines = self.items
        rows = csv.reader(lines, delimiter=delimiter) if delimiter else csv.reader(lines)
        return fx.RowsFlux(
            rows,
            self.count,
        )

    def to_pairs(self, delimiter=None):
        lines = self.items
        rows = csv.reader(lines, delimiter=delimiter) if delimiter else csv.reader(lines)
        return fx.RowsFlux(
            rows,
            self.count,
        ).to_pairs()
