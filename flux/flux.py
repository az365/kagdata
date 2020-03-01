from itertools import chain
import csv


class Flux:
    def __init__(self, items, count=None):
        self.items = items
        self.count = count

    def validate(self, item):
        return True

    def iterable(self):
        for i in self.items:
            yield i

    def next(self):
        return next(
            self.iterable(),
        )

    def one(self):
        for i in self.items:
            return i

    def expected_count(self):
        return self.count

    def final_count(self):
        result = 0
        for _ in self.items:
            result += 1
        return result

    def set_count(self, count):
        return Flux(
            self.items,
            count,
        )

    def apply(self, function):
        return Flux(
            function(self.items),
        )

    def map(self, function):
        return Flux(
            map(function, self.items),
            self.count,
        )

    def filter(self, function):
        return Flux(
            filter(function, self.items),
        )

    def enumerated(self):
        for n, i in enumerate(self.items):
            yield n, i

    def enumerate(self):
        return Flux(
            items=self.enumerated(),
            count=self.count,
        )

    def take(self, max_count=1):
        def take_items(m):
            for n, i in self.enumerated():
                if n >= m:
                    break
                yield i
        return Flux(
            take_items(max_count)
        )

    def skip(self, count=1):
        def skip_items(c):
            for n, i in self.enumerated():
                if n >= c:
                    yield i
        return Flux(
            skip_items(count),
            self.count - count
        )

    def add(self, flux_or_items, before=False, **kwargs):
        if isinstance(flux_or_items, Flux):
            return self.add_flux(
                flux_or_items,
                before=before,
            )
        else:
            return self.add_items(
                flux_or_items,
                before=before
            )

    def add_items(self, items, before=False):
        old_items = self.items
        new_items = items
        if before:
            chain_records = chain(new_items, old_items)
        else:
            chain_records = chain(old_items, new_items)
        return Flux(
            chain_records,
            count=None,
        )

    def add_flux(self, flux, before=False):
        old_count = self.count
        new_count = flux.count
        if old_count is not None or new_count is not None:
            total_count = new_count + old_count
        else:
            total_count = None
        return self.add_items(
            flux.items,
            before=before,
        ).set_count(
            total_count,
        )

    def count_to_items(self):
        return self.add_items(
            [self.count],
            before=True,
        )

    def separate_count(self):
        return (
            self.count,
            self,
        )

    def separate_first(self):
        iterable = self.iterable()
        count = self.count
        if count:
            count -= 1
        title_item = next(iterable)
        data_flux = Flux(
            iterable,
            count=count,
        )
        return (
            title_item,
            data_flux,
        )

    def to_list(self):
        return list(self.items)

    def convert_to_list(self):
        return Flux(
            self.to_list(),
            self.count,
        )

    def save(self, filename, encoding=None, end='\n', verbose=True):
        def write_and_yield(fh, lines, verbose):
            n = 0
            for n, i in enumerate(lines):
                if n > 0:
                    fileholder.write(end)
                fh.write(str(i))
                yield i
            fh.close()
            if verbose:
                print('Done. {} rows has written into {}'.format(n + 1, filename))
        fileholder = open(filename, 'w', encoding=encoding) if encoding else open(filename, 'w')
        return Flux(
            write_and_yield(fileholder, self.items, verbose),
            count=self.count,
        )

    def to_file(self, filename, encoding=None, end='\n', verbose=True):
        saved_flux = self.save(filename, encoding, end, verbose)
        for _ in saved_flux.items:
            pass

    def to_rows(self, delimiter=None):
        lines = self.items
        rows = csv.reader(lines, delimiter=delimiter) if delimiter else csv.reader(lines)
        return Flux(
            rows,
            self.count,
        )

    def to_records(self, columns):
        def get_records(rows, cols):
            for r in rows:
                yield {k: v for k, v in zip(cols, r)}
        return Flux(
            get_records(self.items, columns),
            self.count,
        )
