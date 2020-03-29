from itertools import chain, tee
import json

try:  # Assume we're a sub-module in a package.
    from . import fluxes as fx
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    import fluxes as fx


class AnyFlux:
    def __init__(self, items, count=None):
        self.items = items
        self.count = count

    def meta(self):
        return dict(
            count=self.count,
        )

    @staticmethod
    def is_valid_item(item):
        return True

    @staticmethod
    def valid_items(items, **kwargs):
        return items

    def validated(self, skip_errors=False):
        return self.__class__(
            self.valid_items(self.items, skip_errors=skip_errors),
            **self.meta()
        )

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
        return self.__class__(
            self.items,
            **self.meta()
        )

    def tee(self, n=2):
        return [
            self.__class__(
                i,
                count=self.count,
            ) for i in tee(
                self.items,
                n,
            )
        ]

    def copy(self):
        self.items, copy_items = tee(self.items, 2)
        return self.__class__(
            copy_items,
            **self.meta()
        )

    def apply(self, function, native=True, save_count=False):
        target_class = self.__class__ if native else AnyFlux
        props = self.meta()
        if not save_count:
            props.pop('count')
        return target_class(
            function(self.items),
            **props
        )

    def flat_map(self, function):
        return self.__class__(
            map(function, self.items),
            self.count,
        )

    def map(self, function):
        return AnyFlux(
            map(function, self.items),
            self.count,
        )

    def filter(self, function):
        props = self.meta()
        props.pop('count')
        return self.__class__(
            filter(function, self.items),
            **props
        )

    def enumerated_items(self):
        for n, i in enumerate(self.items):
            yield n, i

    def enumerate(self, native=False):
        props = self.meta()
        if native:
            target_class = self.__class__
        else:
            target_class = fx.PairsFlux
            props['secondary'] = fx.FluxType(self.__class__.__name__)
        return target_class(
            items=self.enumerated_items(),
            **props
        )

    def take(self, max_count=1):
        def take_items(m):
            for n, i in self.enumerated_items():
                if n >= m:
                    break
                yield i
        props = self.meta()
        props['count'] = min(self.count, max_count) if self.count else None
        return self.__class__(
            take_items(max_count),
            **props
        )

    def skip(self, count=1):
        def skip_items(c):
            for n, i in self.enumerated_items():
                if n >= c:
                    yield i
        props = self.meta()
        props['count'] = self.count - count if self.count else None
        return self.__class__(
            skip_items(count),
            **props
        )

    def pass_items(self):
        for _ in self.items:
            pass

    def add(self, flux_or_items, before=False, **kwargs):
        if isinstance(flux_or_items, AnyFlux):
            return self.add_flux(
                flux_or_items,
                before=before,
            )
        else:
            return self.add_items(
                flux_or_items,
                before=before,
            )

    def add_items(self, items, before=False):
        old_items = self.items
        new_items = items
        if before:
            chain_records = chain(new_items, old_items)
        else:
            chain_records = chain(old_items, new_items)
        props = self.meta()
        props['count'] = None
        return self.__class__(
            chain_records,
            **props
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
        items = self.iterable()
        props = self.meta()
        if props.get('count'):
            props['count'] -= 1
        title_item = next(items)
        data_flux = self.__class__(
            items,
            **props
        )
        return (
            title_item,
            data_flux,
        )

    def split_by_pos(self, pos):
        first_flux, second_flux = self.tee(2)
        return (
            first_flux.take(pos),
            second_flux.skip(pos),
        )

    def split_by_list_pos(self, list_pos):
        count_limits = len(list_pos)
        cloned_fluxes = self.tee(count_limits + 1)
        filtered_fluxes = list()
        prev_pos = 0
        for n, cur_pos in enumerate(list_pos):
            count_items = cur_pos - prev_pos
            filtered_fluxes.append(
                cloned_fluxes[n].skip(
                    prev_pos,
                ).take(
                    count_items,
                ).set_count(
                    count_items,
                )
            )
            prev_pos = cur_pos
        filtered_fluxes.append(
            cloned_fluxes[-1].skip(
                list_pos[-1],
            )
        )
        return filtered_fluxes

    def split_by_numeric(self, func, count):
        return [
            f.filter(
                lambda i, n=n: func(i) == n,
            ) for n, f in enumerate(
                self.tee(count),
            )
        ]

    def split_by_boolean(self, func):
        return self.split_by_numeric(
            func=lambda f: int(bool(func(f))),
            count=2,
        )

    def split(self, by, count=None):
        if isinstance(by, int):
            return self.split_by_pos(by)
        elif isinstance(by, (list, tuple)):
            return self.split_by_list_pos(by)
        elif callable(by):
            if count:
                return self.split_by_numeric(by, count)
            else:
                return self.split_by_boolean(by)
        else:
            raise TypeError('split(by): by-argument must be int, list, tuple or function, {} received'.format(type(by)))

    def get_list(self):
        return list(self.items)

    def convert_to_list(self):
        return self.__class__(
            self.get_list(),
            **self.meta()
        )

    def to_any(self):
        return fx.AnyFlux(
            self.items,
            count=self.count,
        )

    def to_lines(self, **kwargs):
        return fx.LinesFlux(
            self.map(str).items,
            count=self.count,
            check=True,
        )

    def to_json(self, **kwargs):
        return self.map(
            json.dumps
        ).to_lines()

    def to_rows(self, **kwargs):
        return fx.RowsFlux(
            self.items,
            count=self.count,
        )

    def to_pairs(self, **kwargs):
        return fx.PairsFlux(
            self.items,
            count=self.count,
        )

    def to_records(self, function=lambda i: dict(item=i), **kwargs):
        return fx.RecordsFlux(
            items=map(function, self.items) if function else self.items,
            count=self.count,
            check=True,
        )
