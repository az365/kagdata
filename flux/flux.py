class Flux:
    def __init__(self, iterable, count=None):
        self.input_iterable = iterable
        self.expected_count = count

    def map(self, function):
        return Flux(
            map(function, self.input_iterable),
            self.expected_count,
        )

    def filter(self, function):
        return Flux(
            filter(function, self.input_iterable),
        )

    def take(self, max_count=1):
        def internal_take(m):
            for n, i in enumerate(self.input_iterable):
                if n >= m:
                    break
                yield i
        return Flux(
            internal_take(max_count)
        )

    def next(self):
        return next(self.input_iterable)

    def one(self):
        for i in self.input_iterable:
            return i

    def expected_count(self):
        return self.expected_count()

    def final_count(self):
        result = 0
        for _ in self.input_iterable:
            result += 1
        return result

    def to_list(self):
        return list(self.input_iterable)
