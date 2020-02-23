try:  # Assume we're a sub-module in a package.
    from .flux import Flux
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from flux import Flux


def iterable(any_iterable):
    return Flux(any_iterable)


def from_list(input_list):
    def get_generator_from_list(mylist):
        for i in mylist:
            yield i
    return Flux(
        get_generator_from_list(input_list),
        count=len(input_list),
    )
