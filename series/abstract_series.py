from abc import ABC, abstractmethod


class AbstractSeries(ABC):
    def __init__(
            self,
            values,
            validate=False,
    ):
        self.values = values
        if validate:
            self.validate()

    @abstractmethod
    def get_data_fields(self):
        pass

    @abstractmethod
    def get_meta_fields(self):
        pass

    def get_data(self):
        return {f: self.__dict__[f] for f in self.get_data_fields()}

    def get_meta(self):
        return {f: self.__dict__[f] for f in self.get_meta_fields()}

    def set_meta(self, dict_meta, inplace=False):
        if inplace:
            for k, v in dict_meta.items():
                if hasattr(v, 'copy'):
                    v = v.copy()
                self.__dict__[k] = v
        else:
            new = self.copy()
            new.set_meta(dict_meta, inplace=True)
            return new

    def get_class_name(self):
        return self.__class__.__name__

    def new(self, *args, save_meta=False, **kwargs):
        new = self.__class__(*args, **kwargs)
        if save_meta:
            new.set_meta(
                self.get_meta(),
                inplace=True,
            )
        return new

    def get_properties(self):
        properties = {k: v.copy() for k, v in self.get_data().items()}
        properties.update(self.get_meta())
        return properties

    def copy(self):
        return self.__class__(
            validate=False,
            **self.get_properties()
        )

    def get_values(self):
        return self.values

    @abstractmethod
    def get_items(self):
        pass

    def set_values(self, values):
        new = self.new(save_meta=True)
        new.values = values
        return new

    def __iter__(self):
        yield from self.get_items()

    def get_list(self):
        return list(self.get_items())

    @abstractmethod
    def get_errors(self):
        pass

    def is_valid(self):
        return not list(self.get_errors())

    def validate(self, raise_exception=True, default=None):
        if self.is_valid():
            return self
        elif raise_exception:
            errors = list(self.get_errors())
            raise ValueError('; '.join(errors))
        else:
            return default
