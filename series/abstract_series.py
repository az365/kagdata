from abc import ABC, abstractmethod


class AbstractSeries(ABC):
    def __init__(
            self,
            values,
    ):
        self.values = values

    @abstractmethod
    def get_data_fields(self):
        return ['values']

    @abstractmethod
    def get_meta_fields(self):
        pass

    def get_data(self):
        return {f: self.__dict__[f] for f in self.get_data_fields()}

    def get_meta(self):
        return {f: self.__dict__[f] for f in self.get_meta_fields()}

    def set_meta(self, dict_meta):
        new = self.copy()
        if dict_meta:
            new.__dict__.update(dict_meta)
        return new

    # def new(self, *args, save_meta=False, **kwargs):
    #     return self.__class__(
    #         *args, **kwargs
    #     ).set_meta(
    #         self.get_meta() if save_meta else dict(),
    #     )

    def copy(self):
        dict_copy = {k: v.copy() for k, v in self.get_data().items()}
        dict_copy.update(self.get_meta())
        return self.__class__(**dict_copy)

    def get_values(self):
        return self.values

    def set_values(self, values):
        new = self.new(save_meta=True)
        new.values = values
        return new

    def get_iter(self):
        yield from self.get_items()

    def get_list(self):
        return list(self.get_items())
