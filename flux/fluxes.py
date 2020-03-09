from enum import Enum

try:  # Assume we're a sub-module in a package.
    from .any_flux import AnyFlux
    from .lines_flux import LinesFlux
    from .rows_flux import RowsFlux
    from .pairs_flux import PairsFlux
    from .schema_flux import SchemaFlux
    from .records_flux import RecordsFlux
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from any_flux import AnyFlux
    from lines_flux import LinesFlux
    from rows_flux import RowsFlux
    from pairs_flux import PairsFlux
    from schema_flux import SchemaFlux
    from records_flux import RecordsFlux


class FluxType(Enum):
    AnyFlux = 'AnyFlux'
    LinesFlux = 'LinesFlux'
    RowsFlux = 'RowsFlux'
    RecordsFlux = 'RecordsFlux'
    SchemaFlux = 'SchemaFlux'


def get_class(flux_type):
    assert isinstance(flux_type, FluxType), TypeError('flux_type must be an instance of FluxType')
    if flux_type == FluxType.SchemaFlux:
        return SchemaFlux
    elif flux_type == FluxType.LinesFlux:
        return LinesFlux
    elif flux_type == FluxType.RowsFlux:
        return RowsFlux
    elif flux_type == FluxType.PairFlux:
        return PairsFlux
    elif flux_type == FluxType.SchemaFlux:
        return SchemaFlux
    elif flux_type == FluxType.RecordsFlux:
        return RecordsFlux


