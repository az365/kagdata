try:  # Assume we're a sub-module in a package.
    from .any_flux import AnyFlux
    from .lines_flux import LinesFlux
    from .rows_flux import RowsFlux
    from .records_flux import RecordsFlux
    from .schema_flux import SchemaFlux
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from any_flux import AnyFlux
    from lines_flux import LinesFlux
    from rows_flux import RowsFlux
    from records_flux import RecordsFlux
    from schema_flux import SchemaFlux
