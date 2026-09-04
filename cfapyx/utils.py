__author__ = "Daniel Westwood"
__contact__ = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import logging

import numpy as np

logging.basicConfig(level=logging.WARN)
logstream = logging.StreamHandler()

formatter = logging.Formatter("%(levelname)s [%(name)s]: %(message)s")
logstream.setFormatter(formatter)

logger = logging.getLogger(__name__)


def correct_slice(extent: tuple, shape: tuple, named_dims: tuple, array_dims: tuple):
    """
    Drop size-1 dimensions from the set of slices if there is an issue.

    :param array_dims:      (tuple) The set of named dimensions present in
        the source file. If there are fewer array_dims than the expected
        set in ``named_dims`` then this function is used to remove extra
        dimensions from the ``extent`` if possible.
    """
    extent = []
    for dim in range(len(named_dims)):
        named_dim = named_dims[dim]
        if named_dim in array_dims:
            extent.append(extent[dim])

        # named dim not present
        ext = extent[dim]

        start = ext.start or 0
        stop = ext.stop or shape[dim]
        step = ext.step or 1

        if int(stop - start) / step > 1:
            raise ValueError(
                f'Attempted to slice dimension "{named_dim}" using slice "{ext}" '
                "but the requested dimension is not present"
            )
    return extent


def supported_by_cftime(unit: str, calendar: str = "standard"):
    try:
        import cftime

        cftime.num2date(0, unit, calendar=calendar)
        return True
    except ImportError:
        logger.error(
            '"cftime" package not installed, unable to conform temporal units.'
        )
        return False
    except Exception:
        return False


def supported_by_pint(unit: str):
    try:
        from pint import UnitRegistry

        ureg = UnitRegistry()

        ureg.Unit(unit)
        return True
    except ImportError:
        logger.error('"pint" package not installed, unable to conform standard units.')
        return False
    except Exception:
        return False


def conform_data_to_units(data: np.ndarray, units: str, prime_units: str):

    if supported_by_cftime(units) and supported_by_cftime(prime_units):
        import cftime

        return cftime.date2num(cftime.num2date(data, units=units), units=prime_units)

    elif supported_by_pint(units) and supported_by_pint(prime_units):
        from pint import UnitRegistry

        ureg = UnitRegistry()
        # Data dtype does not change, units means some conversion should take place
        # but this should not impact the dtype.
        return np.array((data * ureg(units)).to(prime_units), dtype=data.dtype)
    else:
        raise ValueError(
            f'Unit conversion is required from "{units}" to "{prime_units}"'
            ' - no suitable method available. Install "cftime" and/or "pint"'
            "to enable unit conversion."
        )


def set_verbose(level: int):
    """
    Reset the logger basic config.
    """

    levels = [
        logging.WARN,
        logging.INFO,
        logging.DEBUG,
    ]

    if level >= len(levels):
        level = len(levels) - 1

    for name in logging.root.manager.loggerDict:
        lg = logging.getLogger(name)
        lg.setLevel(levels[level])


CONVENTIONS = {
    "primary": ("map", "uris", "identifiers"),
    "secondary": ("map", "unique_values"),
    "interim": ("shape", "location", "address"),
    "beta": ("location", "file", "format"),
}


def slice_to_shape(slice, dshape):
    """
    Transform python slice to resulting shape"""

    start = slice.start or 0
    stop = slice.stop or dshape
    step = slice.step or 1

    while start < 0:
        start += dshape
    while start > dshape:
        start -= dshape

    while stop < 0:
        stop += dshape
    while stop > dshape:
        stop -= dshape
    shape = int((stop - start) / step)
    logger.debug(slice)
    logger.debug(f"Resolved: {start}, {stop}, {step}")
    logger.debug(f"Shape: {shape}")

    # Stopped drop 1-dimensional shape elements
    return shape
