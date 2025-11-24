__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import logging

logging.basicConfig(level=logging.WARN)
logstream = logging.StreamHandler()

formatter = logging.Formatter('%(levelname)s [%(name)s]: %(message)s')
logstream.setFormatter(formatter)

logger = logging.getLogger(__name__)

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
    'primary':   ('map','uris','identifiers'),
    'secondary': ('map', 'unique_values'),
    'interim':   ('shape', 'location', 'address'),
    'beta':      ('location', 'file', 'format')
}

def slice_to_shape(slice, dshape):
    """
    Transform python slice to resulting shape"""
    
    start = slice.start or 0
    stop  = slice.stop or dshape
    step  = slice.step or 1

    while start < 0:
        start += dshape
    while start > dshape:
        start -= dshape
    
    while stop < 0:
        stop += dshape
    while stop > dshape:
        stop -= dshape
    shape = int((stop-start)/step)
    logger.debug(slice)
    logger.debug(f'Resolved: {start}, {stop}, {step}')
    logger.debug(f'Shape: {shape}')

    # Always drop 1-dimensional shape elements
    if shape != 1:
        return shape