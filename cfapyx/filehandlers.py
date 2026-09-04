import logging
import threading

import fsspec
import netCDF4
import numpy as np
import pyfive
from dask.utils import SerializableLock

from cfapyx.utils import correct_slice, logstream

logger = logging.getLogger(__name__)

logger.addHandler(logstream)
logger.propagate = False

GLOBAL_LOCK = SerializableLock()


class NumpyDatasetHandler:
    def __init__(
        self,
        filename: str,
        address: str,
        dtype: object,
        named_dims: tuple,
        extent: tuple | None = None,
        remote: bool = False,
    ):
        """
        Wrapper method for opening
        """

        self.filename = filename
        self.address = address
        self.dtype = dtype
        self.extent = extent

        self.named_dims = named_dims

        self.units = None
        self._array = None

        if not remote:
            logger.debug("ENTER" + threading.current_thread().name + filename)

            with GLOBAL_LOCK:
                self.open_netcdf4()

            logger.debug("EXIT" + threading.current_thread().name + filename)

            # NetCDF4 library requires dask single thread scheduling

        else:
            logger.info("Using pyfive for remote HDF5 file (NetCDF3 not supported)")
            self.open_pyfive()

    def __array__(self):
        """Extract numpy array already held"""
        return self._array

    def open_netcdf4(self):

        ds = netCDF4.Dataset(self.filename)

        # Apply variable
        if "/" in self.address:
            addr = self.address.split("/")
            group = "/".join(addr[1:-1])
            varname = addr[-1]

            array = ds.groups[group][varname]
        else:
            array = ds[self.address]

        if hasattr(array, "units"):
            self.units = array.units

        # Apply extent
        if len(array.shape) != len(self.extent):
            # Extract named dims from pyfive variable

            self.extent = correct_slice(
                self.extent, array.shape, self.named_dims, array.dimensions
            )

        var = np.array(array[tuple(self.extent)], dtype=self.dtype)
        ds.close()

        self._array = var

    def open_pyfive(self):

        fs = fsspec.filesystem("http")
        fh = fs.open(self.filename, "rb")
        try:
            ds = pyfive.File(fh)
        except pyfive.core.InvalidHDF5File:
            raise ValueError(
                "Remote access unavailable for non-HDF5 files. (NetCDF3 not supported)"
            )

        # Apply variable
        if "/" in self.address:
            addr = self.address.split("/")
            array = ds[addr[1]]
            for g in addr[2:]:
                array = array[g]
        else:
            array = ds[self.address]

        # Apply extent
        if len(array.shape) != len(self.extent):
            # Extract named dims from pyfive variable
            dims = tuple([dim[0].name.split("/")[-1] for dim in array.dims])

            self.extent = correct_slice(self.extent, array.shape, self.named_dims, dims)

        var = np.array(array[tuple(self.extent)], dtype=self.dtype)
        ds.close()

        self._array = var
