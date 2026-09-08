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

# Global filesystem
fs = fsspec.filesystem("http")


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
            with GLOBAL_LOCK:
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

        with fs.open(self.filename, "rb") as fh:
            # Coming with new pyfive version
            # max_request_block = os.environ.get('PYFIVE_REQUEST_MAX')
            # batch_request_size = os.environ.get('PYFIVE_REQUEST_BATCH', 150)

            try:
                ds = pyfive.File(fh)  # , max_request_block=max_request_block,
                # batch_request_size=batch_request_size)
            except pyfive.core.InvalidHDF5File:
                raise ValueError(
                    "Remote access unavailable for non-HDF5 files. "
                    "(NetCDF3 not supported)"
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

                self.extent = correct_slice(
                    self.extent, array.shape, self.named_dims, dims
                )

            # Correct handling of units
            if hasattr(array, "attrs"):
                if "units" in array.attrs:
                    self.units = str(np.array(array.attrs.get("units"), dtype=str))

            var = np.array(array[tuple(self.extent)], dtype=self.dtype)
            ds.close()

            self._array = var
