__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

# Chunk wrapper is common to both CFAPyX and XarrayActive
VERSION = 1.0

import numpy as np
import netCDF4

from itertools import product
from dask.utils import SerializableLock

class ChunkWrapper:
    description = "Wrapper class for individual Fragment retrievals. May incorporate Active Storage routines as applicable methods called via Dask."

    def __init__(self,
                 filename,
                 address,
                 dtype=None,
                 shape=None,
                 position=None,
                 extent=None
            ):
        
        """
        Wrapper object for the 'array' section of a fragment. Contains some metadata to ensure the
        correct fragment is selected, but generally just serves the fragment array to dask when required.

        Parameters: extent - in the form of a list/tuple of slices for this fragment. Different from the 
                             'location' parameter which states where the fragment fits into the total array.
        """
        
        self.__array_function__ = self.get_array

        self.filename = filename
        self.address  = address
        self.extent   = extent
        self.dtype    = dtype
        self.shape    = shape
        self.size     = product(shape)
        self.ndim     = len(shape)

        # Required by dask for thread-safety.
        self._lock    = SerializableLock()

        self.position = position

    def _combine_slices(self, newslice):

        if len(newslice) != self.shape:
            if hasattr(self, 'active'):
                # Mean has already been computed. Raise an error here 
                # since we should be down to dealing with numpy arrays now.
                raise ValueError(
                    "Active chain broken - mean has already been accomplished."
                )
            else:
                self._array = self.get_array(extent=newslice)
                return None
        
        def combine_sliced_dim(old, new):
            start = old.start + old.step*new.start
            step  = old.step * new.step
            stop  = start + step * (new.stop - new.start)
            return slice(start, stop, step)


        if not self._extent:
            return newslice
        else:
            extent = self._extent
            for dim in self.ndim:
                extent[dim] = combine_sliced_dim(extent[dim], newslice[dim])
            return extent
        
    def __getitem__(self, selection):
        self.extent = self._combine_slices(selection)
        return self

    def get_array(self, extent=None):
        """
        Retrieves the array of data for this variable chunk, casted into a Numpy array.
        """
        ds = self.open()

        if extent and self.extent:
            extent = self._combine_slices(extent)
        elif self.extent:
            extent = self.extent
        else:
            pass

        if '/' in self.address:
            # Assume we're dealing with groups but we just need the data for this variable.

            addr = self.address.split('/')
            group = '/'.join(addr[1:-1])
            varname = addr[-1]

            ds = ds.groups[group]

        else:
            varname = self.address

        try:
            array = ds.variables[varname]
        except KeyError:
            raise ValueError(
                f"Dask Chunk at '{self.position}' does not contain "
                f"the variable '{varname}'."
            )
        
        if extent:
            try:
                var = np.array(array[tuple(extent)])
            except IndexError:
                raise ValueError(
                    f"Unable to select required 'extent' of {self.extent} "
                    f"from fragment {self.position} with shape {array.shape}"
                )
        else:
            var = np.array(array)

        return var
    
    def open(self):
        """Must be implemented by child class to properly open different file types."""
        raise NotImplementedError
    
class NetCDFChunkWrapper(ChunkWrapper):
    def open(self): # get lock/release lock
        return netCDF4.Dataset(self.filename, mode='r')
