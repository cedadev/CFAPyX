__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

VERSION = 1.0

from CFAPyX.utils import OneOrMoreList
from CFAPyX.decoder import get_nfrags_per_dim, fragment_descriptors
from CFAPyX.active import CFAActiveArray

import dask.array as da
from dask.array.core import getter
from dask.base import tokenize
from dask.utils import SerializableLock

from itertools import product

import netCDF4

import numpy as np

class FragmentArrayWrapper():

    def __init__(self, fragment_info, fragment_array_shape, ndim, shape, units, dtype, cfa_options=None):

        self.fragment_info = fragment_info
        self.fragment_array_shape  = fragment_array_shape

        fragments      = list(self.fragment_info.keys())

        # Required parameters list
        self.ndim  = ndim
        self.shape = shape
        self.units = units
        self.dtype = dtype

        self.fragmented_dim_indexes = tuple([i for i in range(self.ndim) if self.fragment_shape[i] != 1])

        if cfa_options:
            self.cfa_options = cfa_options
            self.apply_cfa_options(**cfa_options)

        self.__array_function__ = self.get_array

    def apply_cfa_options(
            self,
            substitutions=None,
            decode_cfa=None
            ):
        
        if substitutions:

            if type(substitutions) != list:
                substitutions = [substitutions]

            for s in substitutions:
                base, substitution = s.split(':')
                for f in self.fragment_info.keys():
                    self.fragment_info[f]['filename'] = self.fragment_info[f]['filename'].replace(base, substitution)

    @property
    def shape(self):
        return self._shape
    
    @shape.setter
    def shape(self, value):
        self._shape = value

    @property
    def units(self):
        return self._units
    
    @units.setter
    def units(self, value):
        self._units = value

    @property
    def dtype(self):
        return self._dtype
    
    @dtype.setter
    def dtype(self, value):
        self._dtype = value

    @property
    def ndim(self):
        return self._ndim
    
    @ndim.setter
    def ndim(self, value):
        self._ndim = value

    def __getitem__(self, selection):
        array = self.get_array()
        return array[selection]

    def get_array(self, needs_lock=True):

        name = (f"{self.__class__.__name__}-{tokenize(self)}",)

        dtype = self.dtype
        units = self.units

        calendar = None # Fix later

        # Fragment info dict at this point
        fragment_info = self.fragment_info

        # For now expect to deal only with NetCDF Files

        nfrags_per_dim = get_nfrags_per_dim(
            array_shape = self.shape,
            fragmented_dim_indexes = self.fragmented_dim_indexes,
            fragment_array_shape = self.fragment_array_shape,
            fragment_info = fragment_info,
            ndim=self.ndim,
            dtype=np.dtype(np.float64),
            explicit_shapes = None
        )

        # dict of array-like objects to pass to the dask Array constructor.
        dsk = {}

        for finfo in self.fragment_info.keys():

            # Needs f_indices
            
            fragment_format   = 'nc'
            fragment_shape    = self.fragment_info[finfo]['shape']
            fragment_position = finfo

            fragment = get_fragment_wrapper(
                fragment_format,
                dtype=dtype,
                shape=fragment_shape,
                position=fragment_position,
                aggregated_units=units,
                aggregated_calendar=calendar,
            )

            key = f"{fragment.__class__.__name__}-{tokenize(fragment)}"
            dsk[key] = fragment
            dsk[name + fragment_position] = (
                getter, # From dask docs
                key,
                f_indices,
                False,
                getattr(fragment, "_lock", False) # Check version cf-python
            )

        return CFAActiveArray(dsk, name[0], chunks=nfrags_per_dim, dtype=dtype)

def get_fragment_wrapper(format, **kwargs):
    if format == 'nc':
        return NetCDFFragmentWrapper(**kwargs)
    else:
        raise NotImplementedError(
            f"Fragment type '{format}' not supported"
        )

class FragmentWrapper():

    description = "Wrapper class for individual Fragment retrievals. May incorporate Active Storage routines as applicable methods called via Dask."

    def __init__(self,
                 filename,
                 address,
                 extent=None,
                 dtype=None,
                 shape=None,
                 position=None,
                 aggregated_units=None,
                 aggregated_calendar=None
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
        self.aggregated_units    = aggregated_units # cfunits conform method.
        self.aggregated_calendar = aggregated_calendar
        
    def __getitem__(self, selection):
        ds = self.get_array(extent=tuple(selection))
        return ds

    def get_array(self, extent=None):
        ds = self.open()
        # Use extent to just select the section and variable I'd actually like to deal with here.

        if not extent:
            extent = self.extent
        if extent and self.extent:
            raise NotImplementedError(
                "Nested selections not supported. "
                f"FragmentWrapper.get_array supplied '{extent}' "
                f"and '{self.extent}' as selections."
            )
        
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
                f"CFA fragment '{self.position}' does not contain "
                f"the variable '{varname}'."
            )
        
        if extent:
            print(f'Post-processed Extent: {extent}')
            try:
                # This may not be loading the data in the most efficient way.
                # Current: Slice the NetCDF-Dataset object then write to numpy.
                #  - This should hopefully take into account chunks and loading 
                #    only the required data etc.

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
    
class NetCDFFragmentWrapper(FragmentWrapper):
    def open(self): # get lock/release lock
        return netCDF4.Dataset(self.filename, mode='r')
