from CFAPyX.utils import OneOrMoreList
from CFAPyX.decoder import fragment_shapes, fragment_descriptors

import dask.array as da
from dask.array.core import getter
from dask.base import tokenize

from itertools import product

import netCDF4

import numpy as np

class FragmentArrayWrapper():

    def __init__(self, decoded_cfa, ndim, shape, units, dtype):

        self.aggregated_data = decoded_cfa['aggregated_data']
        self.fragment_shape  = decoded_cfa['fragment_shape']

        fragments      = list(self.aggregated_data.keys())
        self.sources   = [self.aggregated_data[i]['filename'] for i in fragments]
        self.names     = OneOrMoreList([self.aggregated_data[i]['address'] for i in fragments])

        # Required parameters list
        self.ndim  = ndim
        self.shape = shape
        self.units = units
        self.dtype = dtype
        self.__array_function__ = self.get_array

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

        aggregated_data = self.aggregated_data
        
        #chunks = subarray_shapes()

        # For now expect to deal only with NetCDF Files

        fsizes_per_dim = fragment_shapes(
            shapes = None,
            array_shape = self.shape,
            fragment_dims = (0,),
            fragment_shape = self.fragment_shape,
            aggregated_data = aggregated_data,
            ndim=3,
            dtype=np.dtype(np.float64)

        )
        fragment_dims = (0,)

        dsk = {}

        # Replace with CFAPyX custom class
        #from cf.data.fragment import NetCDFFragmentArray

        for (
            u_indices,
            u_shape,
            f_indices,
            fragment_location,
            fragment_location,
            fragment_shape,
        ) in zip(*fragment_descriptors(fsizes_per_dim, fragment_dims, self.shape)):
            kwargs = aggregated_data[fragment_location].copy()
            kwargs.pop("location",None)

            fragment_format = kwargs.pop("format",None)
            # Assume nc format for now.

            kwargs['fragment_location'] = fragment_location
            kwargs['extent'] = None

            fragment = get_fragment_wrapper(
                fragment_format,
                dtype=dtype,
                shape=fragment_shape,
                aggregated_units=units,
                aggregated_calendar=calendar,
                **kwargs
            )

            key = f"{fragment.__class__.__name__}-{tokenize(fragment)}"
            dsk[key] = fragment
            dsk[name + fragment_location] = (
                getter,
                key,
                f_indices,
                False,
                getattr(fragment, "_lock", False)
            )

        return da.Array(dsk, name[0], chunks=fsizes_per_dim, dtype=dtype)

def get_fragment_wrapper(format, **kwargs):
    if format == 'nc':
        return NetCDFFragmentWrapper(**kwargs)
    else:
        raise NotImplementedError(
            f"Fragment type '{format}' not supported"
        )

class FragmentWrapper():

    """
    Possible attributes to add:
     - Units (special class)
     - aggregated_Units
     - size
    
    Possible Methods/Properties:
    _atol (prop)
    _components (prop)
    _conform_to_aggregated_units (method)
    _custom (prop)
    _dask_meta (prop)
    
    """

    def __init__(self,
                 filename,
                 address,
                 extent=None,
                 fragment_location=None,
                 dtype=None,
                 shape=None,
                 aggregated_units=None,
                 aggregated_calendar=None,
                 **kwargs):
        
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

        self.fragment_location   = fragment_location
        self.aggregated_units    = aggregated_units
        self.aggregated_calendar = aggregated_calendar
        
    def __getitem__(self, selection):
        ds = self.get_array(extent=selection)
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

        try:
            array = ds.variables[self.address]
        except KeyError:
            raise ValueError(
                f"CFA fragment '{self.fragment_location}' does not contain "
                f"the variable '{self.address}'."
            )
        
        if extent:
            try:
                # This may not be loading the data in the most efficient way.
                # Current: Slice the NetCDF-Dataset object then write to numpy.
                #  - This should hopefully take into account chunks and loading 
                #    only the required data etc.

                var = np.array(array[tuple(extent)])
            except IndexError:
                raise ValueError(
                    f"Unable to select required 'extent' of {self.extent} "
                    f"from fragment {self.fragment_location} with shape {array.shape}"
                )
        else:
            var = np.array(array)

        return var
    
    def open(self):
        """Must be implemented by child class to properly open different file types."""
        raise NotImplementedError
    
class NetCDFFragmentWrapper(FragmentWrapper):
    def open(self):
        return netCDF4.Dataset(self.filename, mode='r')
