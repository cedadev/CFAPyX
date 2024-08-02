from CFAPyX.utils import OneOrMoreList
from CFAPyX.decoder import fragment_shapes, fragment_descriptors
from XarrayActive import DaskActiveArray

import dask.array as da
from dask.array.core import getter
from dask.base import tokenize
from dask.utils import SerializableLock, is_arraylike
from dask.array.reductions import numel

from itertools import product
import netCDF4
import numpy as np

class FragmentArrayWrapper():

    def __init__(self, decoded_cfa, ndim, shape, units, dtype, cfa_options=None, active_options=None):

        self.aggregated_data = decoded_cfa['aggregated_data']
        self.fragment_shape  = decoded_cfa['fragment_shape']

        fragments      = list(self.aggregated_data.keys())

        # Required parameters list
        self.ndim  = ndim
        self.shape = shape
        self.units = units
        self.dtype = dtype

        self.fragment_dims = tuple([i for i in range(self.ndim) if self.fragment_shape[i] != 1])

        if cfa_options:
            self.cfa_options = cfa_options
            self.apply_cfa_options(**cfa_options)

        self.active_options = active_options

        self.__array_function__ = self.get_array

    @property
    def active_options(self):
        """Property of the datastore that relates private option variables to the standard ``active_options`` parameter."""
        return {
            'use_active': self._use_active,
        }
    
    @active_options.setter
    def active_options(self, value):
        self._set_active_options(**value)

    def _set_active_options(self, use_active=False):
        self._use_active = use_active

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
                for f in self.aggregated_data.keys():
                    self.aggregated_data[f]['filename'] = self.aggregated_data[f]['filename'].replace(base, substitution)

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

        # For now expect to deal only with NetCDF Files

        fsizes_per_dim = fragment_shapes(
            shapes = None,
            array_shape = self.shape,
            fragment_dims = self.fragment_dims,
            fragment_shape = self.fragment_shape,
            aggregated_data = aggregated_data,
            ndim=self.ndim,
            dtype=np.dtype(np.float64)
        )
        dsk = {}

        for (
            u_indices,
            u_shape,
            f_indices,
            fragment_location,
            fragment_location,
            fragment_shape,
        ) in zip(*fragment_descriptors(fsizes_per_dim, self.fragment_dims, self.shape)):
            kwargs = aggregated_data[fragment_location].copy()
            kwargs.pop("location",None) # Update for CF-1.12

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
                fragment_getter,
                key,
                f_indices,
                False,
                getattr(fragment, "_lock", False) # Check version cf-python
            )

        if self._use_active:
            darr = DaskActiveArray(dsk, name[0], chunks=fsizes_per_dim, dtype=dtype)
        else:
            darr = da.Array(dsk, name[0], chunks=fsizes_per_dim, dtype=dtype)
        return darr
    
def fragment_getter(a, b, asarray=True, lock=None):
    if isinstance(b, tuple) and any(x is None for x in b):
        b2 = tuple(x for x in b if x is not None)
        b3 = tuple(
            None if x is None else slice(None, None)
            for x in b
        )
        return fragment_getter(a, b2, asarray=asarray, lock=lock)[b3]

    # Don't need the lock here anymore.
    a.set_extent(b)
    return a

def get_fragment_wrapper(format, **kwargs):
    if format == 'nc':
        return NetCDFFragmentWrapper(**kwargs)
    else:
        raise NotImplementedError(
            f"Fragment type '{format}' not supported"
        )

# Private class to hold all CFA-specific Active routines.
class _ActiveFragment:
    def __init__(self, *args, **kwargs):
        raise NotImplementedError

    def _standard_mean(self, axis=None, skipna=None, **kwargs):
        """
        Standard Mean routine matches the normal routine for dask, required at this
        stage if Active mean not available.
        """
        size = 1
        for i in axis:
            size *= self.shape[i]

        arr = np.array(self)
        if skipna:
            total = np.nanmean(arr, axis=axis, **kwargs) *size
        else:
            total = np.mean(arr, axis=axis, **kwargs) *size
        return {'n': self._numel(arr, axis=axis), 'total': total}

    def _numel(self, axis=None):
        if not axis:
            return self.size
        
        size = 1
        for i in axis:
            size *= self.shape[i]
        newshape = list(self.shape)
        newshape[axis] = 1

        return np.full(newshape, size)

    def active_mean(self, axis=None, skipna=None, **kwargs):
        """
        Use PyActiveStorage package functionality to perform mean of this Fragment.

        :param axis:        (int) The axis over which to perform the active_mean operation.

        :param skipna:      (bool) Skip NaN values when calculating the mean.

        :returns:       A ``duck array`` (numpy-like) with the reduced array or scalar value, 
                        as specified by the axis parameter.
        """
        try:
            from activestorage.active import Active
        except:
            # Unable to import Active package. Default to using normal mean.
            print("ActiveWarning: Unable to import active module - defaulting to standard method.")
            return self._standard_mean(axis=axis, skipna=skipna, **kwargs)
            
        active = Active(self.filename, self.address)
        active.method = "mean"
        extent = self.get_extent()

        if not axis is None:
            return {'n': self._numel(axis=axis), 'total': active[extent]}

        # Experimental Recursive requesting to get each 1D column along the axis being requested.
        range_recursives = []
        for dim in range(self.ndim):
            if dim != axis:
                range_recursives.append(range(extent[dim].start, extent[dim].stop+1))
            else:
                range_recursives.append(extent[dim])
        results = np.array(self._get_elements(active, range_recursives, hyperslab=[]))

        return {'n': self._numel(axis=axis), 'total': results}

    def _get_elements(self, active, recursives, hyperslab=[]):
        dimarray = []
        current = recursives[0]
        if not len(recursives) > 1:

            # Perform active slicing and meaning here.
            return active[hyperslab]

        if type(current) == slice:
            newslab = hyperslab + [current]
            dimarray.append(self._get_elements(active, recursives[1:], hyperslab=newslab))

        else:
            for i in current:
                newslab = hyperslab + [slice(i, i+1)]
                dimarray.append(self._get_elements(active, recursives[1:], hyperslab=newslab))

        return dimarray


class FragmentWrapper(_ActiveFragment):
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
        self.ndim     = len(shape)

        # Required by dask for thread-safety.
        self._lock    = SerializableLock()

        self.fragment_location   = fragment_location
        self.aggregated_units    = aggregated_units # cfunits conform method.
        self.aggregated_calendar = aggregated_calendar
        
    @property
    def shape(self):
        # Apply extent to shape.
        current_shape = []
        if not self.extent:
            return self._shape
        for e in self.extent:
            current_shape.append(int((e.stop - e.start)/e.step))
        return tuple(current_shape)

    @shape.setter
    def shape(self, value):
        self._shape = value

    def get_extent(self):
        if self.extent:
            return self.extent
        else:
            return (slice(0,self.shape[i]-1) for i in range(self.ndim))
    
    def set_extent(self, value):
        self.extent = value

    def __getitem__(self, selection):
        self.set_extent(selection)
        #ds = self.get_array(extent=tuple(selection))
        return self
    
    def __array__(self):
        return self.get_array()

    def get_array(self, extent=None):
        ds = self.open()
        # Use extent to just select the section and variable I'd actually like to deal with here.

        if not extent:
            extent = self.extent
        elif extent and self.extent:
            # New extent overrides previously given extent which should
            # just be stored and not yet applied.
            pass
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
                f"CFA fragment '{self.fragment_location}' does not contain "
                f"the variable '{varname}'."
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
    # Needs to handle the locking/unlocking here
    def open(self): # get lock/release lock
        return netCDF4.Dataset(self.filename, mode='r')

