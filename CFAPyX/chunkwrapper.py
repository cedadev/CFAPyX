__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

# Chunk wrapper is common to both CFAPyX and XarrayActive
VERSION = 1.0

import numpy as np
import netCDF4

from itertools import product
from dask.utils import SerializableLock

try:
    from XarrayActive import ActiveChunk
except:
    class ActiveChunk:
        pass

class ArrayLike:
    description = 'Container class for Array-Like behaviour'

    def __init__(self, shape, units=None, dtype=None):

        # Standard parameters to store for array-like behaviour
        self.shape = shape
        self.units = units
        self.dtype = dtype

    # Shape-based properties (Lazy loading means this may change in some cases)
    @property
    def size(self):
        return product(self.shape)
    
    @property
    def ndim(self):
        return len(self.shape)

class SuperLazyArrayLike(ArrayLike):

    description = "Container class for SuperLazy Array-Like behaviour"

    def __init__(self, shape, **kwargs):

        self._extent = [
            slice(0, i) for i in shape
        ]

        super().__init__(shape, **kwargs)

    def _combine_slices(self, newslice):

        if len(newslice) != len(self.shape):
            if hasattr(self, 'active'):
                # Mean has already been computed. Raise an error here 
                # since we should be down to dealing with numpy arrays now.
                raise ValueError(
                    "Active chain broken - mean has already been accomplished."
                )
            else:
                self._array = np.array(self)[newslice]
                return None
        
        def combine_sliced_dim(old, new, dim):

            ostart = old.start or 0
            ostop  = old.stop or self._shape[dim]
            ostep  = old.step or 1

            nstart = new.start or 0
            nstop  = new.stop or self._shape[dim]
            nstep  = new.step or 1

            start = ostart + ostep*nstart
            step  = ostep * nstep
            stop  = start + step * (nstop - nstart)
            return slice(start, stop, step)


        if not self._extent:
            return newslice
        else:
            extent = self._extent
            for dim in range(len(newslice)):
                extent[dim] = combine_sliced_dim(extent[dim], newslice[dim], dim)
            return extent
    
    def __getitem__(self, selection):
        """
        SuperLazy behaviour supported by saving index information to be applied when fetching the array.
        This is considered ``SuperLazy`` because Dask already loads dask subarrays lazily, but a further lazy
        approach is required when applying Active methods.
        """
        self._extent = self._combine_slices(selection)
        return self
    
    def get_extent(self):
        return self._extent

    @property
    def shape(self):
        # Apply extent to shape.
        current_shape = []
        if not self._extent:
            return self._shape
        for d, e in enumerate(self._extent):
            start = e.start or 0
            stop  = e.stop or self.shape[d]
            step  = e.step or 1
            current_shape.append(int((stop - start)/step))
        return tuple(current_shape)

    @shape.setter
    def shape(self, value):
        self._shape = value

class ChunkWrapper(ActiveChunk, SuperLazyArrayLike):
    description = "Wrapper class for individual chunk retrievals. May incorporate Active Storage routines as applicable methods called via Dask."

    def __init__(self,
                 filename,
                 address,
                 dtype=None,
                 units=None,
                 shape=None,
                 position=None,
                 extent=None,
                 format=None,
            ):
        
        """
        Wrapper object for the 'array' section of a fragment or chunk. Contains some metadata to ensure the
        correct fragment/chunk is selected, but generally just serves the array to dask when required.

        :param filename:    (str) The path to the data file from which this fragment or chunk is 
                            derived, if known. Not used in this class other than to support a ``.copy`` mechanism of
                            higher-level classes like ``CFAChunk``.
         
        :param address:     (str) The variable name/address within the underlying data file which this class represents.

        :param dtype:       (obj) The datatype of the values represented in this Array-like class.

        :param units:       (obj) The units of the values represented in this Array-like class

        :param shape:       (tuple) The shape of the array or subarray represented by this class.

        :param position:    (tuple) The position in ``index space`` into which this chunk belongs, this could be
                            ``fragment space`` or ``chunk space`` if using Active chunks.

        :param extent:      (tuple) Initial set of slices to apply to this chunk. Further slices may be applied which
                            are concatenated to the original slice defined here, if present. For fragments this will be
                            the extent of the whole array, but when using Active chunks the fragment copies may only
                            cover a subsection of the subarray.

        :param format:      (str) The format type of the underlying data file from which this fragment or chunk is 
                            derived, if known. Not used in this class other than to support a ``.copy`` mechanism of
                            higher-level classes like ``CFAChunk``.
        """
        
        self.__array_function__ = self.__array__

        self.filename = filename
        self.address  = address
        
        self.format   = format
        self.position = position

        self._extent  = extent
        self._lock    = SerializableLock()

        super().__init__(shape, dtype=dtype, units=units)

    def get_kwargs(self):
        """
        Return all the initial kwargs from instantiation, to support ``.copy()`` mechanisms by higher classes.
        """
        return {
            'dtype': self.dtype,
            'units': self.units,
            'shape': self.shape,
            'position': self.position,
            'extent': self.extent,
            'format': self.format
        }

    def _post_process_data(self, data):
        """
        Perform any post-processing steps on the data here.
        - unit correction
        - calendar correction
        """
        return data
    
    def __array__(self):
        """
        Retrieves the array of data for this variable chunk, casted into a Numpy array. Use of this method 
        breaks the ``Active chain`` by retrieving all the data before any methods can be applied.

        :returns:       A numpy array of the data for the correct variable with correctly applied selections
                        defined by the ``extent`` parameter.
        """
        ds = self.open()

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
        
        try:
            var = np.array(array[tuple(self._extent)])
        except IndexError:
            raise ValueError(
                f"Unable to select required 'extent' of {self.extent} "
                f"from fragment {self.position} with shape {array.shape}"
            )

        return self._post_process_data(var)
    
    def _try_openers(self, filename):
        """
        Attempt to open the dataset using all possible methods. Currently only NetCDF is supported.
        """
        for open in [
            self._open_netcdf,
            self._open_pp,
            self._open_um
        ]:
            try:
                ds = open(filename)
            except:
                pass
        if not ds:
            raise FileNotFoundError(
                'No file type provided and opening failed with all known types.'
            )
        return ds
    
    def _open_pp(self, filename):
        raise NotImplementedError

    def _open_um(self, filename):
        raise NotImplementedError

    def _open_netcdf(self, filename):
        return netCDF4.Dataset(filename, mode='r')

    def open(self):
        """
        Open the source file for this chunk to extract data. Multiple file locations may be provided
        for this object, in which case there is a priority for 'remote' sources first, followed by 
        'local' sources - otherwise the order is as given in the fragment array variable ``location``.
        """
 
        filenames = self.filename

        if type(filenames) == 'str':
            filenames = [filenames]

        # Tidy code - never going to be many filenames
        local  = [l for l in filenames if '://' not in l]
        remote = [r for r in filenames if '://' in r]

        # Prioritise remote options first if any are present.
        filenames = remote + local

        for filename in filenames:
            try:
                if not self.format:
                    # guess opening format.
                    return self._try_openers(filename)
                
                if self.format == 'nc':
                    return self._open_netcdf(filename)
                else:
                    raise ValueError(
                        f"Unrecognised format '{self.format}'"
                    )
            except ValueError as err:
                raise err
            except:
                pass

        raise FileNotFoundError(
            f'None of the location options for chunk "{self.position}" could be accessed.'
            f'Locations tried: {filenames}.'
        )
    
