__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

# Chunk wrapper is common to both CFAPyX and XarrayActive
VERSION = 1.2

import numpy as np
import netCDF4

from itertools import product
from copy import deepcopy
from dask.utils import SerializableLock

try:
    from XarrayActive import ActiveChunk
except:
    class ActiveChunk:
        pass

class ArrayLike:
    """
    Container class for Array-like behaviour
    """
    description = 'Container class for Array-Like behaviour'

    def __init__(self, shape, units=None, dtype=None, source_shape=None):

        # Standard parameters to store for array-like behaviour
        self.shape = shape
        self.units = units
        self.dtype = dtype

        if not source_shape: # First time instantiation - all other copies will not use this.
            source_shape = shape
        self._source_shape = source_shape

    # Shape-based properties (Lazy loading means this may change in some cases)
    @property
    def size(self):
        """
        Size property is derived from the current shape. In an ``ArrayLike`` 
        instance the shape is fixed, but other classes may alter the shape
        at runtime.
        """
        return product(self.shape)
    
    @property
    def ndim(self):
        """
        ndim property is derived from the current shape. In an ``ArrayLike`` 
        instance the shape is fixed, but other classes may alter the shape
        at runtime.
        """
        return len(self.shape)
    
    def copy(self, **kwargs):
        """
        Return a new basic ArrayLike instance. Ignores provided kwargs
        this class does not require, but other inheritors may."""
        return ArrayLike(
            self.shape,
            **self.get_kwargs()
        )
    
    def get_kwargs(self):
        """
        Get the kwargs provided to this class initially - for creating a copy."""
        return {
            'units':self.units,
            'dtype':self.dtype,
            'source_shape': self._source_shape
        }

class SuperLazyArrayLike(ArrayLike):
    """
    Container class for SuperLazy Array-Like behaviour. ``SuperLazy`` behaviour is
    defined as Lazy-Slicing behaviour for objects that are below the 'Dask Surface',
    i.e for object that serve as Dask Chunks."""

    description = "Container class for SuperLazy Array-Like behaviour"

    def __init__(self, shape, named_dims=None, **kwargs):
        """
        Adds an ``extent`` variable derived from the initial shape,
        this can be altered by performing slices, which are not applied 
        'Super-Lazily' to the data.
        """

        self._extent = [
            slice(0, i) for i in shape
        ]

        self.named_dims = named_dims

        super().__init__(shape, **kwargs)
 
    def __getitem__(self, selection):
        """
        SuperLazy behaviour supported by saving index information to be applied when fetching the array.
        This is considered ``SuperLazy`` because Dask already loads dask chunks lazily, but a further lazy
        approach is required when applying Active methods.
        """
        return self.copy(selection)
    
    @property
    def shape(self):
        """
        Apply the current ``extent`` slices to determine the current array shape,
        given all current slicing operations. This replaces shape as a simple
        attribute in ``ArrayLike``, on instantiation the ``_shape`` private attribute
        is defined, and subsequent attempts to retrieve the ``shape`` will depend on
        the current ``extent``.
        """
        current_shape = []
        if not self._extent:
            return self._shape
        for d, e in enumerate(self._extent):
            start = e.start or 0
            stop  = e.stop or self._shape[d]
            step  = e.step or 1
            current_shape.append(int((stop - start)/step))
        return tuple(current_shape)

    @shape.setter
    def shape(self, value):
        self._shape = value

    def _combine_slices(self, newslice):
        """
        Combine existing ``extent`` attribute with a new set of slices.

        :param newslice:        (tuple) A set of slices to apply to the data 
            'Super-Lazily', i.e the slices will be combined with existing information
            and applied later in the process.

        :returns:   The combined set of slices.
        """

        if len(newslice) != len(self.shape):
            if hasattr(self, 'active'):
                # Mean has already been computed. Raise an error here 
                # since we should be down to dealing with numpy arrays now.
                raise ValueError(
                    "Active chain broken - mean has already been accomplished."
                )
            
            else:
                raise ValueError(
                    "Compute chain broken - dimensions have been reduced already."
                )
        
        def combine_sliced_dim(old, new, dim):

            ostart = old.start or 0
            ostop  = old.stop or self._shape[dim]
            ostep  = old.step or 1

            osize = (ostop - ostart)/ostep

            nstart = new.start or 0
            nstop  = new.stop or self._shape[dim]
            nstep  = new.step or 1

            nsize = (nstop - nstart)/nstep

            if nsize > osize:
                raise IndexError(
                    f'Attempted to slice dimension "{dim}" with new slice "({nstart},{nstop},{nstep})'
                    f'but the dimension size is limited to {osize}.'
                )

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
   
    def get_extent(self):
        return self._extent

    def copy(self, newextent=None):
        """
        Create a new instance of this class with all attributes of the current instance, but
        with a new initial extent made by combining the current instance extent with the ``newextent``.
        Each ArrayLike class must overwrite this class to get the best performance with multiple 
        slicing operations.
        """
        kwargs = self.get_kwargs()
        if newextent:
            kwargs['extent'] = self._combine_slices(newextent)

        new_instance = SuperLazyArrayLike(
            self.shape,
            **kwargs
            )
        return new_instance
    
    def get_kwargs(self):
        return {
            'named_dims': self.named_dims
        } | super().get_kwargs()

class ArrayPartition(ActiveChunk, SuperLazyArrayLike):
    """
    Complete Array-like object with all proper methods for data retrieval.
    May include methods from ``XarrayActive.ActiveChunk`` if installed."""

    description = "Complete Array-like object with all proper methods for data retrieval."

    def __init__(self,
                 filename,
                 address,
                 shape=None,
                 position=None,
                 extent=None,
                 format=None,
                 **kwargs
            ):
        
        """
        Wrapper object for the 'array' section of a fragment or chunk. Contains some metadata to ensure the
        correct fragment/chunk is selected, but generally just serves the array to dask when required.

        :param filename:    (str) The path to the data file from which this fragment or chunk is 
                            derived, if known. Not used in this class other than to support a ``.copy`` mechanism of
                            higher-level classes like ``CFAPartition``.


         
        :param address:     (str) The variable name/address within the underlying data file which this class represents.

        :param dtype:       (obj) The datatype of the values represented in this Array-like class.

        :param units:       (obj) The units of the values represented in this Array-like class.

        :param shape:       (tuple) The shape of the partition represented by this class.



        :param position:    (tuple) The position in ``index space`` into which this chunk belongs, this could be
                            ``fragment space`` or ``chunk space`` if using Active chunks.

        :param extent:      (tuple) Initial set of slices to apply to this chunk. Further slices may be applied which
                            are concatenated to the original slice defined here, if present. For fragments this will be
                            the extent of the whole array, but when using Active chunks the fragment copies may only
                            cover a partition of the fragment.

        :param format:      (str) The format type of the underlying data file from which this fragment or chunk is 
                            derived, if known. Not used in this class other than to support a ``.copy`` mechanism of
                            higher-level classes like ``CFAPartition``.
        """
        
        self.__array_function__ = self.__array__

        self.filename = filename
        self.address  = address
        
        self.format   = format
        self.position = position

        self._lock    = SerializableLock()

        super().__init__(shape, **kwargs)

        if extent:
            # Apply a specific extent if given by the initiator
            self._extent  = extent
    
    def __array__(self, *args, **kwargs):
        """
        Retrieves the array of data for this variable chunk, casted into a Numpy array. Use of this method 
        breaks the ``Active chain`` by retrieving all the data before any methods can be applied.

        :returns:       A numpy array of the data for the correct variable with correctly applied selections
                        defined by the ``extent`` parameter.
        """

        # Unexplained xarray behaviour:
        # If using xarray indexing, __array__ should not have a positional 'dtype' option.
        # If casting DataArray to numpy, __array__ requires a positional 'dtype' option.
        dtype = None
        if args:
            dtype = args[0]

        if dtype != self.dtype:
            raise ValueError(
                'Requested datatype does not match this chunk'
            )

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
        
        if hasattr(array, 'units'):
            self.units = array.units
        
        if len(array.shape) != len(self._extent):
            self._correct_slice(array.dimensions)

        try:
            var = np.array(array[tuple(self._extent)])
        except IndexError:
            raise ValueError(
                f"Unable to select required 'extent' of {self.extent} "
                f"from fragment {self.position} with shape {array.shape}"
            )

        return self._post_process_data(var)
    
    def _correct_slice(self, array_dims):
        """
        Drop size-1 dimensions from the set of slices if there is an issue.

        :param array_dims:      (tuple) The set of named dimensions present in
            the source file. If there are fewer array_dims than the expected
            set in ``named_dims`` then this function is used to remove extra
            dimensions from the ``extent`` if possible.
        """
        extent = []
        for dim in range(len(self.named_dims)):
            named_dim = self.named_dims[dim]
            if named_dim in array_dims:
                extent.append(self._extent[dim])

            # named dim not present
            ext = self._extent[dim]
            
            start = ext.start or 0 
            stop  = ext.stop or self.shape[dim]
            step  = ext.step or 1

            if int(stop - start)/step > 1:
                raise ValueError(
                    f'Attempted to slice dimension "{named_dim}" using slice "{ext}" '
                    'but the requested dimension is not present'
                )
        self._extent = extent
            
    def _post_process_data(self, data):
        """
        Perform any post-processing steps on the data here.
        - unit correction
        - calendar correction
        """
        return data

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
        """
        Open a NetCDF file using the netCDF4 python package."""
        return netCDF4.Dataset(filename, mode='r')

    def get_kwargs(self):
        """
        Return all the initial kwargs from instantiation, to support ``.copy()`` mechanisms by higher classes.
        """
        return {
            'shape': self.shape,
            'position': self.position,
            'extent': self._extent,
            'format': self.format
        } | super().get_kwargs()
    
    def copy(self, newextent=None):
        """
        Create a new instance of this class with all attributes of the current instance, but
        with a new initial extent made by combining the current instance extent with the ``newextent``.
        Each ArrayLike class must overwrite this class to get the best performance with multiple 
        slicing operations.
        """
        kwargs = self.get_kwargs()
        if newextent:
            kwargs['extent'] = self._combine_slices(newextent)

        new_instance = ArrayPartition(
            self.filename,
            self.address,
            **kwargs,
            )
        return new_instance

    def open(self):
        """
        Open the source file for this chunk to extract data. Multiple file locations may be provided
        for this object, in which case there is a priority for 'remote' sources first, followed by 
        'local' sources - otherwise the order is as given in the fragment array variable ``location``.
        """
 
        filenames = self.filename

        if type(filenames) == str:
            filenames = [filenames]

        # Tidy code - never going to be many filenames
        local    = [l for l in filenames if '://' not in l]
        remote   = [r for r in filenames if '://' in r]
        relative = [d for d in filenames if d[:5] not in ('https','s3://','file:')]

        # Prioritise relative then remote options first if any are present.
        filenames = relative + remote + local

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
            f'None of the location options for chunk "{self.position}" could be accessed. '
            f'Locations tried: {filenames}.'
        )
    
