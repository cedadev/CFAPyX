__author__ = "Daniel Westwood"
__contact__ = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import logging
import math
from itertools import product
from typing import Union

import numpy as np
from dask.array.core import normalize_chunks

from cfapyx.filehandlers import NumpyDatasetHandler
from cfapyx.utils import logstream

logger = logging.getLogger(__name__)

logger.addHandler(logstream)
logger.propagate = False


class ArrayLike:
    """
    Container class for Array-like behaviour
    """

    description = "Container class for Array-Like behaviour"

    def __init__(
        self,
        shape: tuple,
        units: Union[str, None] = None,
        dtype: Union[np.dtype, None] = None,
        source_shape: Union[tuple, None] = None,
    ):

        # Standard parameters to store for array-like behaviour
        self.shape = shape
        self.dtype = dtype
        self.units = units

        if not source_shape:
            # First time instantiation - all other copies will not use this.
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
        this class does not require, but other inheritors may.
        """
        return ArrayLike(self.shape, **self.get_kwargs())

    def get_kwargs(self):
        """
        Get the kwargs provided to this class initially - for creating a copy.
        """
        return {
            "units": self.units,
            "dtype": self.dtype,
            "source_shape": self._source_shape,
        }


class SuperLazyArrayLike(ArrayLike):
    """
    Container class for SuperLazy Array-Like behaviour. ``SuperLazy`` behaviour is
    defined as Lazy-Slicing behaviour for objects that are below the 'Dask Surface',
    i.e for object that serve as Dask Chunks.
    """

    description = "Container class for SuperLazy Array-Like behaviour"

    def __init__(self, shape, named_dims=None, **kwargs):
        """
        Adds an ``extent`` variable derived from the initial shape,
        this can be altered by performing slices, which are not applied
        'Super-Lazily' to the data.
        """

        self._extent = [slice(0, i) for i in shape]

        self.named_dims = named_dims

        super().__init__(shape, **kwargs)

    def __getitem__(self, selection):
        """
        SuperLazy behaviour supported by saving index information to be applied when
        fetching the array. This is considered ``SuperLazy`` because Dask already
        loads dask chunks lazily, but a further lazy approach is required when
        applying Active methods.
        """
        return self.copy(extent=selection)

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
            if isinstance(e, int):
                continue

            start = e.start or 0
            stop = e.stop or self._shape[d]
            step = e.step or 1
            current_shape.append(int((stop - start) / step))
        return tuple(current_shape)

    @shape.setter
    def shape(self, value):
        self._shape = value

    def get_extent(self):
        """
        Method for getting private variable `_extent` outside of this class.
        """
        return tuple(self._smooth_extent(self._extent))

    def set_extent(self, extent):
        """
        Method for directly setting the `_extent` for this class, once it has been
        'smoothed'."""
        self._extent = self._smooth_extent(extent)

    def _smooth_extent(self, extent):
        """
        Replace values of None within each provided slice of the extent with integer
        values derived from the current shape.
        """
        if len(extent) != self.ndim:
            raise ValueError("Direct assignment of truncated extent is not supported.")

        smoothextent = []
        for x, ext in enumerate(extent):
            if isinstance(ext, int):
                smoothextent.append(ext)
                continue
            start = ext.start or 0
            stop = ext.stop or self.shape[x]
            step = ext.step or 1
            smoothextent.append(slice(start, stop, step))

        return smoothextent

    def copy(self, extent=None):
        """
        Create a new instance of this class with all attributes of the
        current instance, but with a new initial extent made by combining
        the current instance extent with the ``newextent``. Each ArrayLike
        class must overwrite this class to get the best performance with
        multiple slicing operations.
        """
        kwargs = self.get_kwargs()
        if extent:
            kwargs["extent"] = combine_slices(
                self.shape, list(self.get_extent()), extent
            )

        new_instance = SuperLazyArrayLike(self.shape, **kwargs)
        return new_instance

    def get_kwargs(self):
        return {"named_dims": self.named_dims} | super().get_kwargs()


class ArrayPartition(SuperLazyArrayLike):
    """
    Complete Array-like object with all proper methods for
    data retrieval.

    Overridden in Active-enabled packages."""

    description = (
        "Complete Array-like object with all proper methods for data retrieval."
    )

    def __init__(
        self,
        filename: str,
        address: str,
        shape: Union[tuple, None] = None,
        position: Union[tuple, None] = None,
        extent: Union[tuple, None] = None,
        format: Union[str, None] = None,
        mask_and_scale: bool = False,
        **kwargs,
    ):
        """
        Wrapper object for the 'array' section of a fragment or chunk. Contains
        some metadata to ensure the  correct fragment/chunk is selected, but
        generally just serves the array to dask when required.

        :param filename:    (str) The path to the data file from which this fragment
            or chunk is derived, if known. Not used in this class other than to support
            a ``.copy`` mechanism of higher-level classes like ``CFAPartition``.



        :param address:     (str) The variable name/address within the underlying
            data file which this class represents.

        :param dtype:       (obj) The datatype of the values represented in this
            Array-like class.

        :param units:       (obj) The units of the values represented in this
            Array-like class.

        :param shape:       (tuple) The shape of the partition represented by this
            class.



        :param position:    (tuple) The position in ``index space`` into which this
            chunk belongs, this could be ``fragment space`` or ``chunk space`` if
            using Active chunks.

        :param extent:      (tuple) Initial set of slices to apply to this chunk.
            Further slices may be applied which are concatenated to the original
            slice defined here, if present. For fragments this will be the extent of
            the whole array, but when using Active chunks the fragment copies may only
            cover a partition of the fragment.

        :param format:      (str) The format type of the underlying data file from
            which this fragment or chunk is derived, if known. Not used in this class
            other than to support a ``.copy`` mechanism of higher-level classes like
            ``CFAPartition``.
        """

        # self.__array_function__ = self.__array__

        self.filename = filename
        self.address = address
        self.format = format
        self.position = position

        self.mask_and_scale = mask_and_scale

        if shape is None:
            # Identify shape
            shape = tuple(self._get_array().shape)

        super().__init__(shape, **kwargs)

        if extent:
            # Apply a specific extent if given by the initiator
            self.set_extent(extent)

    def __array__(self, *args, **kwargs) -> np.ndarray:
        """
        Retrieves the array of data for this variable chunk, casted into a Numpy
        array. Use of this method breaks the ``Active chain`` by retrieving all
        the data before any methods can be applied.

        :returns:       A numpy array of the data for the correct variable with
            correctly applied selections defined by the ``extent`` parameter.
        """

        # Gets a numpy array handler object - no longer passing hdf filehandlers around
        arrayf = self.open()

        if self.units is None:
            # Partition Inherits array units
            self.units = arrayf.units

        if self.units != arrayf.units:
            logger.info(
                "Units do not match from CFA representation to fragment file"
                " - will be handled by Unit conversion"
            )

        try:
            # Apply extent to array object.
            var = np.array(arrayf)

            # Disable auto mask/scaling - implemented by Xarray
            if self.format == "nc" and self.mask_and_scale:
                var = self._perform_mask_and_scale(var)

        except IndexError:
            raise ValueError(
                f"Unable to select required 'extent' of {arrayf.extent} "
                f"from fragment {self.position} with shape {arrayf.shape}"
            )

        return self._post_process_data(var)

    def _perform_mask_and_scale(self, array: np.ndarray) -> np.ndarray:
        """
        Mask and scaling applied to the array selection.
        """

        scale = array.attrs.get("scale_factor")
        offset = array.attrs.get("offset")

        if scale and offset:
            return array * scale + offset

        raise ValueError(
            f'Unable to mask and scale with "scale_factor:{scale}" "offset:{offset}"'
        )

    def _post_process_data(self, data: np.ndarray):
        """
        Perform any post-processing steps on the data here.

        Method to be overriden by inherrited classes (CFAPyX.CFAPartition
        and XarrayActive.ActivePartition)
        """
        return data

    def _try_openers(self, filename: str, remote: bool = False) -> NumpyDatasetHandler:
        """
        Attempt to open the dataset using all possible methods.

        Currently only NetCDF is supported.
        """
        for open in [self._open_netcdf, self._open_pp, self._open_um]:
            try:
                ds = open(filename, remote=remote)
            except Exception:
                pass
        if not ds:
            raise FileNotFoundError(
                "No file type provided and opening failed with all known types."
            )
        return ds

    def _open_pp(self, filename: str, remote: bool = False):
        raise NotImplementedError

    def _open_um(self, filename: str, remote: bool = False):
        raise NotImplementedError

    def _open_netcdf(self, filename: str, remote: bool = False) -> NumpyDatasetHandler:
        """
        Open a NetCDF file using the pyfive/netCDF4 python packages."""

        return NumpyDatasetHandler(
            filename,
            self.address,
            dtype=self.dtype,
            named_dims=self.named_dims,
            extent=self._extent,
            remote=remote,
        )

    def get_kwargs(self):
        """
        Return all the initial kwargs from instantiation, to support ``.copy()``
        mechanisms by higher classes.
        """
        return {
            "shape": self.shape,
            "position": self.position,
            "extent": self._extent,
            "format": self.format,
        } | super().get_kwargs()

    def copy(self, extent: Union[tuple, None] = None):
        """
        Create a new instance of this class with all attributes of the current
        instance.

        The copy has annew initial extent made by combining the current instance
        extent with the ``newextent``.
        Each ArrayLike class must overwrite this class to get the best performance
        with multiple slicing operations.
        """
        kwargs = self.get_kwargs()
        if extent:
            kwargs["extent"] = combine_slices(
                self.shape, list(self.get_extent()), extent
            )

        new_instance = ArrayPartition(
            self.filename,
            self.address,
            **kwargs,
        )
        return new_instance

    def open(self) -> NumpyDatasetHandler:
        """
        Open the source file for this chunk to extract data.

        Multiple file locations may be provided for this object, in which case there
        is a priority for 'remote' sources first, followed by 'local' sources -
        otherwise the order is as given in the fragment array variable ``location``.
        """

        filenames = self.filename

        if isinstance(filenames, str):
            filenames = [filenames]

        # Tidy code - never going to be many filenames
        local = [loc for loc in filenames if "://" not in loc]
        remote = [rem for rem in filenames if "://" in rem]
        relative = [
            rel
            for rel in filenames
            if any([prefix in rel for prefix in ("https", "s3://", "file:")])
        ]

        # Prioritise relative then remote options first if any are present.
        filenames = relative + remote + local

        err = False
        for filename in filenames:
            is_remote = "http" in filename
            try:
                if not self.format:
                    # guess opening format.
                    return self._try_openers(filename, is_remote)

                if self.format == "nc":
                    return self._open_netcdf(filename, is_remote)
                else:
                    raise ValueError(f"Unrecognised format '{self.format}'")
            except ValueError:
                raise
            except Exception as e:
                err = e
        if err:
            raise err

        raise FileNotFoundError(
            f'None of the location options for chunk "{self.position}" could be '
            f"accessed. Locations tried: {filenames}."
        )


def _identical_extents(old: slice, new: slice, dshape: int):
    """
    Determine if two slices match precisely.

    :param old:     (slice) Current slice applied to the dimension.

    :param new:     (slice) New slice to be combined with the old slice.

    :param dshape:     (int) Total size of the given dimension.
    """

    if isinstance(new, int):
        new = slice(new, new + 1)

    ostart = old.start or 0
    ostop = old.stop or dshape
    ostep = old.step or 1

    nstart = new.start or 0
    nstop = new.stop or dshape
    nstep = new.step or 1

    return (ostart == nstart) and (ostop == nstop) and (ostep == nstep)


def get_chunk_space(chunk_shape: tuple, shape: tuple) -> tuple:
    """
    Derive the chunk space in each dimension.

    Calculated from the ratio between the chunk shape and array shape in
    each dimension. Chunk space is the number of chunks in each dimension which is
    referred to as a ``space`` because it effectively represents the lengths of the
    each dimension in 'chunk space' rather than any particular chunk coordinate.

    Example:
        50 chunks across the time dimension of 1000 values which is represented by 8
        fragments. Chunk space representation is (50,) and the chunk shape is (20,).

        Each chunk is served by at most 2 fragments, where each chunk is described
        using a MultiFragmentWrapper object which appropriately sets the extents
        of each Fragment object. The Fragments cover 125 values each:

        Chunk 0 served by Fragment 0 slice(0,20)
        Chunk 1 served by Fragment 0 slice(20,40)
        ...
        Chunk 6 served by Fragment 0 slice(120,None) and Fragment 1 slice(0,15)
        ...
        and so on.

    :param chunk_shape:     (tuple) The shape of each chunk in array space.

    :param shape:           (tuple) The total array shape in array space -
        alternatively the total array space size.

    """

    space = tuple([math.ceil(i / j) for i, j in zip(shape, chunk_shape)])
    return space


def get_chunk_shape(
    chunks: dict, shape: tuple, dims: tuple, chunk_limits: bool = True
) -> tuple:
    """
    Calculate the chunk shape from the user-provided ``chunks`` parameter,
    the array shape and named dimensions, and apply chunk limits if enabled.

    :param chunks:  (dict) The user specified chunks, which match the usual dask
        chunks from xr.open_dataset, except these come from the ``cfa_options``.

    :param shape:   (tuple) The array shape of the data array to be chunked.

    :param dims:    (tuple) The names of each dimension to match to the ``chunks``
        provided.

    :param chunk_limits (bool) Option to disable, chunk limits will prevent chunking
        to beyond a useful chunk size which is likely to be much less than the memory
        chunk size of the source files, in which case there would be a lot of wasted
        data retrieval.

    :returns:   A tuple of the shape of each chunk in ``array space`` for each
        dimension.
    """
    chunk_shape = [i for i in shape]

    for dim in chunks.keys():
        idim = None
        for x, d in enumerate(dims):
            if d == dim:
                idim = x

        if idim is None:
            raise ValueError(
                f"Requested chunking across dimension '{dim}'"
                f"but only '{dims}' present in the dataset"
            )

        # Apply chunk limits unless disabled.
        min_size = int(shape[idim] / np.prod(shape))
        if chunk_limits:
            min_size = int(min_size * 2e6)
            # 2M data points is the smallest total size allowed.

        chunk_size = chunks[dim]
        chunk_shape[idim] = max(chunk_size, min_size)

    return tuple(chunk_shape)


def get_chunk_positions(chunk_space: tuple) -> list[tuple]:
    """
    Get the list of chunk positions in ``chunk space``.

    Given the size of the space, list all possible positions
    within the space. A space of ``(1,1)`` has a single possible
    position; ``(0,0)``, whereas a space of ``(2,2)`` has four
    positions: ``(0,0)``,``(0,1)``,``(1,0)`` and ``(1,1)``.

    :param chunk_space:     (tuple) The total size of the space in
        all dimensions
    """
    origin = [0 for i in chunk_space]

    positions = [
        coord
        for coord in product(*[range(r[0], r[1]) for r in zip(origin, chunk_space)])
    ]

    return positions


def get_chunk_extent(position: tuple, shape: tuple, chunk_space: tuple) -> tuple:
    """
    Get the extent of a particular chunk within the space.

    Given its position, the array shape and the extent of the
    chunk space, find the extent of a particular chunk.

    :param position:    (tuple) The position of the chunk in chunk space.

    :param shape:       (tuple) The total array shape for the whole chunk space.

    :param chunk_space: (tuple) The size of the chunk space (number of chunks
        in each dimension).
    """
    extent = []
    for dim in range(len(position)):
        pos_index = position[dim]
        shape_size = shape[dim]
        space_size = chunk_space[dim]

        conversion = shape_size / space_size

        ext = slice(int(pos_index * conversion), int((pos_index + 1) * conversion))
        extent.append(ext)

    return extent


def get_dask_chunks(
    array_space: tuple,
    fragment_space: tuple,
    extent: tuple,
    dtype: np.dtype,
    explicit_shapes: Union[tuple, None] = None,
) -> tuple:
    """
    Define the `chunks` array passed to Dask when creating a Dask Array. This is an
    array of fragment sizes per dimension for each of the relevant dimensions.
    Copied from cf-python version 3.14.0 onwards.

    :param array_space:     (tuple) The shape of the array in ``array space``.

    :param fragment_space:  (tuple) The shape of the array in ``fragment space``.

    :param extent:          (dict) The global extent of each fragment - where it
        fits into the total array for this variable (in array space).

    :param dtype:           (obj) The datatype for this variable.

    :param explicit_shapes:     (tuple) Set of shapes to apply to the fragments -
        currently not implemented outside this function.

    :returns:       A tuple of the chunk sizes along each dimension.
    """

    from numbers import Number

    from dask.array.core import normalize_chunks

    ndim = len(array_space)
    fsizes_per_dim, fragmented_dim_indices = [], []

    for dim, n_fragments in enumerate(fragment_space):
        if n_fragments != 1:
            fsizes = []
            index = [0] * ndim
            for n in range(n_fragments):
                index[dim] = n
                ext = extent[tuple(index)][dim]
                fragment_size = ext.stop - ext.start
                fsizes.append(fragment_size)

            fsizes_per_dim.append(tuple(fsizes))
            fragmented_dim_indices.append(dim)
        else:
            # This aggregated dimension is spanned by exactly one
            # fragment. Store None, for now, in the expectation
            # that it will get overwritten.
            fsizes_per_dim.append(None)

    ## Handle explicit shapes for the fragments.

    if isinstance(explicit_shapes, (str, Number)) or explicit_shapes is None:
        # For each dimension, use fs or explicit_shapes if the dimension is
        # fragmented or not respectively.
        fsizes_per_dim = [
            fs if i in fragmented_dim_indices else explicit_shapes
            for i, fs in enumerate(fsizes_per_dim)
        ]
    elif isinstance(explicit_shapes, dict):
        fsizes_per_dim = [
            fsizes_per_dim[i]
            if i in fragmented_dim_indices
            else explicit_shapes.get(i, "auto")
            for i, fs in enumerate(fsizes_per_dim)
        ]
    else:
        # explicit_shapes is a sequence
        if len(explicit_shapes) != ndim:
            raise ValueError(
                f"Wrong number of 'explicit_shapes' elements in {explicit_shapes}: "
                f"Got {len(explicit_shapes)}, expected {ndim}"
            )

        fsizes_per_dim = [
            fs if i in fragmented_dim_indices else explicit_shapes[i]
            for i, fs in enumerate(fsizes_per_dim)
        ]

    return normalize_chunks(fsizes_per_dim, shape=array_space, dtype=dtype)


def combine_sliced_dim(old: slice, new: slice, shape: tuple, dim: int) -> slice:
    """
    Combine slices for a given dimension.

    :param old:     (slice) Current slice applied to the dimension.

    :param new:     (slice) New slice to be combined with the old slice.

    :param shape:   (tuple) The shape of the native array, before
        application of any slices.

    :param dim:     (int) Integer index of the dimension in the array.
    """

    if isinstance(new, int):
        new = slice(new, new + 1)

    ostart = old.start or 0
    ostop = old.stop or shape[dim]
    ostep = old.step or 1

    osize = (ostop - ostart) / ostep

    nstart = new.start or 0
    nstop = new.stop or shape[dim]
    nstep = new.step or 1

    nsize = (nstop - nstart) / nstep

    if nsize > osize:
        raise IndexError(
            f'Attempted to slice dimension "{dim}" with new slice '
            f'"({nstart},{nstop},{nstep}) '
            f"but the dimension size is limited to {osize}."
        )

    start = ostart + ostep * nstart
    step = ostep * nstep
    stop = start + step * (nstop - nstart)

    return slice(start, stop, step)


def combine_slices(
    shape: tuple[int], extent: tuple[slice], newslice: tuple[slice, int]
) -> tuple[slice]:
    """
    Combine existing ``extent`` attribute with a new set of slices.

    :param shape:       (tuple) The native source shape of the array.

    :param extent:      (tuple) The current set of slices recorded
        for this data selection but not yet applied.

    :param newslice:    (tuple) A set of slices to apply to the data
        'Super-Lazily', i.e the slices will be combined with existing information
        and applied later in the process.

    :returns:   The combined set of slices.
    """

    if len(newslice) != len(shape):
        raise ValueError("Compute chain broken - dimensions have been reduced already.")

    if not extent:
        return newslice
    else:
        for dim in range(len(newslice)):
            if isinstance(newslice[dim], int):
                extent[dim] = getattr(extent[dim], "start", None) or 0 + newslice[
                    dim
                ] * (getattr(extent[dim], "step", None) or 1)

            elif not _identical_extents(extent[dim], newslice[dim], shape[dim]):
                extent[dim] = combine_sliced_dim(extent[dim], newslice[dim], shape, dim)

        return extent


def normalize_partition_chunks(
    chunks: dict, shape: tuple[int], dtype: np.dtype, named_dims: list
):
    """
    Prepare information for dask to normalise chunks.

    :param chunks:      (dict) Dictionary of named dimensions and their number
        of partitions across different sources.

    :param shape:       (tuple) The total shape of the array in all dimensions.

    :param dtype:       (np.dtype) The data type for this array.

    :param named_dims:  (list) The set of named dimensions for this array.
    """

    chunk_values = []

    for nd in named_dims:
        if nd not in chunks.keys():
            chunk_values.append("auto")
            continue
        try:
            chunk_values.append(int(chunks[nd]))
        except ValueError:
            chunk_values.append(chunks[nd])

    # Construct chunk values for each named dimension

    return normalize_chunks(chunk_values, shape, dtype=dtype)
