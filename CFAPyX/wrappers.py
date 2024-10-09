__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

from arraypartition import (
    ArrayPartition, 
    ArrayLike,
    get_chunk_shape,
    get_chunk_space,
    get_chunk_positions,
    get_chunk_extent,
    get_dask_chunks,
    combine_slices,
    normalize_partition_chunks
)

import dask.array as da
from dask.array.core import getter
from dask.base import tokenize
from dask.utils import SerializableLock, is_arraylike
from dask.array.reductions import numel

from itertools import product
import math
import numpy as np

try:
    from XarrayActive import ActiveOptionsContainer
except:
    class ActiveOptionsContainer:
        pass

import logging

logger = logging.getLogger(__name__)

class CFAOptionsMixin:
    """
    Simple container for CFA options properties.
    """

    __slots__ = (
        'chunks',
        '_chunk_limits',
        '_substitutions',
        '_decode_cfa'
    )

    @property
    def cfa_options(self):
        """
        Relates private option variables to the ``cfa_options`` parameter of the backend.
        """

        return {
            'substitutions': self._substitutions,
            'decode_cfa': self._decode_cfa,
            'chunks': self.chunks,
            'chunk_limits':self._chunk_limits
        }

    @cfa_options.setter
    def cfa_options(self, value):
        self._set_cfa_options(**value)

    def _set_cfa_options(
            self,
            substitutions=None,
            decode_cfa=None,
            chunks={},
            chunk_limits=None,
            use_active=False,
            **kwargs):
        """
        Sets the private variables referred by the ``cfa_options`` parameter to the backend. 
        Ignores additional kwargs.
        """

        self._substitutions = substitutions
        self._decode_cfa    = decode_cfa
        self._chunk_limits  = chunk_limits
        self.chunks         = chunks
        self.use_active     = use_active

class FragmentArrayWrapper(ArrayLike, CFAOptionsMixin, ActiveOptionsContainer):
    """
    FragmentArrayWrapper behaves like an Array that can be indexed or referenced to 
    return a Dask-like array object. This class is essentially a constructor for the 
    partitions that feed into the returned Dask-like array into Xarray.
    """
    
    description = 'Wrapper-class for the array of fragment objects'

    def __init__(
            self, 
            fragment_info, 
            fragment_space, 
            shape, 
            units, 
            dtype, 
            cfa_options={}, 
            named_dims=None
        ):
        """
        Initialisation method for the FragmentArrayWrapper class

        :param fragment_info:   (dict) The information relating to each fragment with the 
            fragment coordinates in ``fragment space`` as the key. Each 
            fragment is described by the following:
            - ``shape`` - The shape of the fragment in ``array space``.
            - ``location`` - The file from which this fragment originates.
            - ``address`` - The variable and group name relating to this variable.
            - ``extent`` - The slice object to apply to the fragment on retrieval (usually get
            the whole array)
            - ``global_extent`` - The slice object that equates to a particular fragment out
            of the whole array (in ``array space``).
                        
        :param fragment_space:      (tuple) The coordinate system that refers to individual 
                                    fragments. Each coordinate eg. i, j, k refers to the 
                                    number of fragments in each of the associated dimensions.

        :param shape:               (tuple) The total shape of the array in ``array space``

        :param units:       (obj) The units of the values represented in this Array-like class.

        :param dtype:       (obj) The datatype of the values represented in this Array-like class.

        :param cfa_options:     (dict) The set of options defining some specific decoding behaviour.

        :param named_dims:  (list) The set of dimension names that apply to this Array object.

        :returns: None
        """

        self.fragment_info    = fragment_info
        self.fragment_space   = fragment_space
        self.named_dims       = named_dims

        super().__init__(shape, dtype=dtype, units=units)

        # Set internal private variables
        self.cfa_options    = cfa_options

        self._apply_substitutions()

        self.__array_function__ = self.__array__

    def __getitem__(self, selection):
        """
        Non-lazy retrieval of the dask array when this object is indexed.
        """
        arr = self.__array__()
        return arr[tuple(selection)]
    
    def __array__(self):
        """
        Non-lazy array construction, this will occur as soon as the instance is ``indexed`` 
        or any other ``array`` behaviour is attempted. Construction of a Dask-like array 
        occurs here based on the decoded fragment info and any other specified settings.
        """

        array_name = (f"{self.__class__.__name__}-{tokenize(self)}",)

        dtype = self.dtype
        units = self.units

        calendar = None # Fix later

        # Fragment info dict at this point
        fragment_info = self.fragment_info

        # For now expect to deal only with NetCDF Files

        # dict of array-like objects to pass to the dask Array constructor.
        fragments = {}

        for pos in self.fragment_info.keys():

            fragment_shape    = self.fragment_info[pos]['shape']
            fragment_position = pos
            global_extent     = self.fragment_info[pos]['global_extent']
            extent            = self.fragment_info[pos]['extent']

            fragment_format   = 'nc'

            if 'fill_value' in self.fragment_info[pos]:
                filename = None
                address = None
                # Extra handling required for this condition.
            else:
                filename   = self.fragment_info[pos]['location']
                address    = self.fragment_info[pos]['address']
                
                # Wrong extent type for both scenarios but keep as a different label for 
                # dask chunking.

            fragment = CFAPartition(
                filename,
                address,
                dtype=dtype,
                extent=extent,
                shape=fragment_shape,
                position=fragment_position,
                aggregated_units=units,
                aggregated_calendar=calendar,
                format=fragment_format,
                named_dims=self.named_dims,
                global_extent=global_extent
            )

            fragments[pos] = fragment
        
        if not self.chunks:
            dsk = self._assemble_dsk_dict(fragments, array_name)

            global_extent = {
                k: fragment_info[k]["global_extent"] for k in fragment_info.keys()
            }

            dask_chunks = get_dask_chunks(
                self.shape,
                self.fragment_space,
                extent=global_extent,
                dtype=self.dtype,
                explicit_shapes=None
            )

        else:
            dask_chunks, partitions = self._create_partitions(fragments)
                
            dsk = self._assemble_dsk_dict(partitions, array_name)

        darr = self._assemble_array(dsk, array_name[0], dask_chunks)
        return darr

    def _optimise_chunks(self):
        """
        Replace the keyword ``optimised`` in the provided chunks with a chunk size for 
        the specified dimension that will be most optimised. The optimal chunk sizes are such 
        that the number of chunks is close to a power of 2."""

        auto_chunks = {}
        for c in self.chunks:
            if self.chunks[c] == 'optimised':
                auto_chunks[c] = 'auto'
            else:
                auto_chunks[c] = self.chunks[c]

        nchunks = normalize_partition_chunks(
            auto_chunks,
            self.shape,
            self.dtype,
            self.named_dims
        )

        # For each 'optimised' dimension, take the log2 of the number of chunks (len)
        # and round to the nearest integer. Divide the array length by 2^(this number) and 
        # round again to give the optimised chunk size for that dimension.

        for x, nd in enumerate(self.named_dims):
            if nd not in self.chunks:
                continue

            if self.chunks[nd] == 'optimised':
                nchunk = len(nchunks[x])

                power  = round(math.log2(nchunk))
                opsize = round(self.shape[x]/2**power)

                self.chunks[nd] = opsize

    def _create_partitions(self, fragments):
        """
        Creates a partition structure that falls along the existing fragment boundaries.
        This is done by simply chunking each fragment given the user provided chunks, rather 
        than the whole array, because user provided chunk sizes apply to each fragment equally.

        :param fragments:       (dict) The set of fragment objects (CFAPartitions) in ``fragment space``
            before any chunking is applied.

        :returns:   The set of dask chunks to provide to dask when building the array and the corresponding
            set of copied fragment objects for each partition.
        """
        if 'optimised' in self.chunks.items():
            # Running on standard dask chunking mode.
            self._optimise_chunks()
            
        dask_chunks       = [[] for i in range(self.ndim)]
        fragment_coverage = [[] for i in range(self.ndim)]
        for dim in range(self.ndim):
            for x in range(self.fragment_space[dim]):
                # Position eg. 0, 0, X
                position = [0 for i in range(self.ndim)]
                position[dim] = x 

                fragment = fragments[tuple(position)]

                dchunks = normalize_partition_chunks( # Needs the chunks
                    self.chunks,
                    fragment.shape,
                    dtype=self.dtype,
                    named_dims=self.named_dims
                )

                dask_chunks[dim] += dchunks[dim]
                fragment_coverage[dim].append(len(dchunks[dim]))

        def outer_cumsum(array):
            cumsum = np.cumsum(array)
            cumsum = np.append(cumsum, 0)
            return np.roll(cumsum,1)
        
        def global_combine(internal, external):
            local = []
            for dim in range(len(internal)):
                start = internal[dim].start - external[dim].start
                stop  = internal[dim].stop  - external[dim].start
                local.append(slice(start,stop))
            return local

        fragment_cumul  = [outer_cumsum(d) for d in fragment_coverage]
        partition_cumul = [outer_cumsum(p) for p in dask_chunks]
        partition_space = [len(d) for d in dask_chunks]

        partitions = {}
        partition_coords = get_chunk_positions(partition_space)
        for coord in partition_coords:
            fragment_coord = []
            internal = []
            for dim, c in enumerate(coord):
                cumulative = fragment_cumul[dim]

                if c < cumulative[0]:
                    cumul = cumulative[0]
                else:
                    cumul = max(filter(lambda l: l <= c, cumulative))

                fc = np.where(cumulative == cumul)[0]
                fragment_coord.append(int(fc))

                ext = slice(
                    partition_cumul[dim][c],
                    partition_cumul[dim][c+1]
                )
                internal.append(ext)
            
            # Currently applying GLOBAl extent not internal extent to each fragment.

            source   = fragments[tuple(fragment_coord)]
            external = source.global_extent
            extent   = global_combine(internal, external)

            partitions[coord] = source.copy(extent=extent)

        return dask_chunks, partitions

    def _assemble_dsk_dict(self, partitions, array_name):
        """
        Assemble the base ``dsk`` task dependency graph which includes the fragment objects 
        plus the method to index each object (with locking).

        :param partitions:   (dict) The set of partition objects (CFAPartition) with 
            their positions in the relevant ``space``.

        :returns:       A task dependency graph with all the partitions included to use 
            when constructing the dask array.
        """

        dsk = {}
        for part_position in partitions.keys():
            part = partitions[part_position]

            p_identifier = f"{part.__class__.__name__}-{tokenize(part)}"
            dsk[p_identifier] = part
            dsk[array_name + part_position] = (
                getter, 
                p_identifier,
                part.get_extent(),
                False,
                getattr(part, "_lock", False) # Check version cf-python
            )
        return dsk

    def _apply_substitutions(self):
        """
        Perform substitutions for this fragment array.
        """
        if not self._substitutions:
            return

        if type(self._substitutions) != list:
            self._substitutions = [self._substitutions]

        for s in self._substitutions:
            base, substitution = s.split(':')
            for f in self.fragment_info.keys():

                if isinstance(self.fragment_info[f]['location'], str):
                    self.fragment_info[f]['location'] = self.fragment_info[f]['location'].replace(base, substitution)
                else:
                    for finfo in self.fragment_info[f]['location']:
                        finfo = finfo.replace(base, substitution)
                
    def _assemble_array(self, dsk, array_name, dask_chunks):

        """
        Assemble the dask/dask-like array for this FragmentArrayWrapper from the 
        assembled ``dsk`` dict and set of dask chunks. Also provides an array name
        for the dask tree to register.
        """

        meta = da.empty(self.shape, dtype=self.dtype)
        if not hasattr(self, 'use_active'):
            darr = da.Array(dsk, array_name, chunks=dask_chunks, dtype=self.dtype, meta=meta)
            return darr

        if not self.use_active:
            darr = da.Array(dsk, array_name, chunks=dask_chunks, dtype=self.dtype, meta=meta)
            return darr
        try:
            from XarrayActive import DaskActiveArray

            darr = DaskActiveArray(dsk, array_name, chunks=dask_chunks, dtype=self.dtype, meta=meta)
        except ImportError:
            raise ImportError(
                '"DaskActiveArray" from XarrayActive failed to import - please ensure '
                'you have the XarrayActive package installed.'
            )
        return darr
            
class CFAPartition(ArrayPartition):
    """
    Wrapper object for a CFA Partition, extends the basic ArrayPartition with CFA-specific 
    methods.
    """
  
    description = 'Wrapper object for a CFA Partition (Fragment or Chunk)'


    def __init__(self,
                 filename,
                 address,
                 aggregated_units=None,
                 aggregated_calendar=None,
                 global_extent=None,
                 **kwargs
            ):
        
        """
        Wrapper object for the 'array' section of a fragment. Contains some metadata 
        to ensure the correct fragment is selected, but generally just serves the 
        fragment array to dask when required.

        :param filename:        (str) The path to a Fragment file from which this 
            partition object will access data from. The partition may represent 
            all or a subset of the data from the Fragment file.

        :param address:         (str) The address of the data variable within the 
            Fragment file, may include a group hierarchy structure.

        :param aggregated_units:    (obj) The expected units for the received data.
            If the units of the received data are not equal to the ``aggregated_units``
            then the data is 'post-processed' using the cfunits ``conform`` function.

        :param aggregated_calendar:     None
        """

        super().__init__(filename, address, units=aggregated_units, **kwargs)
        self.aggregated_units    = aggregated_units
        self.aggregated_calendar = aggregated_calendar
        self.global_extent = global_extent

    def copy(self, extent=None):
        """
        Create a new instance of this class from its own methods and attributes, and 
        apply a new extent to the copy if required.
        """
        
        kwargs = self.get_kwargs()

        if 'units' in kwargs:
            if not kwargs['aggregated_units']:
                kwargs['aggregated_units'] = kwargs['units']
            kwargs.pop('units')

        if extent:
            kwargs['extent'] = combine_slices(self.shape, list(self.get_extent()), extent)
            kwargs['global_extent'] = combine_slices(self.shape, list(self.global_extent), extent)

        new = CFAPartition(
            self.filename,
            self.address,
            **kwargs
        )
        return new

    def _post_process_data(self, data):
        """Correct units/data conversions - if necessary at this stage"""

        if self.units != self.aggregated_units:
            try:
                from cfunits import Units
            except FileNotFoundError:
                raise ValueError(
                    'Encountered issue when trying to import the "cfunits" library:'
                    "cfunits requires UNIDATA UDUNITS-2. Can't find the 'udunits2' library."
                    ' - Consider setting up a conda environment, and installing '
                    '`conda install -c conda-forge udunits2`'
                )

            data = Units.conform(data, self.units, self.aggregated_units)
        return data

    def get_kwargs(self):
        return {
            'aggregated_units': self.aggregated_units,
            'aggregated_calendar': self.aggregated_calendar
        } | super().get_kwargs()