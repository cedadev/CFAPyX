__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

from CFAPyX.decoder import get_dask_chunks
from CFAPyX.chunkwrapper import ChunkWrapper, ArrayLike

import dask.array as da
from dask.array.core import getter
from dask.base import tokenize
from dask.utils import SerializableLock, is_arraylike
from dask.array.reductions import numel

from itertools import product
import netCDF4
import numpy as np

class FragmentArrayWrapper(ArrayLike):
    """
    FragmentArrayWrapper behaves like an Array that can be indexed or references to return a Dask-like array object. This class
    is essentially a constructor for the subarrays that feed into the returned Dask-like array into Xarray.
    """

    description = 'Wrapper class for the Array-like behaviour required by Xarray for the array of fragments.'

    def __init__(
            self, 
            fragment_info, 
            fragment_space, 
            shape, 
            units, 
            dtype, 
            cfa_options={}, 
            active_options={}, 
            named_dims=None
        ):
        """
        :param fragment_info:       (dict) The information relating to each fragment with the fragment coordinates
                                    in ``fragment space`` as the key. Each fragment is described by the following:
                                     - ``shape`` - The shape of the fragment in ``array space``.
                                     - ``location`` - The file from which this fragment originates.
                                     - ``address`` - The variable and group name relating to this variable.
                                     - ``extent`` - The slice object to apply to the fragment on retrieval (usually get
                                       the whole array)
                                     - ``global_extent`` - The slice object that equates to a particular fragment out
                                       of the whole array (in ``array space``).

        :param fragment_space:      (tuple) The coordinate system that refers to individual fragments. Each coordinate 
                                    eg. i, j, k refers to the number of fragments in each of the associated dimensions.

        :param shape:               (tuple) The total shape of the array in ``array space``

        :param units:       (obj) The units of the values represented in this Array-like class.

        :param dtype:       (obj) The datatype of the values represented in this Array-like class.

        :param cfa_options:     (dict) The set of options defining some specific decoding behaviour.

        :param active_options:  (dict) The set of options defining Active behaviour.

        :param named_dims:  (list) The set of dimension names that apply to this Array object.
        """

        self.fragment_info    = fragment_info
        self.fragment_space   = fragment_space
        self.named_dims       = named_dims

        # Set internal private variables
        self.cfa_options    = cfa_options
        self.active_options = active_options

        self.__array_function__ = self.__array__

        super().__init__(shape, dtype=dtype, units=units)

    def __getitem__(self, selection):
        """
        Non-lazy retrieval of the dask array when this object is indexed.
        """
        arr = self.__array__()
        return arr[selection]
    
    def __array__(self):
        """
        Non-lazy array construction, this will occur as soon as the instance is ``indexed`` or any other ``array`` 
        behaviour is attempted. Construction of a Dask-like array occurs here based on the decoded fragment info
        and any other specified settings.
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
                # Wrong extent type for both scenarios but keep as a different label for dask chunking.

            fragment = CFAChunk(
                filename,
                address,
                dtype=dtype,
                extent=extent,
                shape=fragment_shape,
                position=fragment_position,
                aggregated_units=units,
                aggregated_calendar=calendar,
                format=fragment_format,
            )

            fragments[pos] = fragment
        
        if not self.active_chunks:
            dsk = self._chunk_by_fragment(fragments)

            global_extent = {k: fragment_info[k]["global_extent"] for k in fragment_info.keys()}
            dask_chunks = get_dask_chunks(
                self.shape,
                self.fragment_space,
                extent=global_extent,
                dtype=self.dtype,
                explicit_shapes=None
            )

        else:
            dsk = self._chunk_oversample(fragments)
            dask_chunks = None # Assemble in the same format as the CFA ones.
            raise NotImplementedError

        if self._use_active:
            try:
                from XarrayActive import DaskActiveArray

                darr = DaskActiveArray(dsk, array_name[0], chunks=dask_chunks, dtype=dtype)
            except ImportError:
                raise ImportError(
                    '"DaskActiveArray" from XarrayActive failed to import - please ensure you have the XarrayActive package installed.'
                )
        else:
            darr = da.Array(dsk, array_name[0], chunks=dask_chunks, dtype=dtype)
        return darr
    
    @property
    def active_options(self):
        """Relates private option variables to the ``active_options`` parameter of the backend."""
        return {
            'use_active': self._use_active,
            'active_chunks': self._active_chunks
        }
    
    @active_options.setter
    def active_options(self, value):
        self._set_active_options(**value)

    def _set_active_options(
            self, 
            use_active=False, 
            active_chunks=None, 
            **kwargs):
        """
        Sets the private variables referred by the ``active_options`` parameter to the backend. Ignores
        additional kwargs.
        """
        self._use_active = use_active
        self._active_chunks = active_chunks

    @property
    def cfa_options(self):
        """Relates private option variables to the ``cfa_options`` parameter of the backend."""
        return {
            'substitutions': self._substitutions,
            'decode_cfa': self._decode_cfa
        }

    @cfa_options.setter
    def cfa_options(self, value):
        self._set_cfa_options(**value)

    def _set_cfa_options(
            self,
            substitutions=None,
            decode_cfa=None,
            **kwargs):
        """
        Sets the private variables referred by the ``cfa_options`` parameter to the backend. Ignores
        additional kwargs.
        """
        
        # Don't need this here
        if substitutions:

            if type(substitutions) != list:
                substitutions = [substitutions]

            for s in substitutions:
                base, substitution = s.split(':')
                for f in self.fragment_info.keys():
                    self.fragment_info[f]['location'] = self.fragment_info[f]['location'].replace(base, substitution)

        self._substitutions = substitutions
        self._decode_cfa = decode_cfa

    def _chunk_by_fragment(self, fragments):
        """
        Assemble the base ``dsk`` task dependency graph which includes the fragment objects plus the
        method to index each object (with locking).

        :param fragments:   (dict) The set of Fragment objects (ChunkWrapper/CFAChunk) with their positions
                            in ``fragment space``.

        :returns:       A task dependency graph with all the fragments included to use when constructing the dask array.
        """
        array_name = (f"{self.__class__.__name__}-{tokenize(self)}",)

        dsk = {}
        for fragment_position in fragments.keys():
            fragment = fragments[fragment_position]

            f_identifier = f"{fragment.__class__.__name__}-{tokenize(fragment)}"
            dsk[f_identifier] = fragment
            dsk[array_name + fragment_position] = (
                getter, # Method of retrieving the 'data' from each fragment - but each fragment is Array-like.
                f_identifier,
                fragment.get_extent(),
                False,
                getattr(fragment, "_lock", False) # Check version cf-python
            )
        return dsk

    def _derive_chunk_space(self):
        """
        Derive the chunk space and shape given the user-provided ``active_chunks`` option. Chunk space is the
        number of chunks in each dimension which presents like an array shape, but is referred to as a ``space``
        because it has a novel coordinate system. Chunk shape is the shape of each chunk in ``array space``, which must be regular
        even if lower-level objects used to define the chunk are not.

        Example: 
            50 chunks across the time dimension of 1000 values which is represented by 8 fragments. Chunk space representation is
            (50,) and the chunk shape is (20,). Each chunk is served by at most 2 fragments, where each chunk is described using a 
            MultiFragmentWrapper object which appropriately sets the extents of each Fragment object. The Fragments cover 125 values
            each:
        
            Chunk 0 served by Fragment 0 slice(0,20) 
            Chunk 1 served by Fragment 0 slice(20,40)
            ...
            Chunk 6 served by Fragment 0 slice(120,None) and Fragment 1 slice(0,15)
            ...
            and so on.

        """
        chunk_space = [1 for i in self.shape]
        chunk_shape = [i for i in self.shape]

        for dim in self.active_chunks.keys():
            chunks = self.active_chunks[dim]

            idim = None
            for x, d in enumerate(self.named_dims):
                if d == dim:
                    idim = x

            if not idim:
                raise ValueError(
                    f"Requested chunking across dimension '{dim}'"
                    f"but only '{self.named_dims}' present in the dataset"
                )

            length = self.shape[idim]
            chunk_space[idim] = chunks
            chunk_shape[idim] = int(length/chunks)

        return chunk_space, chunk_shape

    def _chunk_oversample(self, fragments):
        """
        Assemble the base ``dsk`` task dependency graph which includes the chunk objects plus the
        method to index each chunk object (with locking). In this case, each chunk object is a MultiFragmentWrapper
        which serves another dask array used to combine the individual fragment arrays contributing to each chunk.

        :param fragments:   (dict) The set of Fragment objects (ChunkWrapper/CFAChunk) with their positions
                            in ``fragment space``. These are copied into MultiFragmentWrappers with the correctly applied
                            extents such that all the chunks define the scope of the total array.

        Terminology Notes:

            ``cs`` and ``fs`` represent the chunk_shape and fragment_shape respectively, with short names to make the code simpler to read.

        :returns:       A task dependency graph with all the chunks included to use when constructing the dask array.
        """

        chunk_space, cs = self._derive_chunk_space()

        mfwrapper = {}

        for fragment_coord in fragments.keys():

            fragment = fragments[fragment_coord]

            # Shape of each fragment may vary
            fs = fragment.shape

            # Calculate chunk coverage for this fragment
            initial, final = [],[]
            for dim in range(len(fragment_coord)):

                initial.append(
                    int(fragment_coord[dim] * fs[dim]/cs[dim])
                )
                fn = int((fragment_coord[dim]+1) * fs[dim]/cs[dim])
                
                final.append(
                    min(fn, chunk_space[dim])
                )

            # Chunk coverage extent 
            # - Two chunk-space coordinates defining the chunks that are covered by this fragment.
            cce = [tuple(initial), tuple(final)]

            # Generate the list of chunks covered by this fragment.
            chunk_list = [
                coord for coord in product(
                    *[range(r[0], r[1]) for r in zip(cce[0], cce[1])]
                )
            ]

            # Generate the 'extent of this fragment' in ``fragment_space``
            # i.e Fragment (0,0) has extent (0,0) to (1,1)
            fragment_extent = [
                tuple(fragment_coord),
                (i +1 for i in fragment_coord)
            ]

            # Copy the fragment with the correct extent for each chunk covered.
            for c in chunk_list:

                # For each fragment, the subdivisions caused by chunking create an irregular array 
                # of sliced fragments which comprises the whole chunk. Each of these sliced fragment 
                # needs a coordinate relative to ...
                relative_fragment = tuple([c[i] - chunk_list[0][i] for i in range(len(c))])

                chunk = [
                    tuple(c),
                    (i+1 for i in c)
                ]

                hyperslab = _overlap(chunk, cs, fragment_extent, fs)

                newfragment = fragment.copy(extent=hyperslab)

                if c in mfwrapper:
                    mfwrapper[c][relative_fragment] = newfragment
                else:
                    mfwrapper[c] = {relative_fragment: newfragment}

        array_name = (f"{self.__class__.__name__}-{tokenize(self)}",)
        dsk = {}

        for chunk in mfwrapper.keys():
            fragments = mfwrapper[chunk]
            mfwrap = MultiFragmentWrapper(fragments)

            # f_indices is the initial_extent for the ChunkWrapper

            mf_identifier = f"{mfwrap.__class__.__name__}-{tokenize(mfwrap)}"
            dsk[mf_identifier] = mfwrap
            dsk[array_name + chunk] = (
                getter, # From dask docs - replaces fragment_getter
                mf_identifier,
                fragment.get_extent(), # Needs a think on how to get this out.
                False,
                getattr(fragment, "_lock", False) # Check version cf-python
            )
        return dsk

class CFAChunk(ChunkWrapper):

    description = 'Wrapper object for a CFA Fragment, extends the basic ChunkWrapper with CFA-specific methods.'

    def __init__(self,
                 filename,
                 address,
                 aggregated_units=None,
                 aggregated_calendar=None,
                 **kwargs
            ):
        
        """
        Wrapper object for the 'array' section of a fragment. Contains some metadata to ensure the
        correct fragment is selected, but generally just serves the fragment array to dask when required.

        Parameters: extent - in the form of a list/tuple of slices for this fragment. Different from the 
                             'location' parameter which states where the fragment fits into the total array.
        """

        super().__init__(filename, address, **kwargs)
        self.aggregated_units    = aggregated_units # cfunits conform method.
        self.aggregated_calendar = aggregated_calendar

    def copy(self, extent=None):
        """
        Create a new instance of this class from its own methods and attributes, and apply
        a new extent to the copy if required.
        """
        new = CFAChunk(
            self.filename,
            self.address,
            aggregated_units=self.aggregated_units,
            aggregated_calendar=self.aggregated_calendar,
            **super().get_kwargs()
        )
        if extent:
            return new[extent]
        return new

    def _post_process_data(self, data):
        """Correct units/data conversions - if necessary at this stage"""
        return data

class MultiFragmentWrapper:
    description = 'Brand new array class for handling any-size dask chunks.'

    """
    Requirements:
     - Fragments are initialised with a position in index space. (Fragment Space)
     - Chunk position array initialised with a different index space. (Compute Space)
     - For each fragment, identify which chunk positions it falls into and add that `CFAChunk` to a dict.
     - The dict contains Chunk coordinates (compute space) as keys, with the values being a list of pairs of 
       CFAChunk objects that are already sliced and the array shapes those sliced segments fit into.
    """

    def __init__(self, fragments):
        self.fragments = fragments

        raise NotImplementedError

    def __array__(self):
        array_name = (f"{self.__class__.__name__}-{tokenize(self)}",)

        dsk = {}
        for fragment_position in self.fragments.keys():
            fragment = self.fragments[fragment_position]

            # f_indices is the initial_extent for the ChunkWrapper

            f_identifier = f"{fragment.__class__.__name__}-{tokenize(fragment)}"
            dsk[f_identifier] = fragment
            dsk[array_name + fragment_position] = (
                getter, # From dask docs - replaces fragment_getter
                f_identifier,
                fragment.get_extent(),
                False,
                getattr(fragment, "_lock", False) # Check version cf-python
            )

        # Should return a dask array.
        return dsk

def _overlap(chunk, chunk_size, fragment, fragment_size):
    """
    Determine the overlap between a chunk and fragment. Not yet properly implemented.

    :param chunk:           None    

    :param chunk_size:      None

    :param fragment:        None
    
    :param fragment_size:   None

    Chunk and Fragment need to have structure (2,N) where 2 signifies the start and end of each dimension
    and N is the number of dimensions.
    """

    extent = []
    for dim in range(len(chunk[0])):
        dimslice = _overlap_in_1d(
            (chunk[0][dim], chunk[1][dim]),
            chunk_size,
            (fragment[0][dim], fragment[1][dim]),
            fragment_size
        )
        extent.append(dimslice)
    return extent # Total slice-based overlap of chunk and fragment

def _overlap_in_1d(chunk, chunk_size, fragment, fragment_size):

    start = max(chunk[0]*chunk_size, fragment[0]*fragment_size)
    end   = min(chunk[1]*chunk_size, fragment[1]*chunk_size)

    return slice(start, end) # And possibly more