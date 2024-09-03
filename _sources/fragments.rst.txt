==================
CFAPyX Terminology
==================

Provided here is a list of the terminology used throughout ``CFAPyX`` and the equivalents in the CF conventions. In general most terms
within the CF conventions for 'Aggregated variables' are preserved within this package, with only some additional terms required.

.. Note::

    The page 'CFAPyX Usage and Options' covers keyword arguments to provide to ``xarray.open_dataset`` when using the ``CFA`` engine.
    This page is specifically for the terms and conventions used within the package, and will enable developers to understand the meaning of
    terms across multiple functions and classes.

.. _Fragments Chunks and Partitions:

Fragments, Chunks and Partitions
--------------------------------

CFAPyX has three specific terms for dealing with portions of arrays in different contexts. Directly from the CF conventions, a ``Fragment File`` is a 
source file for an aggregated variable, therefore the term ``Fragment`` is in relation to an array from one of these source files which constitutes part
of an aggregation.

``Chunks`` is in reference to the ``Dask Computer Chunks`` provided by the user with the ``chunks={}`` argument. If there are no chunks specified,
the Dask array defined with the ``Fragments`` as individual objects will simply use each ``Fragment`` as a ``Chunk``, so these terms are equivalent.
Alternatively if a chunk scheme is given to dask (which in most cases will not match the Fragment scheme), then additional steps must be taken to
optimise the retrieval of data.

.. image:: _images/FragmentChunkPartition.png
   :alt: Fragments, Chunks and Partitions example.

The above figure shows a case where a Chunk scheme is provided, as well as the underlying Fragment structure. The convention within this package is to 
refer to any array section as a ``Partition``. Both ``Fragments`` and ``Chunks`` are considered to be ``Partitions``. A ``Partition`` can take any shape within the
given ``space`` (see Terms in CFAPyX below). Originally it was thought that the Chunk and Fragment schemes should be allowed to overlap, and a nested Dask array 
could be used to handle  the various shapes and positions, but it was later shown to be much simpler to match the provided chunk structure to the existing fragment
structure, so each chunk is composed of exactly one fragment.

In CFAPyX, all Fragment/Chunk/Partition objects inherit from a generalised ``ArrayPartition`` class which defines certain Lazy behaviour. For the case in the
figure above, CFA would create a Dask array from several ``ChunkWrapper`` objects, corresponding to each (Orange) Dask chunk. These ``ChunkWrapper`` instances
would each contain a Dask array created from several ``CFAPartition`` objects that hold some ``extent`` of a Fragment. This means there will be multiple
low-level objects for each ``Fragment`` but their extents will not overlap, so all data points will be covered by exactly 1 low-level object and exactly 1
ChunkWrapper (now considered the second level).

Terms in CFAPyX
---------------

 - ``fragment_size_per_dim`` : The non-padded fragment sizes for each dimension (fragmented or otherwise).
 - ``fragment space`` : The coordinate system that refers to individual fragments. Each coordinate i, j, k refers to the number of 
   fragments in each of the associated dimensions. Non fragmented dimensions should take the value 0 in that dimension for all fragments. 
   Otherwise the indices (i,j,k...) will range from 0 to the number of fragments in that corresponding dimension minus 1 (since we are starting from zero.) 
 - ``_array_space`` : The space taken by an array in the ``array space``.
 - ``fragment_space`` : The total shape of the fragment array in ``fragment space``. (Formerly fragment_array_shape)
 - ``fragment_position(s)`` : A single or list of tuple values where each value is the index of a fragment in ``fragment space``
 - ``fragment_shape(s)``    : A single or list of tuple values where each value is the ``array shape`` of the array fragment.
 - ``array shape`` : The shape of a real data array.
 - ``frag_pos/frag_shape``  : The identifier for an individual fragment position or shape (see above) when iterating across all or some fragments.
 - ``nfrags_per_dim``       : The total number of fragments in each dimension (1 for non-fragmented dimensions.) 
 - ``fragmented_dim_indexes`` : The indexes of dimensions which are fragmented (0,1,2 etc.) in axis ``index space``. 
 - ``fragment_info``        : A dictionary of fragment metadata where each key is the coordinates of a fragment in index space and 
   the value is a dictionary of the attributes specific to that fragment.
 - ``constructor_shape``    : A tuple object representing the full shape of the ``fragment array variables`` where in some cases 
   (i.e all fragments having the same value) this shape can be used to expand the array into the proper shape. May not be the most 
   efficient way of implementing this though, could instead use a get_location/address method and provide the ``frag_pos`` and whole 
   location/address ``fragment array variable``.