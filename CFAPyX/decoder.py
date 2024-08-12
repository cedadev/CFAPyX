__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

from itertools import accumulate, product

from dask.array.core import normalize_chunks

def get_fragment_positions(fragment_size_per_dim):
    """
    Get the positions in index space for each fragment.

    :param fragment_size_per_dim:       (list) The set of fragment sizes per dimension. first dimension has length 
                                        equal to the number of array dimensions, second dimension is a list of the
                                        fragment sizes for the corresponding array dimension.

    :returns:       A list of tuples representing the positions of all the fragments in index space given by the
                    fragment_size_per_dim.

    """
    return product(*(range(len(sizes)) for sizes in fragment_size_per_dim))

def get_fragment_extents(fragment_size_per_dim, array_shape):
    """
    Return descriptors for every fragment. Copied from cf-python version 3.14.0 onwards.

    :param fragment_size_per_dim:       (list) The set of fragment sizes per dimension. first dimension has length 
                                        equal to the number of array dimensions, second dimension is a list of the
                                        fragment sizes for the corresponding array dimension.

    :param array_shape:             (tuple) The shape of the total array in ``array space``.

    :returns:

            global_extents - The array of slice objects for each fragment which define where the fragment
                             slots into the total array.

            extents - The extents to be applied to each fragment, usually just the whole fragment array.

            shapes - The shape of each fragment in ``array space``.

    """

    ndim = len(fragment_size_per_dim)

    initial = [0 for i in range(ndim)]
    final   = [len(i) for i in fragment_size_per_dim]

    fragmented_dims = [i for i in range(len(fragment_size_per_dim)) if len(fragment_size_per_dim[i]) != 1]

    dim_shapes  = []
    dim_indices = []
    f_locations = []
    for dim, fs in enumerate(fragment_size_per_dim):

        # (num_fragments) for each dimension
        dim_shapes.append(fs)

        fsa = tuple(accumulate((0,) + tuple(fs)))

        if dim in fragmented_dims:
            # Same as s_locations
            f_locations.append(tuple(range(len(fs))))
        else:
            # No fragmentation along this dimension
            # (0, 0, 0, 0 ...) in each non-fragmented dimension.
            f_locations.append((0,) * len(fs))

        # List of slices a to a+1 for every item in the new fs.
        dim_indices.append([slice(i, j) for i, j in zip(fsa[:-1], fsa[1:])])

    # Transform f_locations to get a dict of positions with a slice and shape for each.
    positions = [
        coord for coord in product(
            *[range(r[0], r[1]) for r in zip(initial, final)]
        )
    ]

    f_indices = []
    for dim, u in enumerate(dim_indices):
        if dim in fragmented_dims:
            f_indices.append( (slice(None),) * len(u))
        else:
            f_indices.append( u )

    f_shapes = [
        dim_shape if dim in fragmented_dims else (size,) * len(dim_shape)
        for dim, (dim_shape, size) in enumerate(zip(dim_shapes, array_shape))
    ]

    global_extents = {}
    extents = {}
    shapes = {}

    for frag_pos in positions:
        extents[frag_pos] = []
        global_extents[frag_pos] = []
        shapes[frag_pos] = []
        for a, i in enumerate(frag_pos):
            extents[frag_pos].append(
                f_indices[a][i]
            )
            global_extents[frag_pos].append(
                dim_indices[a][i]
            )
            shapes[frag_pos].append(
                f_shapes[a][i]
            )
            
    return global_extents, extents, shapes

def get_dask_chunks(
        array_space,
        fragment_space,
        extent,
        dtype, 
        explicit_shapes=None):
    """
    Define the `chunks` array passed to Dask when creating a Dask Array. This is an array of fragment sizes 
    per dimension for each of the relevant dimensions. Copied from cf-python version 3.14.0 onwards.

    :param array_space:     (tuple) The shape of the array in ``array space``.

    :param fragment_space:  (tuple) The shape of the array in ``fragment space``.

    :param extent:      (dict) The global extent of each fragment - where it fits into the total array for this variable (in array space).

    :param dtype:       (obj) The datatype for this variable.

    :param explicit_shapes:     (tuple) Set of shapes to apply to the fragments - currently not implemented outside this function.

    :returns:       A tuple of the chunk sizes along each dimension.
    """
            
    from numbers import Number
    from dask.array.core import normalize_chunks

    ndim = len(array_space)
    fsizes_per_dim, fragmented_dim_indices = [],[]

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
        fsizes_per_dim = [ # For each dimension, use fs or explicit_shapes if the dimension is fragmented or not respectively.
            fs if i in fragmented_dim_indices else explicit_shapes for i, fs in enumerate(fsizes_per_dim)
        ]
    elif isinstance(explicit_shapes, dict):
        fsizes_per_dim = [
            fsizes_per_dim[i] if i in fragmented_dim_indices else explicit_shapes.get(i, "auto")
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
            fs if i in fragmented_dim_indices else explicit_shapes[i] for i, fs in enumerate(fsizes_per_dim)
        ]

    return normalize_chunks(fsizes_per_dim, shape=array_space, dtype=dtype)