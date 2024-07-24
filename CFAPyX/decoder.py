from itertools import accumulate, product

from dask.array.core import normalize_chunks

def chunk_positions(chunks):
    """
    Determine the position of each chunk. Copied directly from cf-python, version 3.14.0 onwards.

    """
    return product(*(range(len(bds)) for bds in chunks))

def chunk_locations(chunks):
    """Determine the shape of each chunk. Copied directly from cf-python, version 3.15.0 onwards.

    :param chunks:          (tuple) The chunk sizes along each dimension, as output by
                            `dask.array.Array.chunks`.

    :returns:       The location/shape of each array chunk within the corresponding fragment file.
    """
    from dask.utils import cached_cumsum

    cumdims = [cached_cumsum(bds, initial_zero=True) for bds in chunks]
    locations = [
        [(s, s + dim) for s, dim in zip(starts, shapes)]
        for starts, shapes in zip(cumdims, chunks)
    ]
    return product(*locations)

def fragment_descriptors(fsizes_per_dim, fragment_dims, array_shape):
    """
    Return descriptors for every fragment. Copied from cf-python version 3.14.0 onwards.

    :param fsizes_per_dim:          (tuple) Size of the fragment array in each dimension.

    :param fragment_dims:           (tuple) The indexes of dimensions which are fragmented.

    :param array_shape:             (tuple) The shape of the total array.

    :returns:

        6-`tuple` of iterators
            Each iterator iterates over a particular descriptor
            from each subarray.

            1. The indices of the aggregated array that correspond
                to each subarray.

            2. The shape of each subarray.

            3. The indices of the fragment that corresponds to each
                subarray (some subarrays may be represented by a
                part of a fragment).

            4. The location of each subarray.

            5. The location on the fragment dimensions of the
                fragment that corresponds to each subarray.

            6. The shape of each fragment that overlaps each chunk.

    """

    # The indices of the uncompressed array that correspond to
    # each subarray, the shape of each uncompressed subarray, and
    # the location of each subarray
    s_locations = []
    u_shapes = []
    u_indices = []
    f_locations = []
    for dim, fs in enumerate(fsizes_per_dim):
        num_fs = len(fs)
        # (0, 1, 2, 3 ... num_fragments) in each dimension
        s_locations.append(tuple(range(num_fs)))
        # (num_fragments) for each dimension
        u_shapes.append(fs)

        if dim in fragment_dims:
            # Same as s_locations
            f_locations.append(tuple(range(num_fs)))
        else:
            # No fragmentation along this dimension
            # (0, 0, 0, 0 ...) in each non-fragmented dimension.
            f_locations.append((0,) * num_fs)

        fs = tuple(accumulate((0,) + fs))
        u_indices.append([slice(i, j) for i, j in zip(fs[:-1], fs[1:])])

    # For each fragment, we define a slice that corresponds
    # to the data we want to collect from it.
    f_indices = [
        (slice(None),) * len(u) if dim in fragment_dims else u
        for dim, u in enumerate(u_indices)
    ]

    # For each fragment, we define a shape that corresponds to 
    # the sliced data.
    f_shapes = [
        u_shape if dim in fragment_dims else (size,) * len(u_shape)
        for dim, (u_shape, size) in enumerate(zip(u_shapes, array_shape))
    ]

    return (
        product(*u_indices),
        product(*u_shapes),
        product(*f_indices),
        product(*s_locations),
        product(*f_locations),
        product(*f_shapes),
    )

def fragment_shapes(shapes, array_shape, fragment_dims, fragment_shape, aggregated_data, ndim, dtype):
    """
    Create what is later referred to as 'chunks'. Copied from cf-python version 3.14.0 onwards.

    **Requires updating**.

    :Parameters:

        shapes: `int`, sequence, `dict` or `str`, optional
            Define the chunk shapes.

            Any value accepted by the *chunks* parameter of the
            `dask.array.from_array` function is allowed.

            The chunk sizes implied by *chunks* for a dimension
            that has been fragmented are ignored, so their
            specification is arbitrary.

        array_shape: 

        fragment_dims:

        fragment_shape:

        aggregated_data:

        ndim:

        dtype:

    :Returns:

        `tuple`
            The chunk sizes along each dimension.
    """
            
    from numbers import Number

    from dask.array.core import normalize_chunks

    # Create the base chunks.
    fsizes_per_dim = []

    for dim, (n_fragments, size) in enumerate(
        zip(fragment_shape, array_shape)
    ):
        if dim in fragment_dims:
            # This aggregated dimension is spanned by more than
            # one fragment.
            fs = []
            index = [0] * ndim
            for n in range(n_fragments):
                index[dim] = n
                loc = aggregated_data[tuple(index)]["location"][dim]
                fragment_size = loc[1] - loc[0]
                fs.append(fragment_size)

            fsizes_per_dim.append(tuple(fs))
        else:
            # This aggregated dimension is spanned by exactly one
            # fragment. Store None, for now, in the expectation
            # that it will get overwrittten.
            fsizes_per_dim.append(None)

    ## Handle custom shapes for the fragments.

    if isinstance(shapes, (str, Number)) or shapes is None:
        fsizes_per_dim = [ # For each dimension, use fs or shapes if the dimension is fragmented or not respectively.
            fs if i in fragment_dims else shapes for i, fs in enumerate(fsizes_per_dim)
        ]
    elif isinstance(shapes, dict):
        fsizes_per_dim = [
            fsizes_per_dim[i] if i in fragment_dims else shapes.get(i, "auto")
            for i, fs in enumerate(fsizes_per_dim)
        ]
    else:
        # Shapes is a sequence
        if len(shapes) != ndim:
            raise ValueError(
                f"Wrong number of 'shapes' elements in {shapes}: "
                f"Got {len(shapes)}, expected {ndim}"
            )

        fsizes_per_dim = [
            fs if i in fragment_dims else shapes[i] for i, fs in enumerate(fsizes_per_dim)
        ]

    return normalize_chunks(fsizes_per_dim, shape=array_shape, dtype=dtype)