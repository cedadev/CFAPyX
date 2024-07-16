from itertools import product

def chunk_positions(chunks):
    """Find the position of each chunk.

    .. versionadded:: 3.14.0

    .. seealso:: `chunk_indices`, `chunk_locations`, `chunk_shapes`

    :Parameters:

        chunks: `tuple`
            The chunk sizes along each dimension, as output by
            `dask.array.Array.chunks`.

    **Examples**

    >>> chunks = ((1, 2), (9,), (44, 55, 66))
    >>> for position in cf.data.utils.chunk_positions(chunks):
    ...     print(position)
    ...
    (0, 0, 0)
    (0, 0, 1)
    (0, 0, 2)
    (1, 0, 0)
    (1, 0, 1)
    (1, 0, 2)

    """
    return product(*(range(len(bds)) for bds in chunks))

def chunk_locations(chunks):
    """Find the shape of each chunk.

    .. versionadded:: 3.15.0

    .. seealso:: `chunk_indices`, `chunk_positions`, `chunk_shapes`

    :Parameters:

        chunks: `tuple`
            The chunk sizes along each dimension, as output by
            `dask.array.Array.chunks`.

    **Examples**

    >>> chunks = ((1, 2), (9,), (4, 5, 6))
    >>> for location in cf.data.utils.chunk_locations(chunks):
    ...     print(location)
    ...
    ((0, 1), (0, 9), (0, 4))
    ((0, 1), (0, 9), (4, 9))
    ((0, 1), (0, 9), (9, 15))
    ((1, 3), (0, 9), (0, 4))
    ((1, 3), (0, 9), (4, 9))
    ((1, 3), (0, 9), (9, 15))

    """
    from dask.utils import cached_cumsum

    cumdims = [cached_cumsum(bds, initial_zero=True) for bds in chunks]
    locations = [
        [(s, s + dim) for s, dim in zip(starts, shapes)]
        for starts, shapes in zip(cumdims, chunks)
    ]
    return product(*locations)
