fragment_shape = (5, 1, 5)
shape = (100, 1, 20)
fragment_dims = (0, 2)
ndim = 3

locations = []

fragment_sizes = [int(shape[i]/fragment_shape[i]) for i in range(ndim)]
n_fragments = 1
for i in fragment_shape:
    n_fragments *= i

coord_mixers = [1]
for i in range(ndim-1):
    coord_mixers.append(fragment_shape[i]*coord_mixers[i])

def find_coords(n):
    n += 1
    qs = []
    coord_pointer = ndim - 1
    while coord_pointer >= 0:
        q = 0
        while n > coord_mixers[coord_pointer]:
            n -= coord_mixers[coord_pointer]
            q += 1
        if n < 0:
            n += coord_mixers[coord_pointer]
            q -= 1
        coord_pointer -= 1
        qs.append(q)
    return tuple(reversed(qs))


def build_chunk_set():

    for n in range(n_fragments):
        
        coords = find_coords(n)
        loc = []
        for x, i in enumerate(coords):
            p1 = i*fragment_sizes[x]
            p2 = (i+1)*fragment_sizes[x]
            loc.append((p1, p2))
        locations.append(loc)

build_chunk_set()

chunks = []

for dim, (n_fragments, size) in enumerate(
    zip(fragment_shape, shape)
):
    if dim in fragment_dims:
        # This aggregated dimension is spanned by more than
        # one fragment.
        c = []
        index = [0] * ndim
        for j in range(n_fragments):
            index[dim] = j
            loc = locations[j][dim]
            chunk_size = loc[1] - loc[0]
            c.append(chunk_size)

        chunks.append(tuple(c))
    else:
        # This aggregated dimension is spanned by exactly one
        # fragment. Store None, for now, in the expectation
        # that it will get overwrittten.
        chunks.append(None)

z = 1