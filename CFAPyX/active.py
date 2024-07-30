import dask.array as da

class CFAActiveArray():

    description = "CFA wrapper to the xarray.Dataset dask array, enabling the use of Active Storage."

    # Note this implementation is currently ignored by xarray, which may just extract the `array` element?

    # __getitem__ is called which means the dask array is just a container, the actual method operations
    # take place elsewhere

    def __init__(
            self, 
            dsk, 
            name, 
            chunks=None, 
            dtype=None, 
            **kwargs
        ):

        self._array = da.Array(dsk, name, chunks=chunks, dtype=dtype, **kwargs)

    def __getattr__(self, attr):
        return getattr(self._array, attr)
    
    def __getitem__(self, item):
        return self._array[item]

    def mean(self, **kwargs):
        return self._array.mean(**kwargs)