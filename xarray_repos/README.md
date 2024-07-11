# xarray open dataset for Kerchunk/Zarr

`ZarrBackendEntrypoint` instance created and given the zarr store URI or kerchunk/fsspec mapper object to deal with.
 - See what methods this backend uses although it is likely to be zarr-specific.

In xarray.backends.zarr:
`data = indexing.LazilyIndexedArray(ZarrArrayWrapper(name, self))`

Some equivalent of this for loading data in part `B` will be needed.