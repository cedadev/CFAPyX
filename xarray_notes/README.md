# xarray open dataset for Kerchunk/Zarr

`ZarrBackendEntrypoint` instance created and given the zarr store URI or kerchunk/fsspec mapper object to deal with.
 - See what methods this backend uses although it is likely to be zarr-specific.

In xarray.backends.zarr:
`data = indexing.LazilyIndexedArray(ZarrArrayWrapper(name, self))`

Some equivalent of this for loading data in part `B` will be needed.

## Open a typical NetCDF4 file in Xarray

`xr.open_dataset(filename,format='netcdf4')` leads directly into:
`xarray.backends.NetCDF4BackendEntrypoint` which I will now follow to see where either the netcdf4 or h5py libraries are initially used to load the file, which is then pushed into an xarray.Dataset object.

- NetCDF4DataStore opens `filename`: 362
  - netcdf4.Dataset used as part of a CachingFileManager -> __init__

xarray.backends.file_manager.CachingFileManager :217
 - file becomes netcdf4-type dataset using `_opener` (set as netcdf4.Dataset) where `args` is the filename/path

NOTE: Could use Netcdf4DataStore or override some of the workflow.

NetCDF4DataStore._acquire:412
`ds = _nc4_require_group(root,...)`
 - Root is already the whole opened dataset (comes from manager.acquire_context)
 - acquire_context yields the file from the CachningFileManager (217) already looked at.
