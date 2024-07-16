from xarray.backends import (
    BackendEntrypoint, 
)

from CFAPyX.datastore import CFADataStore
from CFAPyX.backendentrypoint import CFAStoreBackendEntrypoint

def open_cfa_dataset(filename_or_obj, drop_variables=None, **kwargs):
    """
    Completes part A by opening the CFA-netCDF file and returns an
    Xarray virtual dataset with dask arrays that are lazily loaded.

     - Dask arrays make use of the following:
     # data = indexing.LazilyIndexedArray(CFAArrayWrapper())
    where the CFAArrayWrapper provides a method to fetch the specific data required
    for each fragment?

    NOTE: Will likely need some kind of module check for any additional modules, especially
    non-Python related content. I suggest creating a new module just to handle CFA (CFA-Python is a candidate)
    and checking for it here.

    """
    store = CFADataStore.open(filename_or_obj)
    # Perform post-processing/cfa decoding on the store.ds object which is still
    # a netcdf4-type dataset from the netcdf4 library
    store_entrypoint = CFAStoreBackendEntrypoint()
    ds = store_entrypoint.open_dataset(
        store,
        **kwargs
        # mask_and_scale=mask_and_scale,
        # decode_times=decode_times,
        # concat_characters=concat_characters,
        # decode_coords=decode_coords,
        # drop_variables=drop_variables,
        # use_cftime=use_cftime,
        # decode_timedelta=decode_timedelta,
    )

    return ds

class CFANetCDFBackendEntrypoint(BackendEntrypoint):
    def open_dataset(
            self,
            filename_or_obj,
            *,
            drop_variables=None,
            **kwargs,
            # backend specific keyword arguments
            # do not use 'chunks' or 'cache' here
        ):
        """
        returns a complete xarray representation of a CFA-netCDF dataset which includes expanding/decoding
        CFA aggregated variables into proper arrays.
        """

        return open_cfa_dataset(filename_or_obj, drop_variables=drop_variables, **kwargs)

        ## Support for Lazy Loading

        # - backend_array ported from somewhere else?

        # data = indexing.LazilyIndexedArray(backend_array)
        # var  = xr.Variable(dims, data, attrs=attrs, encoding=encoding)

        ## Required steps:

        # Takes address of CFA-netCDF file and opens using *someting*
        # Perform 'unpacking' of CFA terminology i.e aggregated_data/location etc.
        # Use LazilyIndexedArray when loading the fragments into an array collection
        # Collate everything into a single Xarray Dataset Object and return.

        # return something
    
if __name__ == '__main__':
    bd = CFANetCDFBackendEntrypoint()
    ds = bd.open_dataset('../testfiles/rainmaker.nca')
    #store = CFADataStore.open('../testfiles/rainmaker.nca')