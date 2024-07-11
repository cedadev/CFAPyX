from xarray.backends import BackendEntrypoint

def my_open_dataset(filename_or_obj, drop_variables=None, **kwargs):
    """
    Completes part A by opening the CFA-netCDF file and returns an
    Xarray virtual dataset with dask arrays that are lazily loaded.

     - Dask arrays make use of the following:
     # data = indexing.LazilyIndexedArray(CFAArrayWrapper())
    where the CFAArrayWrapper provides a method to fetch the specific data required
    for each fragment?
    """

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
        Should return an xarray representation of the dataset
        """

        return my_open_dataset(filename_or_obj, drop_variables=drop_variables, **kwargs)

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
        raise NotImplementedError