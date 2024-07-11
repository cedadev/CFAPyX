from xarray.backends import (
    BackendEntrypoint, 
    NetCDF4DataStore, 
    StoreBackendEntrypoint, 
    NetCDF4ArrayWrapper,
)
from xarray.backends.common import AbstractDataStore
from xarray import conventions
from xarray.core.dataset import Dataset
from xarray.core.variable import Variable

from xarray.core import indexing
from xarray.coding.variables import pop_to

from xarray.core.utils import FrozenDict


import numpy as np
import netCDF4

# DUPLICATED FUNCTION: xarray.backends.netcdf4_ (PRIVATE)
def _ensure_fill_value_valid(data, attributes):
    # work around for netCDF4/scipy issue where _FillValue has the wrong type:
    # https://github.com/Unidata/netcdf4-python/issues/271
    if data.dtype.kind == "S" and "_FillValue" in attributes:
        attributes["_FillValue"] = np.bytes_(attributes["_FillValue"])

class CFADataStore(NetCDF4DataStore):

    def get_variables(self):
        """
        Optional override for get_variables method - may not be needed
        """
        return FrozenDict(
            (k, self.open_store_variable(k, v)) for k, v in self.ds.variables.items()
        )

    def get_attrs(self):
        """
        Optional override for get_attrs - may be required for aggregated variables.
        """
        return FrozenDict((k, self.ds.getncattr(k)) for k in self.ds.ncattrs())

    def open_store_variable(self, name: str, var):
        """
        Overrides parent class method for insertion of CFA-decoder steps.
        """

        dimensions = var.dimensions
        attributes = {k: var.getncattr(k) for k in var.ncattrs()}
        data = indexing.LazilyIndexedArray(NetCDF4ArrayWrapper(name, self))
        encoding = {}
        if isinstance(var.datatype, netCDF4.EnumType):
            encoding["dtype"] = np.dtype(
                data.dtype,
                metadata={
                    "enum": var.datatype.enum_dict,
                    "enum_name": var.datatype.name,
                },
            )
        else:
            encoding["dtype"] = var.dtype
        _ensure_fill_value_valid(data, attributes)
        # netCDF4 specific encoding; save _FillValue for later
        filters = var.filters()
        if filters is not None:
            encoding.update(filters)
        chunking = var.chunking()
        if chunking is not None:
            if chunking == "contiguous":
                encoding["contiguous"] = True
                encoding["chunksizes"] = None
            else:
                encoding["contiguous"] = False
                encoding["chunksizes"] = tuple(chunking)
                encoding["preferred_chunks"] = dict(zip(var.dimensions, chunking))
        # TODO: figure out how to round-trip "endian-ness" without raising
        # warnings from netCDF4
        # encoding['endian'] = var.endian()
        pop_to(attributes, encoding, "least_significant_digit")
        # save source so __repr__ can detect if it's local or not
        encoding["source"] = self._filename
        encoding["original_shape"] = data.shape

        return Variable(dimensions, data, attributes, encoding)

class CFAStoreBackendEntrypoint(StoreBackendEntrypoint):
    description = "Open CFA-based Abstract Data Store"
    url = "https://docs.xarray.dev/en/stable/generated/xarray.backends.StoreBackendEntrypoint.html"

    def open_dataset(
        self,
        cfa_xarray_store,
        *,
        mask_and_scale=True,
        decode_times=True,
        concat_characters=True,
        decode_coords=True,
        drop_variables=None,
        use_cftime=None,
        decode_timedelta=None,
    ) -> Dataset:
        """
        Takes cfa_xarray_store of type AbstractDataStore and creates an xarray.Dataset object.
         - cfa_xarray_store.ds is a netcdf4.Dataset type object. The objective is to insert a CFA decoder 
           into the workflow which maps this to an xarray.Dataset object. See how it is done in CFA-Python
           or cf-python currently and possibly include one of those implementations *or* a custom implementation
           here - but any implementation that requires C-libraries to be installed MUST be included in an external
           package and checked for presence here, so the base xarray version does not acquire additional constraints.

        11/07 - 16:26
         - store.load(): leads to get_variables() and get_attributes() methods of NetCDF4DataStore.
                         If these methods are overridden with CFA decoding on aggregated variables, that
                         could be a good way of implementing the required changes.
        """
        assert isinstance(cfa_xarray_store, AbstractDataStore)

        # Load the parameters for this NetCDF4DataStore - looking into this
        vars, attrs = cfa_xarray_store.load()

        """
        # From xarray.backends.common AbstractDataStore on load()

        variables = FrozenDict(
            (_decode_variable_name(k), v) for k, v in self.get_variables().items()
        )
        attributes = FrozenDict(self.get_attrs())
        """

        encoding = cfa_xarray_store.get_encoding()

        # Look into the conventions from xarray.
        vars, attrs, coord_names = conventions.decode_cf_variables(
            vars,
            attrs,
            mask_and_scale=mask_and_scale,
            decode_times=decode_times,
            concat_characters=concat_characters,
            decode_coords=decode_coords,
            drop_variables=drop_variables,
            use_cftime=use_cftime,
            decode_timedelta=decode_timedelta,
        )

        # Create the xarray.Dataset object here.
        ds = Dataset(vars, attrs=attrs)
        ds = ds.set_coords(coord_names.intersection(vars))
        ds.set_close(cfa_xarray_store.close)
        ds.encoding = encoding

        return ds

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
    print(ds)