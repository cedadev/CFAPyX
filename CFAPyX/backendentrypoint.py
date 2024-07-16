
from xarray.backends import StoreBackendEntrypoint
from xarray.backends.common import AbstractDataStore
from xarray.core.dataset import Dataset
from xarray import conventions

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
        #cfa_xarray_store.test_load()
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