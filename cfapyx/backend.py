__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

from xarray.backends import StoreBackendEntrypoint, BackendEntrypoint
from xarray.backends.common import AbstractDataStore
from xarray.core.dataset import Dataset
from xarray import conventions

from cfapyx.datastore import CFADataStore

import logging

logger = logging.getLogger(__name__)

def open_cfa_dataset(
        filename_or_obj,
        drop_variables=None,
        mask_and_scale=None,
        decode_times=None,
        concat_characters=None,
        decode_coords=None,
        use_cftime=None,
        decode_timedelta=None,
        cfa_options: dict=None,
        group=None,
        ):
    """
    Top-level function which opens a CFA dataset using Xarray. Creates a CFA Datastore 
    from the ``filename_or_obj`` provided, then passes this to a CFA StoreBackendEntrypoint
    to create an Xarray Dataset. Most parameters are not handled by CFA, so only the 
    CFA-relevant ones are described here.

    :param filename_or_obj:       (str) The path to a CFA-netCDF file to be opened by Xarray

    :param cfa_options:           (dict) A set of kwargs provided to CFA which provide additional 
                                  configurations. Currently implemented are: substitutions (dict), 
                                  decode_cfa (bool)

    :param group:                 (str) The name or path to a NetCDF group. CFA can handle opening 
                                  from specific groups and will inherit both ``group`` and ``global``
                                  dimensions/attributes.

    :returns:       An xarray.Dataset object composed of xarray.DataArray objects representing the different
                    NetCDF variables and dimensions. CFA aggregated variables are decoded unless the ``decode_cfa``
                    parameter in ``cfa_options`` is false.
    """

    cfa_options = cfa_options or {}

    # Load the CFA datastore from the provided file (object not supported).
    store = CFADataStore.open(filename_or_obj, group=group)

    # Expands cfa_options into individual kwargs for the store.
    store.cfa_options    = cfa_options

    use_active = False
    if hasattr(store, 'use_active'):
        use_active = store.use_active

    # Xarray makes use of StoreBackendEntrypoints to provide the Dataset 'ds'
    store_entrypoint = CFAStoreBackendEntrypoint()
    ds = store_entrypoint.open_dataset(
        store,
        mask_and_scale=mask_and_scale,
        decode_times=decode_times,
        concat_characters=concat_characters,
        decode_coords=decode_coords,
        drop_variables=drop_variables,
        use_cftime=use_cftime,
        decode_timedelta=decode_timedelta,
        use_active=use_active
    )

    return ds

class CFANetCDFBackendEntrypoint(BackendEntrypoint):

    description = 'Open CFA-netCDF files (.nca) using "cfapyx" in Xarray'
    url = "https://cedadev.github.io/CFAPyX/"

    def open_dataset(
            self,
            filename_or_obj,
            *,
            drop_variables=None,
            mask_and_scale=None,
            decode_times=None,
            concat_characters=None,
            decode_coords=None,
            use_cftime=None,
            decode_timedelta=None,
            cfa_options=None,
            group=None,
            # backend specific keyword arguments
            # do not use 'chunks' or 'cache' here
        ):
        """
        Returns a complete xarray representation of a CFA-netCDF dataset which includes expanding/decoding
        CFA aggregated variables into proper arrays.
        """

        cfa_options = cfa_options or {}

        return open_cfa_dataset(
            filename_or_obj, 
            drop_variables=drop_variables,
            mask_and_scale=mask_and_scale,
            decode_times=decode_times,
            concat_characters=concat_characters,
            decode_coords=decode_coords,
            use_cftime=use_cftime,
            decode_timedelta=decode_timedelta,
            cfa_options=cfa_options,
            group=group)

class CFAStoreBackendEntrypoint(StoreBackendEntrypoint):
    description = "Open CFA-based Abstract Data Store"
    url = "https://cedadev.github.io/CFAPyX/"

    def open_dataset(
        self,
        cfa_xarray_store,
        mask_and_scale=True,
        decode_times=True,
        concat_characters=True,
        decode_coords=True,
        drop_variables=None,
        use_cftime=None,
        decode_timedelta=None,
        use_active=False,
    ) -> Dataset:
        """
        Takes cfa_xarray_store of type AbstractDataStore and creates an xarray.Dataset object.
        Most parameters are not handled by CFA, so only the CFA-relevant ones are described here.

        :param cfa_xarray_store:        (obj) The CFA Datastore object which loads and decodes CFA
                                        aggregated variables and dimensions.

        :returns:           An xarray.Dataset object composed of xarray.DataArray objects representing the different
                            NetCDF variables and dimensions. CFA aggregated variables are decoded unless the ``decode_cfa``
                            parameter in ``cfa_options`` is false.

        """
        assert isinstance(cfa_xarray_store, AbstractDataStore)

        # Same as NetCDF4 operations, just with the CFA Datastore
        vars, attrs = cfa_xarray_store.load()
        encoding    = cfa_xarray_store.get_encoding()

        # Ensures variables/attributes comply with CF conventions.
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
        if use_active:
            try:
                from XarrayActive import ActiveDataset

                ds = ActiveDataset(vars, attrs=attrs)
            except ImportError:
                raise ImportError(
                    '"ActiveDataset" from XarrayActive failed to import - please '
                    'ensure you have the XarrayActive package installed.'
                )
        else:
            ds = Dataset(vars, attrs=attrs)
            
        ds = ds.set_coords(coord_names.intersection(vars))
        ds.set_close(cfa_xarray_store.close)
        ds.encoding = encoding

        return ds