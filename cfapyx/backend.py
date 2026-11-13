__author__ = "Daniel Westwood"
__contact__ = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import logging
from collections.abc import Mapping
from functools import partial

import numpy as np
import requests
from xarray import conventions
from xarray.backends import BackendEntrypoint, StoreBackendEntrypoint
from xarray.backends.common import AbstractDataStore
from xarray.coders import CFDatetimeCoder
from xarray.coding.times import decode_cf_datetime
from xarray.coding.variables import lazy_elemwise_func
from xarray.core import indexing
from xarray.core.dataset import Dataset
from xarray.core.formatting import first_n_items, last_item
from xarray.core.variable import Variable
from xarray.namedarray.pycompat import to_numpy

from cfapyx.datastore import CFADataStore
from cfapyx.utils import conform_data_to_units, logstream
from cfapyx.wrappers import FragmentArrayWrapper

logger = logging.getLogger(__name__)

logger.addHandler(logstream)
logger.propagate = False


def fetch_remote_as_bytes(remote_url: str) -> bytes:

    response = requests.get(remote_url)
    response.raise_for_status()

    # Fetch bytes content of remote CFA file
    return response.content


def open_cfa_dataset(
    filename_or_obj,
    drop_variables=None,
    mask_and_scale=False,
    decode_times=True,
    concat_characters=None,
    decode_coords=None,
    use_cftime=None,
    decode_timedelta=None,
    cfa_options: dict = None,
    group=None,
):
    """
    Top-level function which opens a CFA dataset using Xarray.

    Creates a CFA Datastore
    from the ``filename_or_obj`` provided, then passes this to a CFA
    StoreBackendEntrypoint to create an Xarray Dataset. Most parameters are
    not handled by CFA, so only the CFA-relevant ones are described here.

    :param filename_or_obj:       (str) The path to a CFA-netCDF file to be
        opened by Xarray

    :param cfa_options:           (dict) A set of kwargs provided to CFA which
        provide additional configurations. Currently implemented are:
        substitutions (dict), decode_cfa (bool)

    :param group:                 (str) The name or path to a NetCDF group.
        CFA can handle opening from specific groups and will inherit both
        ``group`` and ``global`` dimensions/attributes.

    :returns:       An xarray.Dataset object composed of xarray.DataArray objects
        representing the different NetCDF variables and dimensions. CFA aggregated
        variables are decoded unless the ``decode_cfa`` parameter in ``cfa_options``
        is false.
    """

    cfa_options = cfa_options or {}

    # Add loading CFA file from remote location
    if isinstance(filename_or_obj, str):
        if "https://" in filename_or_obj or "s3://" in filename_or_obj:
            filename_or_obj = fetch_remote_as_bytes(filename_or_obj)

    # Load the CFA datastore from the provided file (object not supported).
    store = CFADataStore.open(filename_or_obj, group=group)

    # Expands cfa_options into individual kwargs for the store.
    store.cfa_options = cfa_options
    # Mask/scale decoding now done internally.
    store.mask_and_scale = mask_and_scale

    # Xarray makes use of StoreBackendEntrypoints to provide the Dataset 'ds'
    store_entrypoint = CFAStoreBackendEntrypoint()
    ds = store_entrypoint.open_dataset(
        store,
        decode_times=decode_times,
        concat_characters=concat_characters,
        decode_coords=decode_coords,
        drop_variables=drop_variables,
        use_cftime=use_cftime,
        decode_timedelta=decode_timedelta,
    )

    return ds


class CFANetCDFBackendEntrypoint(BackendEntrypoint):
    """Open CFA-netCDF files (.nca) using 'cfapyx' in Xarray"""

    description = 'Open CFA-netCDF files (.nca) using "cfapyx" in Xarray'
    url = "https://cedadev.github.io/CFAPyX/"

    def open_dataset(
        self,
        filename_or_obj,
        *,
        drop_variables=None,
        mask_and_scale=False,
        decode_times=True,
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
        Returns a complete xarray representation of a CFA-netCDF dataset which
        includes expanding/decoding CFA aggregated variables into proper arrays.
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
            group=group,
        )


def _is_cfa_lazy_data(data) -> bool:
    """
    Return True if ``data`` is backed by a lazy CFA ``FragmentArrayWrapper``.
    """
    curr = data
    while hasattr(curr, "array"):
        if isinstance(curr, FragmentArrayWrapper):
            return True
        curr = curr.array
    return isinstance(curr, FragmentArrayWrapper)


class CFADatetimeCoder(CFDatetimeCoder):
    """
    CFA-aware extension of Xarray's ``CFDatetimeCoder``.

    Xarray's default ``CFDatetimeCoder`` decodes an ``example_value`` assembled
    from the first and last items of a variable's array in order to infer the
    decoded datetime dtype (e.g. ``datetime64[ns]`` vs ``object`` for
    ``cftime.datetime``). For CFA-aggregated variables such as ``time_bnds``,
    fetching those items forces remote fragment files to be opened at dataset
    open time even when the variable is never accessed.

    ``CFADatetimeCoder`` bypasses fragment fetching for CFA-aggregated variables
    by generating ``example_value`` from the corresponding non-aggregated
    coordinate variable (such as ``time``) already stored in the CFA dataset.
    """

    def __init__(
        self,
        variables: Mapping[str, Variable] | None = None,
        use_cftime: bool | None = None,
        time_unit: str = "ns",
    ) -> None:
        super().__init__(use_cftime=use_cftime, time_unit=time_unit)
        self._coord_candidates = {}
        if variables:
            for k, v in variables.items():
                if not _is_cfa_lazy_data(v._data):
                    self._coord_candidates[k] = (
                        v._data,
                        dict(v.attrs),
                        v.dims,
                        v.size,
                    )

    def _find_time_coord(self, variable: Variable, name):
        """
        Find a non-aggregated CF time coordinate variable corresponding to
        ``variable`` via CF ``bounds``/``climatology`` attributes or shared
        dimensions.
        """
        if name is not None:
            for cand_name, cand_info in self._coord_candidates.items():
                _, cand_attrs, _, _ = cand_info
                cand_units = cand_attrs.get("units")
                has_date_units = isinstance(cand_units, str) and "since" in cand_units
                if has_date_units and (
                    cand_attrs.get("bounds") == name
                    or cand_attrs.get("climatology") == name
                ):
                    return cand_name, cand_info

        for dim in variable.dims:
            if dim in self._coord_candidates:
                cand_info = self._coord_candidates[dim]
                _, cand_attrs, _, _ = cand_info
                cand_units = cand_attrs.get("units")
                if isinstance(cand_units, str) and "since" in cand_units:
                    return dim, cand_info

        return None, None

    def _generate_example_value(self, variable: Variable, coord_info) -> np.ndarray:
        """
        Generate a 1D ``example_value`` array from the first and last elements
        of the non-aggregated coordinate variable.
        """
        coord_data, coord_attrs, _, coord_size = coord_info

        if variable.size == 0 or coord_size == 0:
            return np.array([], dtype=variable.dtype)

        coord_values = indexing.ImplicitToExplicitIndexingAdapter(
            indexing.as_indexable(coord_data)
        )
        example_value = np.concatenate(
            [
                to_numpy(first_n_items(coord_values, 1)),
                to_numpy(last_item(coord_values)),
            ]
        )

        if "scale_factor" in coord_attrs or "add_offset" in coord_attrs:
            scale = coord_attrs.get("scale_factor", 1.0)
            offset = coord_attrs.get("add_offset", 0.0)
            example_value = example_value * scale + offset

        var_units = variable.attrs.get("units")
        var_calendar = variable.attrs.get("calendar")
        coord_units = coord_attrs.get("units")
        coord_calendar = coord_attrs.get("calendar")

        if (
            example_value.size > 0
            and isinstance(coord_units, str)
            and isinstance(var_units, str)
            and (
                coord_units != var_units
                or (coord_calendar or "standard") != (var_calendar or "standard")
            )
        ):
            example_value = conform_data_to_units(
                example_value,
                coord_units,
                var_units,
                old_calendar=coord_calendar or "standard",
                new_calendar=var_calendar or "standard",
            )

        if np.issubdtype(variable.dtype, np.number):
            if not (
                np.issubdtype(variable.dtype, np.integer)
                and np.issubdtype(example_value.dtype, np.floating)
                and np.isnan(example_value).any()
            ):
                example_value = example_value.astype(variable.dtype)

        return example_value

    def decode(self, variable: Variable, name=None) -> Variable:
        """
        Custom Datetime decoder, defaults to CFDatetimeCoder.decode where
        not using a CFA FragmentArrayWrapper for a variable."""

        units = variable.attrs.get("units", None)
        if not (isinstance(units, str) and "since" in units):
            return variable

        if not _is_cfa_lazy_data(variable._data):
            return super().decode(variable, name=name)

        coord_name, coord_info = self._find_time_coord(variable, name)
        if coord_info is None:
            return super().decode(variable, name=name)

        example_value = self._generate_example_value(variable, coord_info)
        logger.debug(
            f"Bypassing fragment fetch for '{name}' using coordinate "
            f"'{coord_name}' example_value={example_value}"
        )

        sample_var = Variable(
            ("time_sample",), example_value, variable.attrs, variable.encoding
        )
        decoded_sample = super().decode(sample_var, name=name)

        if hasattr(decoded_sample._data, "func"):
            transform = decoded_sample._data.func
        else:
            transform = partial(
                decode_cf_datetime,
                units=decoded_sample.encoding.get("units"),
                calendar=decoded_sample.encoding.get("calendar"),
                use_cftime=self.use_cftime,
                time_unit=self.time_unit,
            )

        data = lazy_elemwise_func(variable._data, transform, decoded_sample.dtype)
        return Variable(
            variable.dims,
            data,
            decoded_sample.attrs,
            decoded_sample.encoding,
            fastpath=True,
        )


def _resolve_cfa_decode_times(decode_times, use_cftime, variables):
    """
    Wrap truthy ``decode_times`` specifications with ``CFADatetimeCoder`` so
    CFA-aggregated lazy time variables generate ``example_value`` from their
    coordinate dimension rather than fetching remote fragments.
    """
    if not decode_times:
        return decode_times, use_cftime

    if isinstance(decode_times, CFDatetimeCoder):
        if use_cftime is not None:
            return decode_times, use_cftime
        return (
            CFADatetimeCoder(
                variables=variables,
                use_cftime=decode_times.use_cftime,
                time_unit=decode_times.time_unit,
            ),
            None,
        )

    if isinstance(decode_times, Mapping):
        resolved = {}
        for k in variables:
            dt_k = decode_times.get(k, True)
            uc_k = (
                use_cftime.get(k, None)
                if isinstance(use_cftime, Mapping)
                else use_cftime
            )
            if not dt_k:
                resolved[k] = False
            elif isinstance(dt_k, CFDatetimeCoder):
                if uc_k is not None:
                    return decode_times, use_cftime
                resolved[k] = CFADatetimeCoder(
                    variables=variables,
                    use_cftime=dt_k.use_cftime,
                    time_unit=dt_k.time_unit,
                )
            else:
                resolved[k] = CFADatetimeCoder(
                    variables=variables,
                    use_cftime=uc_k,
                )
        return resolved, None

    if isinstance(use_cftime, Mapping):
        resolved = {
            k: CFADatetimeCoder(
                variables=variables,
                use_cftime=use_cftime.get(k, None),
            )
            for k in variables
        }
        return resolved, None

    return CFADatetimeCoder(variables=variables, use_cftime=use_cftime), None


class CFAStoreBackendEntrypoint(StoreBackendEntrypoint):
    """Open CFA-based Abstract Data Store"""

    description = "Open CFA-based Abstract Data Store"
    url = "https://cedadev.github.io/CFAPyX/"

    def open_dataset(
        self,
        cfa_xarray_store,
        decode_times=True,
        concat_characters=True,
        decode_coords=True,
        drop_variables=None,
        use_cftime=None,
        decode_timedelta=None,
    ) -> Dataset:
        """
        Takes cfa_xarray_store of type AbstractDataStore and creates an
        xarray.Dataset object. Most parameters are not handled by CFA, so only the
        CFA-relevant ones are described here.

        :param cfa_xarray_store:        (obj) The CFA Datastore object which
            loads and decodes CFA aggregated variables and dimensions.

        :returns:           An xarray.Dataset object composed of xarray.DataArray
            objects representing the different NetCDF variables and dimensions.
            CFA aggregated variables are decoded unless the ``decode_cfa``
            parameter in ``cfa_options`` is false.

        """
        assert isinstance(cfa_xarray_store, AbstractDataStore)

        # Same as NetCDF4 operations, just with the CFA Datastore
        vars, attrs = cfa_xarray_store.load()
        encoding = cfa_xarray_store.get_encoding()

        cfa_decode_times, cfa_use_cftime = _resolve_cfa_decode_times(
            decode_times, use_cftime, vars
        )

        # Ensures variables/attributes comply with CF conventions.
        vars, attrs, coord_names = conventions.decode_cf_variables(
            vars,
            attrs,
            decode_times=cfa_decode_times,
            concat_characters=concat_characters,
            decode_coords=decode_coords,
            drop_variables=drop_variables,
            use_cftime=cfa_use_cftime,
            decode_timedelta=decode_timedelta,
        )

        # Create the xarray.Dataset object here.
        ds = Dataset(vars, attrs=attrs)

        ds = ds.set_coords(coord_names.intersection(vars))
        ds.set_close(cfa_xarray_store.close)
        ds.encoding = encoding

        return ds
