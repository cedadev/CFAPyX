import dask.array as da
import numpy as np

from xarray.core.dataset import Dataset
from xarray.core.dataarray import DataArray

class CFAActiveArray(da.Array):

    description = "CFA wrapper to the xarray.Dataset dask array, enabling the use of Active Storage."
    is_active = True

    # Note this implementation is currently ignored by xarray, which may just extract the `array` element?

    # __getitem__ is called which means the dask array is just a container, the actual method operations
    # take place elsewhere

    def copy(self):
        return CFAActiveArray(self.dask, self.name, self.chunks, meta=self)
    
    def __getitem__(self, index):
        arr = super().__getitem__(index)
        return CFAActiveArray(arr.dask, arr.name, arr.chunks, meta=arr)
    
    def active_mean(self, axis, skipna=None):
        q = self.__dask_layers__()
        # Mean across each fragment
        # Combine means

        # Involvement of Dask HighLevelGraphs?

        # dask.delayed(func)(params)

        # Returns a dask mean aggregated array (lazy implementation)
        return self

class CFAActiveDataArray(DataArray):
    def mean(
        self,
        dim,
        *,
        skipna = None,
        keep_attrs = None,
        **kwargs,
    ):
        """
        Reduce this DataArray's data by applying ``mean`` along some dimension(s).

        Parameters
        ----------
        dim : str, Iterable of Hashable, "..." or None, default: None
            Name of dimension[s] along which to apply ``mean``. For e.g. ``dim="x"``
            or ``dim=["x", "y"]``. If "..." or None, will reduce over all dimensions.
        skipna : bool or None, optional
            If True, skip missing values (as marked by NaN). By default, only
            skips missing values for float dtypes; other dtypes either do not
            have a sentinel missing value (int) or ``skipna=True`` has not been
            implemented (object, datetime64 or timedelta64).
        keep_attrs : bool or None, optional
            If True, ``attrs`` will be copied from the original
            object to the new one.  If False, the new object will be
            returned without attributes.
        **kwargs : Any
            Additional keyword arguments passed on to the appropriate array
            function for calculating ``mean`` on this object's data.
            These could include dask-specific kwargs like ``split_every``.

        Returns
        -------
        reduced : DataArray
            New DataArray with ``mean`` applied to its data and the
            indicated dimension(s) removed

        See Also
        --------
        numpy.mean
        dask.array.mean
        Dataset.mean
        :ref:`agg`
            User guide on reduction or aggregation operations.

        Notes
        -----
        Non-numeric variables will be removed prior to reducing.

        """
        return self.reduce(
            cfa_active_mean, # from duck_array_ops.mean
            dim=dim,
            skipna=skipna,
            keep_attrs=keep_attrs,
            **kwargs,
        )
    
class CFAActiveDataset(Dataset):
    def _construct_dataarray(self, name):
        """Construct a DataArray by indexing this dataset"""

        darr = super()._construct_dataarray(name)

        if darr.data.is_active:
            # Where variable is a CFAActiveArray.
            variable = darr.variable
            coords   = {k: v for k, v in zip(darr.coords.keys(), darr.coords.values())}
            name     = darr.name

            # Not ideal to break into the DataArray class but seems to be unavoidable (for now)
            indexes  = darr._indexes

            return CFAActiveDataArray(
                variable,
                coords,
                name=name,
                indexes=indexes,
                fastpath=True
            )
        else:
            return darr
    
# From duck_array_ops

def cfa_active_mean(array, axis=None, skipna=None, **kwargs):
    """
    Must return a dask aggregation object which can be computed at a later point.

    :param array:       (obj) A CFAActiveArray object which may have methods for performing active
                        calculations in a dask-happy way.
    """
    return array.active_mean(axis, skipna=skipna)
