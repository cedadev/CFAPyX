from XarrayActive import DaskActiveArray
import numpy as np

class SubDaskActiveArray(DaskActiveArray):
    description = 'Lower level nested dask array - requires alterations to methods.'

    def copy(self):
        """
        Create a new SubDaskActiveArray instance with all the same parameters as the current instance.
        """
        copy_arr = SubDaskActiveArray(self.dask, self.name, self.chunks, meta=self)
        return copy_arr
    
    def __getitem__(self, index):
        """
        Perform indexing for this ActiveArray. May need to overwrite further if it turns out
        the indexing is performed **after** the dask `getter` method (i.e if retrieval and indexing
        are separate items on the dask graph). If this is the case, will need another `from_delayed`
        and `concatenation` method as used in ``active_mean``.
        """
        arr = super().__getitem__(index)
        return SubDaskActiveArray(arr.dask, arr.name, arr.chunks, meta=arr)

    def _numel(self, axes=None):
        if not axes:
            return self.size
        
        size = 1
        for i in axes:
            size *= self.shape[i]
        newshape = list(self.shape)
        for ax in axes:
            newshape[ax] = 1

        return np.full(newshape, size)

    def active_mean(self, axis=None, skipna=None):
        """
        Perform ``dask delayed`` active mean for each ``dask block`` which corresponds to a single ``chunk``.
        Combines the results of the dask delayed ``active_mean`` operations on each block into a single dask Array,
        which is then mapped to a new DaskActiveArray object.

        :param axis:        (int) The index of the axis on which to perform the active mean.

        :param skipna:      (bool) Skip NaN values when calculating the mean.

        :returns:       A new ``DaskActiveArray`` object which has been reduced along the specified axes using
                        the concatenations of active_means from each chunk.
        """
        n = self._numel(axes=axis)
        total = super().active_mean(axis=axis, skipna=skipna)

        return {
            'n': n,
            'total': total * n
        }
