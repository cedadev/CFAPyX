from xarray.backends import NetCDF4ArrayWrapper

from contextlib import suppress

def netcdf4_create_group(dataset, name):
    """
    Create a new named group within a given NetCDF4.Dataset object.
    """
    return dataset.createGroup(name)


def nc4_require_group(ds, group, mode, create_group=netcdf4_create_group):
    """
    Private Xarray function to handle extraction of a group from a NetCDF dataset. In CFAPyX,
    we also save the rest of the dataset in the CFADataStore so no ``global`` parameters are lost.

    :param ds:              (obj) A NetCDF4.Dataset object from which to extract a group Dataset.

    :param group:           (str) The name or path to a group in a CFA-NetCDF file.

    :param mode:            (str) Option to create a group within the dataset if one is not yet present.

    :param create_group:    (func) Function to use when creating a group within an existing dataset.

    :returns:       NetCDF4.Dataset object which consists of just a single group.
    """
    if group in {None, "", "/"}:
        # use the root group
        return ds
    else:
        # make sure it's a string
        if not isinstance(group, str):
            raise ValueError("group must be a string or None")
        # support path-like syntax
        path = group.strip("/").split("/")
        for key in path:
            try:
                ds = ds.groups[key]
            except KeyError as e:
                if mode != "r":
                    ds = create_group(ds, key)
                else:
                    # wrap error to provide slightly more helpful message
                    raise OSError(f"group not found: {key}", e)
        return ds
    
class CFANetCDF4ArrayWrapper(NetCDF4ArrayWrapper):
    """
    Array-wrapper for NetCDF4 standard (non-aggregated) variables, where the variable may
    come from either a ``group`` or ``global`` datastore. All other functionality is consistent
    with ``xarray.backends.NetCDF4ArrayWrapper``.
    """

    def get_array(self, needs_lock=True):
        """
        Perform the get_array operation for this Variable wrapper, where the array is retrieved from
        the dataset stored in the DataStore object, which is contained by the wrapper instance.
        """

        # Acquire the main dataset:
        # - If group is requested, ds represents the group and global_ds is loaded to self.datastore.
        # - If no group requested, ds represents the global dataset.
        ds = self.datastore._acquire(needs_lock)

        try:
            variable = ds.variables[self.variable_name]
        except KeyError:
            try:
                variable = self.datastore.global_ds.variables[self.variable_name]
            except KeyError:
                raise NotImplementedError
        variable.set_auto_maskandscale(False)
        # only added in netCDF4-python v1.2.8
        with suppress(AttributeError):
            variable.set_auto_chartostring(False)
        return variable