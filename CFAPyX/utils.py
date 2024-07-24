import numpy as np

class OneOrMoreList:
    """
    Simple list wrapper object such that a list of length 1 provides its
    single item when indexed at any ordinal position.
    """

    def __init__(self, array):
        self.array = array
        self.is_one = len(array) == 1

    def __getitem__(self, index):
        if self.is_one:
            return self.array[0]
        else:
            return self.array[index]
        
def _ensure_fill_value_valid(data, attributes):
    """
    Private Xarray function required in CFAPyX.datastore.CFADataStore, hence a copy is placed here.
    """
    # work around for netCDF4/scipy issue where _FillValue has the wrong type:
    # https://github.com/Unidata/netcdf4-python/issues/271
    if data.dtype.kind == "S" and "_FillValue" in attributes:
        attributes["_FillValue"] = np.bytes_(attributes["_FillValue"])