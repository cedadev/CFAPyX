import numpy as np

class OneOrMoreList: # Simple List Wrapper
    def __init__(self, array):
        self.array = array
        self.is_one = len(array) == 1

    def __getitem__(self, index):
        if self.is_one:
            return self.array[0]
        else:
            return self.array[index]
        
# DUPLICATED FUNCTION: xarray.backends.netcdf4_ (PRIVATE)
def _ensure_fill_value_valid(data, attributes):
    # work around for netCDF4/scipy issue where _FillValue has the wrong type:
    # https://github.com/Unidata/netcdf4-python/issues/271
    if not hasattr(data.dtype,'kind'):
        return
    if data.dtype.kind == "S" and "_FillValue" in attributes:
        attributes["_FillValue"] = np.bytes_(attributes["_FillValue"])