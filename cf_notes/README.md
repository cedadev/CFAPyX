## CF Write for CFA

- Trace of cf.write with cfa set to True

From cf.read_write.write: 759
 - Check cfa options and create a copy (check what options are valid)
 - Configures extra write vars - pass to netcdf.write

netcdf from instantiating NetCDFWrite class passed cfdm.CFDMImplementation (cf.cfimplementation)
 - instantiated
 - netcdf.write (cfdm.read_write.netcdf.NetCDFWrite)

NOTE: __init__ comes AFTER __new__ which can be overridden to perform special functions. E.g for cfdm NetCDFWrite, sets/adds new variable _NetCDFRead. Not sure if this is fundamentally different to if this was done in __init__ for this case.

cf.Data.cfa_get_write (found with grep)

from cfdm.data.Data get_data returns the Data class, which is also where cfa_get_write is found

Inside cf.Data.cfa_get_write, calls to `self._custom.get("cfa_write", False)`

_custom['cfa_write'] set in _cfa_set_write?

_custom initialised elsewhere (CFANetCDF, cfdm.Data likely candidates)
 - not cfdm.Data
 - cf.mixin2.cfanetcdf: 


## CF Read for CFA

Parsing the cfa parameter: cf.read_write.read: 803

cfa_options passed to _read_a_file: 939 -> 1053

cfa_options added to extra_read_vars

extra_read_vars used in netcdf.read: 1170
 - links to cfdm.read_write.netcdf.NetCDFRead: 774

extra_read_vars expanded into g/self.read_vars as extra attributes.
