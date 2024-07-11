import xarray as xr

from xarray.backends.netcdf4_ import NetCDF4DataStore
from xarray.backends.store import StoreBackendEntrypoint

store = NetCDF4DataStore.open('../testfiles/rainmaker.nca')




