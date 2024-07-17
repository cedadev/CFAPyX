"""
from xarray.backends import (
    BackendEntrypoint, 
)

from CFAPyX import CFANetCDFBackendEntrypoint

if __name__ == '__main__':
    bd = CFANetCDFBackendEntrypoint()
    ds = bd.open_dataset('testfiles/rainmaker.nca')
    #store = CFADataStore.open('../testfiles/rainmaker.nca')
    print(ds['p'].isel(time=slice(0,3), latitude=slice(0,2), longitude=slice(0,2)).to_numpy())

"""
import xarray as xr

ds = xr.open_dataset('testfiles/rainmaker.nca',engine='CFA')