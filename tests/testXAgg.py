import xarray as xr

ds = xr.open_dataset('ScenarioMIP_CNRM-CERFACS_CNRM-ESM2-1_ssp119_r1i1p1f2_3hr_huss_gr_CFA1.0.nc')

print(ds)