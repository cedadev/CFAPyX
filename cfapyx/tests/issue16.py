import xarray as xr

file = '/gws/nopw/j04/cedaproc/dwest77/CMIP6_ScenarioMIP_CNRM-CERFACS_CNRM-ESM2-1_ssp119_r1i1p1f2_3hr_huss_gr_v20190328_cfa1.0.nca'

ds = xr.open_dataset(file, engine='CFA', cfa_options={'decode_cfa':False})
x = 1
ds = xr.open_dataset(file)