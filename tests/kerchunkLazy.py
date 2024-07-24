import xarray as xr
import fsspec
kfile = '/gws/nopw/j04/cmip6_prep_vol1/kerchunk-pipeline/complete/CMIP6_rel1_6233/ScenarioMIP_CNRM-CERFACS_CNRM-ESM2-1_ssp119_r1i1p1f2_3hr_huss_gr_v20190328_kr1.0.json'


# h1 contains the data array after the slice operation with the below method
#ds = xr.open_dataset(kfile, engine='kerchunk')

mapper = fsspec.get_mapper('reference://', fo=kfile)
#ds = xr.open_zarr(mapper, consolidated=False)
ds = xr.open_dataset(mapper, engine='zarr', consolidated=False)


# Slice operation
h1 = ds['huss'].sel(lat=slice(-60,0), lon=slice(80,180)).isel(time=slice(10000,12000)).mean(dim='time')

h1_data = h1.data
y = 2