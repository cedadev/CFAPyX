import xarray as xr

file = 'issue16_example.nc'

print('INCORRECT')
ds = xr.open_dataset(file, engine='CFA', cfa_options={'decode_cfa':False})
print(ds)
print()
print("CORRECT")
ds = xr.open_dataset(file)
print(ds)