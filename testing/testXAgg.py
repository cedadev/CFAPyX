import xarray as xr

loc = '/'.join(__file__.split('/')[:-2])

ds = xr.open_dataset(f'{loc}/testfiles/raincube.nca', engine='CFA', group='rain1',
                     cfa_options={'substitutions':"cfa_python_dw:CFAPyX", "decode_cfa":True})

p = ds['p'].sel(time=slice(1,3),latitude=slice(50,54), longitude=slice(0,9))
pq = p.mean(dim='time')
pq.plot()