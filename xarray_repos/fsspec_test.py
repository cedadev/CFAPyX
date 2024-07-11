import fsspec as fs
import xarray as xr

kfile = 'https://dap.ceda.ac.uk/badc/cmip6/metadata/kerchunk/pipeline1/CMIP/AS-RCEC/TaiESM1/kr1.0/CMIP6_CMIP_AS-RCEC_TaiESM1_historical_r1i1p1f1_3hr_clt_gn_v20201013_kr1.0.json'

mapper = fs.get_mapper('reference://',fo=kfile, backend_kwargs={'compression':None}, remote_protocol='https')
ds = xr.open_zarr(mapper, consolidated=False, decode_times=True)