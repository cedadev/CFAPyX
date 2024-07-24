import cf

files = [
    '/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_201501010300-203501010000.nc',
    '/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_203501010300-205501010000.nc',
    '/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_205501010300-207501010000.nc',
    '/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_207501010300-209501010000.nc',
    '/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_209501010300-210101010000.nc'
]

filename='ScenarioMIP_CNRM-CERFACS_CNRM-ESM2-1_ssp119_r1i1p1f2_3hr_huss_gr_CFA1.1.nc'

g = cf.read(files, chunks=None)
cf.write(g, filename, cfa=True)