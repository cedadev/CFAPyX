import cf

"""
files = [
    '/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_201501010300-203501010000.nc',
    '/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_203501010300-205501010000.nc',
    '/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_205501010300-207501010000.nc',
    '/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_207501010300-209501010000.nc',
    '/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_209501010300-210101010000.nc'
]

filename='testfiles/ScenarioMIP_CNRM-CERFACS_CNRM-ESM2-1_ssp119_r1i1p1f2_3hr_huss_gr_CFA1.1.nc'

fields = [cf.read(i) for i in files]
print(fields[0][0])
a = fields[0][0][-10:]
print(fields[1][0])
b = fields[1][0][:10]

g = cf.aggregate([a,b])

# 58430 to 58450 time indexes

print(g)

cf.write(g, filename, cfa=True)

"""