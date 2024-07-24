import json

filename = '/gws/nopw/j04/cmip6_prep_vol1/kerchunk-pipeline/complete/CMIP6_rel1_6233/ScenarioMIP_CNRM-CERFACS_CNRM-ESM2-1_ssp119_r1i1p1f2_3hr_huss_gr_v20190328_kr1.0.json'

with open(filename) as f:
    refs=json.load(f)

huss_chunk_info = {}

for i in range(58430,58451):
    huss_chunk_info[f'huss/{i}.0.0'] = None

for r in refs['refs'].keys():
    if r in huss_chunk_info:
        huss_chunk_info[r] = refs['refs'][r]

for h in list(huss_chunk_info.keys()):
    print(h, huss_chunk_info[h])

"""
Outputs:

huss/58430.0.0 ['/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_201501010300-203501010000.nc', 5641249631, 96024]
huss/58431.0.0 ['/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_201501010300-203501010000.nc', 5641345655, 96058]
huss/58432.0.0 ['/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_201501010300-203501010000.nc', 5641441713, 95985]
huss/58433.0.0 ['/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_201501010300-203501010000.nc', 5641540834, 95962]
huss/58434.0.0 ['/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_201501010300-203501010000.nc', 5641636796, 95997]
huss/58435.0.0 ['/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_201501010300-203501010000.nc', 5641732793, 96002]
huss/58436.0.0 ['/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_201501010300-203501010000.nc', 5641828795, 96054]
huss/58437.0.0 ['/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_201501010300-203501010000.nc', 5641924849, 96023]
huss/58438.0.0 ['/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_201501010300-203501010000.nc', 5642020872, 95962]
huss/58439.0.0 ['/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_201501010300-203501010000.nc', 5642116834, 96067]
huss/58440.0.0 ['/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_203501010300-205501010000.nc', 32776, 95925]
huss/58441.0.0 ['/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_203501010300-205501010000.nc', 128701, 95847]
huss/58442.0.0 ['/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_203501010300-205501010000.nc', 224548, 96041]
huss/58443.0.0 ['/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_203501010300-205501010000.nc', 320589, 95997]
huss/58444.0.0 ['/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_203501010300-205501010000.nc', 416586, 96109]
huss/58445.0.0 ['/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_203501010300-205501010000.nc', 512695, 96052]
huss/58446.0.0 ['/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_203501010300-205501010000.nc', 608747, 96037]
huss/58447.0.0 ['/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_203501010300-205501010000.nc', 704784, 96048]
huss/58448.0.0 ['/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_203501010300-205501010000.nc', 800832, 95971]
huss/58449.0.0 ['/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_203501010300-205501010000.nc', 896803, 96086]
huss/58450.0.0 ['/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_203501010300-205501010000.nc', 992889, 96187]
"""