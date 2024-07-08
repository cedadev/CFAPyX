import json

filename = '/gws/nopw/j04/cmip6_prep_vol1/kerchunk-pipeline/complete/CMIP6_rel1_6233/ScenarioMIP_CNRM-CERFACS_CNRM-ESM2-1_ssp119_r1i1p1f2_3hr_huss_gr_v20190328_kr1.0.json'

with open(filename) as f:
    refs=json.load(f)

print(refs['refs']['huss/.zarray'])
print(refs['refs']['huss/.zattrs'])

x=input()

huss_time = {}
for r in refs['refs'].keys():
    if 'huss/' in r:
        h = r.split('.')[0]
        huss_time[h] = refs['refs'][r]

for h in list(huss_time.keys())[:10]:
    print(h, huss_time[h])

print(len(list(huss_time.keys()))-2)
print(list(huss_time.keys())[-1])