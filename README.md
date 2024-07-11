# cfa_python_dw
Sandbox testing of different cf/cfa features in python

## Installation of requirements

```
# If using a venv
conda install -c conda-forge udunits2

pip install cf-python cf-plot
# and/or
conda install -c conda-forge cf-python cf-plot
```

## 08/07/2024 - Day 1

- Successfully generated a CFA file from 5 CMIP6 files.
- Data duplicated into a single combined file, slightly larger than the sum of the initial set of files combined.
- Combined dataset can be opened with Xarray (no surprise there since this is just a combined NetCDF file)

First goal is to get to grips with cf.aggregate in order to produce `domain-only` aggregations that can be interpreted
by cf-python (should be MUCH smaller than any single individual file.)

- NOTE: cf.aggregate and cfa are NOT the same. Aggregations simply combine sets of data, while this investigations specifically look at 'virtual aggregations'

## 09/07/2024 - Day 2

- Running cf.write with cfa engaged using a sample set of CMIP files as a test to see how a sample file will be aggregated.
- Cf generally failed to virtually aggregate CMIP example datasets
- Issues with installing editable packages on Jasmin solved in a 'hacky' way with local versions and a template for new tests

## 10/07/2024 - Day 3

Notes inside cf_repos.README

## 11/07/2024 - Day 4

Notes inside xarray_repos.README

Simplest implementation of CFA to Xarray Dataset could be:
 - Open with xr.open_dataset and post-process inside xarray backend
 - Dive into xarray netcdf backend to find h5py connection and go from there.




