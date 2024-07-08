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

### Open single NetCDF file and extract domain information