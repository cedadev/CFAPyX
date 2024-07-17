# CFAPyX
CFA python Xarray module for using CFA files with xarray.

For use with the Xarray module as an additional backend.

## Installation

```
pip install xarray==2024.6.0
pip install -e .
```

## Usage

```
import xarray as xr

ds = xr.open_dataset('cfa_file.nca', engine='CFA')
# Plot data

```