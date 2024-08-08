![CFAPyX long logo: Blue, Green and White squares arranged in Diamond formation](https://github.com/cedadev/CFAPyX/blob/CF1.12/docs/source/_images/CFAPyX_long.jpg)

CFA python Xarray module for using CFA files with xarray.

See the [Documentation](https://cedadev.github.io/CFAPyX/) for more details.

For use with the Xarray module as an additional backend.

# Installation

```
pip install xarray==2024.6.0
pip install -e .
```

# Usage

```
import xarray as xr

ds = xr.open_dataset('cfa_file.nca', engine='CFA')
# Plot data

```