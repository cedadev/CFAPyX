# All routines for testing CFA general methods.
import xarray as xr

def test_simple():

    ds = xr.open_dataset('rain/rainmaker.nca', engine='CFA',
                         cfa_options={'substitutions':"/home/users/dwest77/Documents/cfa_python_dw/testfiles/:"})
    
    assert 'p' in ds