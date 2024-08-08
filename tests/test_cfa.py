# All routines for testing CFA general methods.
import xarray as xr

def test_cfa_pure():

    ds = xr.open_dataset('tests/rain/rainmaker.nca', engine='CFA',
                         cfa_options={'substitutions':"/home/users/dwest77/Documents/cfa_python_dw/testfiles/:tests/"})
    
    ## Test global dataset
    assert not hasattr(ds,'address')
    assert not hasattr(ds,'shape')
    assert not hasattr(ds,'location')
    
    assert 'p' in ds
    assert ds['p'].shape == (20, 180, 360)

    p_sel = ds['p'].sel(time=slice(1,3),latitude=slice(50,54), longitude=slice(0,9))

    assert p_sel.shape == (3, 5, 10)
    assert not hasattr(p_sel, 'aggregated_data')
    assert not hasattr(p_sel, 'aggregated_dimensions')

    p_mean = p_sel.mean(dim='time')

    assert p_mean.shape == (5, 10)
    assert (p_mean[0][0] - 0.63536) < 0.01

    p_value = p_sel.mean()

    assert p_value.shape == ()
    assert (p_value - 0.511954) < 0.01