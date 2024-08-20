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

    # Using selection with lat/lon values NOT index values
    p_sel = ds['p'].isel(time=slice(0,3),latitude=slice(140,145), longitude=slice(90,100))

    assert p_sel.shape == (3, 5, 10)
    assert not hasattr(p_sel, 'aggregated_data')
    assert not hasattr(p_sel, 'aggregated_dimensions')

    p_mean = p_sel.mean(dim='time')

    assert p_mean.shape == (5, 10)
    assert (p_mean[0][0].to_numpy() - 0.683402) < 0.01

    p_value = p_sel.mean()

    assert p_value.shape == ()
    assert (p_value.to_numpy() - 0.53279) < 0.01

def test_cfa_chunks():
    ds = xr.open_dataset('tests/rain/rainmaker.nca', engine='CFA',
                         cfa_options={
                             'substitutions':"/home/users/dwest77/Documents/cfa_python_dw/testfiles/:tests/",
                             'chunks': {'longitude':180},
                             'chunk_limits':False})
    
    ## Test global dataset
    assert not hasattr(ds,'address')
    assert not hasattr(ds,'shape')
    assert not hasattr(ds,'location')
    
    assert 'p' in ds
    assert ds['p'].shape == (20, 180, 360)

    # Using selection with lat/lon values NOT index values
    p_sel = ds['p'].isel(time=slice(0,3),latitude=slice(140,145), longitude=slice(90,100))

    assert p_sel.shape == (3, 5, 10)
    assert not hasattr(p_sel, 'aggregated_data')
    assert not hasattr(p_sel, 'aggregated_dimensions')

    p_mean = p_sel.mean(dim='time')

    assert p_mean.shape == (5, 10)
    assert (p_mean[0][0].to_numpy() - 0.683402) < 0.01

    p_value = p_sel.mean()

    assert p_value.shape == ()
    assert (p_value.to_numpy() - 0.53279) < 0.01

if __name__ == '__main__':
    test_cfa_pure()
    #test_cfa_chunks()