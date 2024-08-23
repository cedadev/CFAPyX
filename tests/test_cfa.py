# All routines for testing CFA general methods.
import xarray as xr

def test_cfa_pure(active=False):

    # Local testing: Add CFAPyX before tests
    ds = xr.open_dataset('tests/rain/rainmaker.nca', engine='CFA',
                         cfa_options={
                             'substitutions':"/home/users/dwest77/Documents/cfa_python_dw/testfiles/:tests/",
                             'use_active':active
                             })
    
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
    assert (p_mean[0][0].to_numpy() - 0.635366) < 0.01

    p_value = p_sel.mean()

    assert p_value.shape == ()
    assert (p_value.to_numpy() - 0.511954) < 0.01
    print('All tests passed!')

def test_cfa_chunks():
    return False

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
    assert (p_mean[0][0].to_numpy() - 0.635366) < 0.01

    p_value = p_sel.mean()

    assert p_value.shape == ()
    assert (p_value.to_numpy() - 0.511954) < 0.01

if __name__ == '__main__':
    test_cfa_pure(active=False)
    test_cfa_pure(active=True)
    # Chunks not implemented for release
    #test_cfa_chunks()