# All routines for testing CFA general methods.
import xarray as xr
import os
import pytest

from cfapyx import CFANetCDF

TESTS = 'tests/test_space'

@pytest.mark.dependency()
def test_cfa_write(tests=TESTS):

    filepattern = f'{tests}/rain/example*.nc'

    ds = CFANetCDF(filepattern, concat_msg='ThisIsATests')

    ds.create(
        updates={'test_name':'alpha', 'test_remove':'not removed'},
        removals={'test_remove'}
    )

    ds.write(f'{tests}/testrain.nca')

    print('Integration tests: Write - complete')

@pytest.mark.dependency(depends=['test_cfa_write'])
def test_cfa_pure(tests=TESTS, active=False):

    FILE = f'{tests}/testrain.nca'

    # Local testing: Add CFAPyX before tests
    try:
        ds = xr.open_dataset(FILE, engine='CFA',
                            cfa_options={
                                'use_active':active
                                })
    except Exception as err:
        assert isinstance(err, ImportError)
        print(f'Integration tests: Read(pure, active={active}) - skipped')
        return
    
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

    print(f'Integration tests: Read(pure, active={active}) - complete')

@pytest.mark.dependency(depends=['test_cfa_write'])
def test_cfa_chunks(tests=TESTS):

    FILE = f'{tests}/testrain.nca'

    ds = xr.open_dataset(FILE, engine='CFA',
                         cfa_options={
                             'chunks': {'longitude':180},
                             'chunk_limits':False})
    
    ## Test global dataset
    assert not hasattr(ds,'address')
    assert not hasattr(ds,'shape')
    assert not hasattr(ds,'location')
    
    assert 'p' in ds
    assert ds['p'].shape == (20, 180, 360)

    # Using selection with lat/lon values NOT index values
    p_sel = ds['p'].isel(time=slice(0,3),latitude=slice(140,145), longitude=slice(100,300))

    assert p_sel.shape == (3, 5, 200)
    assert not hasattr(p_sel, 'aggregated_data')
    assert not hasattr(p_sel, 'aggregated_dimensions')

    p_mean = p_sel.mean(dim='time')

    assert p_mean.shape == (5, 200)
    assert (p_mean[0][0].to_numpy() - 0.664414) < 0.01

    p_value = p_sel.mean()

    assert p_value.shape == ()
    assert (p_value.to_numpy() - 0.490389) < 0.01

    print(f'Integration tests: Read(chunked) - complete')

@pytest.mark.dependency(depends=['test_cfa_write', 'test_cfa_pure', 'test_cfa_chunks'])
def test_cleanup(tests=TESTS):
    os.system(f'rm {tests}/testrain.nca')
    print('Integration tests: Cleanup - complete')

if __name__ == '__main__':

    tests = '/home/users/dwest77/cedadev/cfatests/CFAPyX/tests/test_space'

    test_cfa_write(tests=tests)
    test_cfa_pure(tests=tests, active=False)
    test_cfa_pure(tests=tests, active=True)
    test_cfa_chunks(tests=tests)

    print('All Integration tests complete.')