# All routines for testing CFA general methods.
import xarray as xr
from cfapyx.utils import set_verbose
set_verbose(2)

TESTDIR = 'cfapyx/tests/test_space'

class TestCFARead:

    def test_cfa_pure(self, testdir=TESTDIR):

        FILE = f'{testdir}/testrain.nca'

        # Local testing: Add CFAPyX before tests
        try:
            ds = xr.open_dataset(FILE, engine='CFA')
        except Exception as err:
            assert isinstance(err, ImportError)
            print('Integration tests: Read(pure) - skipped')
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
        assert abs(p_mean[0][0].to_numpy() - 0.635366) < 0.01, "Pure Data Invalid"

        p_value = p_sel.mean()

        assert p_value.shape == ()
        assert abs(p_value.to_numpy() - 0.511954) < 0.01, "Pure Data Invalid"

        p_indexed = ds['p'].isel(time=5,latitude=slice(140,145), longitude=slice(90,100))
        
        assert p_indexed.shape == (5,10), "Single Indexing Shape Failed"
        assert p_indexed.dims == ('latitude','longitude'), "Single Indexing Dims Failed"

        p_data = p_indexed.to_numpy()
        assert p_data.shape == (5,10), "Single Indexing Data Shape Failed"

        print('Integration tests: Read(pure) - complete')

if __name__ == '__main__':

    #import os
    #os.chdir('CFAPyX')

    TestCFARead().test_cfa_pure()

    print('Run with poetry run pytest -v -s')