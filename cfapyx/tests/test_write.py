import logging

from cfapyx import CFANetCDF
import xarray as xr

TESTDIR = 'cfapyx/tests/test_space'

class TestCFAWrite:
    def test_cfa_write(self, testdir=TESTDIR):

        filepattern = f'{testdir}/rain/example*.nc'

        ds = CFANetCDF(filepattern, concat_msg='ThisIsATests')

        ds.create(
            updates={'test_name':'alpha', 'test_remove':'not removed'},
            removals={'test_remove'}
        )

        assert ds.agg_dims == ('time',)
        assert ds.coord_dims == ('time', 'latitude', 'longitude')
        assert ds.pure_dims == ()
        assert ds.aggregated_vars == ('p',)
        assert ds.identical_vars == ()
        assert ds.scalar_vars == ()

        ds.write(f'{testdir}/testrain.nca')

        print('Integration tests: Write - complete')

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

        print('Integration tests: Write-Read(pure) - complete')

if __name__ == '__main__':
    TestCFAWrite().test_cfa_write()