import os

from cfapyx import CFANetCDF

TESTDIR = 'cfapyx/tests/test_space'

class TestCFAWrite:
    def test_cfa_write(self, testdir=TESTDIR):


        os.system(f'touch {testdir.replace("/","_")}')

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