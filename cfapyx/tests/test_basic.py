import os
import logging

logger = logging.getLogger(__name__)
TESTDIR = 'cfapyx/tests/test_space'

class TestPath:
    def testpath(self, tests=TESTDIR):
        logger.error(os.getcwd())
        logger.error(tests)

class TestCleanup:
    def test_cleanup(self, testdir=TESTDIR):
        os.system(f'rm {testdir}/testrain.nca')
        print('Integration tests: Cleanup - complete')