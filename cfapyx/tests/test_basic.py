import logging
import os

logger = logging.getLogger(__name__)
TESTDIR = 'cfapyx/tests/test_space'

class TestPath:
    def testpath(self, tests=TESTDIR):
        logger.error(os.getcwd())
        logger.error(tests)