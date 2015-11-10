import unittest

from euclid_stubs_generator.stubs_generator import StubsGenerator


class BasicTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        # cleanup
        pass

    def test_simple_run(self):
        generator = StubsGenerator('../../euclidwf_examples/packages/pkgdefs',
                                   '../test_data/output')
        generator.generate_stubs()


if __name__ == '__main__':
    unittest.main()
