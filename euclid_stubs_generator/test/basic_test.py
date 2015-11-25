import os
import unittest

from euclid_stubs_generator.stubs_generator import StubsGenerator


class BasicTest(unittest.TestCase):
    def setUp(self):
        self.input_folder = os.path.abspath('../../euclidwf_examples/packages/pkgdefs')
        self.output_folder = os.path.abspath('../test_data/output')

        print(self.input_folder)
        print(self.output_folder)

        self.generator = StubsGenerator(self.output_folder)

    def tearDown(self):
        # cleanup
        pass

    def test_generate_stubs(self):
        self.generator.generate_stubs_from_folder(self.input_folder)

if __name__ == '__main__':
    unittest.main()
