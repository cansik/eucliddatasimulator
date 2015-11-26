import os
import unittest
from euclidwf.utilities import exec_loader
from euclid_stubs_generator.mock_generator import MockGenerator
from euclid_stubs_generator.stubs_generator import StubsGenerator


class BasicTest(unittest.TestCase):
    def setUp(self):
        self.input_folder = os.path.abspath(
            '../../euclidwf_examples/packages/pkgdefs')
        self.output_folder = os.path.abspath('../test_data/output')
        self.mock_output = os.path.join(self.output_folder, 'mock')

        self.workdir = os.path.join(self.mock_output, 'workdir')

        self.test_pipeline_name = 'vis_split_quadrants'

        print(self.input_folder)
        print(self.output_folder)

        self.generator = StubsGenerator(self.output_folder)
        self.mocker = MockGenerator(self.mock_output)

    def tearDown(self):
        # cleanup
        pass

    def test_generate_stubs(self):
        executables = exec_loader.get_all_executables(self.input_folder)
        executables = dict({(k, v) for k, v in executables.items() if
                            k == self.test_pipeline_name})

        self.generator.generate_stubs(executables,
                                      {self.test_pipeline_name: {
                                          'quadrants_list': 5}})

    def test_mock_generator(self):
        self.mocker.generate_mocks({'exposures': 3})

    def test_run_pipeline(self):
        test_script = os.path.join(self.output_folder,
                                   self.test_pipeline_name + ".py")

        result = os.system(
            "python %s --workdir %s --exposures %s --quadrants_list %s" %
            (test_script, self.workdir, 'exposures.data',
             'quadrants_list.data'))

        assert result == 0


if __name__ == '__main__':
    unittest.main()
