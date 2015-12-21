import os
import unittest

from euclid_stubs_generator.stub_info import StubInfo
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

        self.test_xml_exec = 'vis_correct_dark'
        self.test_list_exec = 'vis_split_quadrants'

        print(self.input_folder)
        print(self.output_folder)

        self.generator = StubsGenerator(self.output_folder)
        self.mocker = MockGenerator(self.mock_output)

    def tearDown(self):
        # cleanup
        pass

    def test_a_generate_xml_stub(self):
        info = StubInfo(self.test_xml_exec)
        info.outputfiles = [('quadrant', 2), ('master_dark', 2), ('control_params', 2)]
        info.inputfiles = ['corrected_frame', 'hot_pixels_map']

        info.ram = 10
        info.walltime = 2
        info.cores = 1

        self.generator.generate_stubs([info])

    def test_b_mock_script_generator(self):
        self.mocker.generate_script({'corrected_frame': 3, 'hot_pixels_map': 3})

    def test_c_run_mock_script(self):
        mock_script = os.path.join(self.workdir, "mock_script.py")

        result = os.system("python %s --destdir %s" % (mock_script, self.workdir))

        assert result == 0

    def test_d_run_xml_exec(self):
        test_script = os.path.join(self.output_folder,
                                   self.test_xml_exec + ".py")

        result = os.system(
                "python %s --workdir %s"
                " --corrected_frame %s --hot_pixels_map %s --quadrant %s --master_dark %s --control_params %s" %
                (test_script, self.workdir, 'corrected_frame.data', 'hot_pixels_map.data', 'quadrant.data',
                 'master_dark.data', 'control_params.data'))

        assert result == 0

    def test_e_generate_list_stub(self):
        info = StubInfo(self.test_list_exec)
        info.outputfiles = [('quadrants_list', 5)]
        info.inputfiles = ['exposures']
        info.isParallelSplit = True

        info.ram = 10
        info.walltime = 2
        info.cores = 1

        self.generator.generate_stubs([info])

    def test_f_mock_generator(self):
        self.mocker.generate_mocks({'exposures': 3})

    def test_g_run_list_exec(self):
        test_script = os.path.join(self.output_folder,
                                   self.test_list_exec + ".py")

        result = os.system(
                "python %s --workdir %s --exposures %s --quadrants_list %s" %
                (test_script, self.workdir, 'exposures.data',
                 'quadrants_list.data'))

        assert result == 0


if __name__ == '__main__':
    unittest.main()
