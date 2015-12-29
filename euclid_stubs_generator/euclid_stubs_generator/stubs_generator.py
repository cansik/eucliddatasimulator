import os

import shutil

import pickle
from euclidwf.utilities import exec_loader
from euclid_stubs_generator.utils import mkdir_p, read_template, write_all_text

__author__ = 'cansik'


class StubsGenerator:
    def __init__(self, output_folder):
        self.output_folder = output_folder
        self.template = read_template('stubs_template.py')

    def __load_executables(self, package_definitions_directory):
        # read all executables out of the files in the package defs
        executables = exec_loader.get_all_executables(package_definitions_directory)
        return executables

    def __prepare_output_folder(self):
        # copy executors and euclidwf modules
        src_dir = os.path.join(os.path.dirname(__file__), 'executors')
        dest_dir = os.path.join(self.output_folder, 'bin')

        # clear bin folder
        if os.path.exists(self.output_folder):
            shutil.rmtree(self.output_folder)
        else:
            # create if tree does not exist yet
            mkdir_p(self.output_folder)

        # copy executors to bin folder
        shutil.copytree(src_dir, dest_dir)

    def __generate_executable(self, command, stub_info):
        # generate new executable
        exec_file = os.path.join(self.output_folder, '%s' % command)

        # create stub info dict
        info_dict = stub_info.__dict__

        # pop nodeType for less dependency
        info_dict.pop('nodeType')

        # generate template
        output = self.template.render(stub_info=pickle.dumps(info_dict))

        # write output
        write_all_text(exec_file, output)

    def generate_stubs(self, stub_infos):
        """
        Generates stub executables for the euclid pipeline.
        :param stub_infos: [StubInfo]
        :return:
        """
        self.__prepare_output_folder()

        for stub_info in stub_infos:
            self.__generate_executable(stub_info.command, stub_info)
