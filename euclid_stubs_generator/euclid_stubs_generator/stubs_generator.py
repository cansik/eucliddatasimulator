import os

import shutil
from euclidwf.utilities import exec_loader
from jinja2 import Template

__author__ = 'cansik'


class StubsGenerator(object):
    def __init__(self, package_definitions_directory, output_folder):
        self.package_definitions_directory = package_definitions_directory
        self.output_folder = output_folder
        self.template = self.__read_template()

    @staticmethod
    def __read_template():
        template_path = os.path.join(os.path.dirname(__file__), 'stubs_template.py')
        with open(template_path, 'r') as template_file:
            data = template_file.read()
            return Template(data)

    def __load_executables(self):
        # read all executables out of the files in the package defs
        executables = exec_loader.get_all_executables(self.package_definitions_directory)
        return executables

    def __prepare_output_folder(self):
        # copy executors and euclidwf modules
        src_dir = os.path.join(os.path.dirname(__file__), 'executors')
        dest_dir = os.path.join(self.output_folder, 'bin')

        # clear bin folder
        if os.path.exists(dest_dir):
            shutil.rmtree(dest_dir)

        # copy executors to bin folder
        shutil.copytree(src_dir, dest_dir)

    def __generate_executable(self, command, executable):
        # generate new executable
        print(command)
        pass

    def generate_stubs(self):
        self.__prepare_output_folder()

        executables = self.__load_executables()

        for command, executable in executables.items():
            self.__generate_executable(command, executable)
