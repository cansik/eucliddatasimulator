import os

import shutil
import stat

import pickle
from euclidwf.utilities import exec_loader
from jinja2 import Template

from euclid_stubs_generator.utils import mkdir_p

__author__ = 'cansik'


class StubsGenerator(object):
    def __init__(self, output_folder):
        self.output_folder = output_folder
        self.template = self.__read_template()

    @staticmethod
    def __read_template():
        template_path = os.path.join(os.path.dirname(__file__), 'stubs_template.py')
        with open(template_path, 'r') as template_file:
            data = template_file.read()
            return Template(data)

    @staticmethod
    def __write_all_text(file_path, content):
        with open(file_path, 'w') as outputfile:
            outputfile.write(content)
        os.chmod(file_path, stat.S_IRWXU)

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

    def __generate_executable(self, command, executable):
        # generate new executable
        exec_file = os.path.join(self.output_folder, '%s.py' % command)

        # generate template
        output = self.template.render(executable=pickle.dumps(executable))

        # write output
        self.__write_all_text(exec_file, output)

    def generate_stubs_from_folder(self, package_definitions_directory):
        executables = self.__load_executables(package_definitions_directory)
        self.generate_stubs(executables)

    def generate_stubs(self, executables):
        self.__prepare_output_folder()

        for command, executable in executables.items():
            self.__generate_executable(command, executable)

