import os

import shutil
import stat

import pickle
# from euclidwf.utilities import exec_loader
from jinja2 import Template

from euclid_stubs_generator.ewf import exec_loader

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

    @staticmethod
    def __write_all_text(file_path, content):
        with open(file_path, 'w') as outputfile:
            outputfile.write(content)
        os.chmod(file_path, stat.S_IRWXU)

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
        else:
            os.mkdir(dest_dir)

        # copy executors to bin folder
        shutil.copytree(src_dir, dest_dir)

    def __generate_executable(self, command, executable):
        # generate new executable
        exec_file = os.path.join(self.output_folder, '%s.py' % command)

        # generate template
        output = self.template.render(command='"%s"' % command,
                                      input_files=map(lambda x: x.name, executable.inputs),
                                      output_files=map(lambda x: x.name, executable.outputs),
                                      resources=repr(executable.resources),
                                      executable=pickle.dumps(executable))

        # write output
        self.__write_all_text(exec_file, output)

    def generate_stubs(self):
        self.__prepare_output_folder()

        executables = self.__load_executables()

        for command, executable in executables.items():
            self.__generate_executable(command, executable)
