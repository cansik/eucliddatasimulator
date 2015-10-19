__author__ = 'cansik'


class StubsGenerator(object):
    def __init__(self, package_definitions_directory, output_folder):
        self.package_definitions_directory = package_definitions_directory
        self.output_folder = output_folder

    def __load_executables(self):
        # read all executables out of the files in the package defs
        pass

    def __prepare_output_folder(self):
        # copy generators and euclidwf modules
        pass

    def __generate_executable(self, command, executable):
        # generate new executable
        pass

    def generate_stubs(self):
        self.__prepare_output_folder()

        executables = self.__load_executables()
        for command, executable in executables:
            self.__generate_executable(command, executable)
