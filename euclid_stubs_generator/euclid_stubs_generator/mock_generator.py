import os

from euclid_stubs_generator.stubs_template import create_product_id, \
    META_DATA_XML, create_file_name
from euclid_stubs_generator.utils import mkdir_p


class MockGenerator:
    __DATA_DIR = 'data'
    __WORK_DIR = 'workdir'
    __EXTENSION = '.dat'

    def __init__(self, output_folder):
        self.output_folder = output_folder
        self.work_dir = os.path.join(self.output_folder, self.__WORK_DIR)
        self.data_dir = os.path.join(self.work_dir, self.__DATA_DIR)

    def __prepare_output_folder(self):
        mkdir_p(self.work_dir)
        mkdir_p(self.data_dir)

    def __create_mock_file(self, file_name, file_size):
        # generate id's
        product_id = create_product_id(file_name)
        data_file_name = create_file_name(file_name)

        # define paths
        xml_path = os.path.join(self.work_dir, file_name + self.__EXTENSION)
        data_path = os.path.join(self.data_dir, data_file_name + self.__EXTENSION)

        # create xml
        with open(xml_path, 'w') as outfile:
            outfile.write(META_DATA_XML % (product_id, data_file_name))

        # data file
        with open(data_path, 'wb') as outfile:
            outfile.write(bytearray(file_size * 1000 * 1000))

    def generate_mocks(self, files):
        self.__prepare_output_folder()

        for k, v in files.items():
            self.__create_mock_file(k, v)
