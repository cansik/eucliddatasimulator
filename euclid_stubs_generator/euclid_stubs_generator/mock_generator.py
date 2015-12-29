import os
import pickle

from euclid_stubs_generator.stubs_template import create_product_id, create_file_name, create_xml_output
from euclid_stubs_generator.utils import mkdir_p, write_all_text, read_template


class MockGenerator:
    __DATA_DIR = 'data'
    __WORK_DIR = 'workdir'
    __EXTENSION = '.dat'
    __MOCK_SCRIPT_NAME = 'mock_script.py'

    def __init__(self, output_folder):
        self.output_folder = output_folder
        self.work_dir = os.path.join(self.output_folder, self.__WORK_DIR)
        self.data_dir = os.path.join(self.work_dir, self.__DATA_DIR)

    def __prepare_output_folder(self):
        mkdir_p(self.work_dir)
        mkdir_p(self.data_dir)

    def __create_mock_file(self, file_name, file_size, write_files=True):
        # generate id's
        product_id = create_product_id(file_name)
        data_file_name = create_file_name(file_name)

        # define paths
        xml_path = os.path.join(self.work_dir, file_name + self.__EXTENSION)
        data_path = os.path.join(self.data_dir,
                                 data_file_name + self.__EXTENSION)

        # define content
        xml_output = create_xml_output(product_id, [data_file_name + self.__EXTENSION])
        data_size = file_size * 1000 * 1000

        # create xml
        if write_files:
            with open(xml_path, 'w') as outfile:
                outfile.write(xml_output.toprettyxml(indent="    ", encoding="utf-8"))

            # data file
            with open(data_path, 'wb') as outfile:
                outfile.write(bytearray(data_size))

        return {file_name: {
            'extension': self.__EXTENSION,
            'data_size': data_size}}

    def generate_mocks(self, files):
        self.__prepare_output_folder()

        for k, v in files.items():
            self.__create_mock_file(k, v)

    def generate_script(self, files):
        self.__prepare_output_folder()

        mock_files = {}
        output_file_name = os.path.join(self.work_dir, self.__MOCK_SCRIPT_NAME)

        # generate file names and content
        for k, v in files.items():
            mock_files.update(self.__create_mock_file(k, v, write_files=False))

        # add files to script
        template = read_template('mock_script_template.py')
        output = template.render(mocks=pickle.dumps(mock_files))
        write_all_text(output_file_name, output)
