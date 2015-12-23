import json
import os
import pickle
import argparse
import errno

import uuid

files = {}

__DATA_DIR = 'data'

META_DATA_XML = """<?xml version="1.0" encoding="UTF-8"?>
<TestDataFiles>
    <Id>%s</Id>
    <Files>
        <DataContainer filestatus='COMMITTED'>
            <FileName>%s</FileName>
        </DataContainer>
    </Files>
</TestDataFiles>"""


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def read_data():
    global files
    files = pickle.loads(u"""{{mocks}}""")


def create_product_id(output_name):
    return "P_" + output_name + "_" + str(uuid.uuid4())


def create_file_name(output_name):
    return "FN_" + output_name + "_" + str(uuid.uuid4())


def parse_cmd_args():
    parser = argparse.ArgumentParser(
            description="Utility generating mock data for executables.")
    parser.add_argument("--destdir", default=".",
                        help="Directory to write the mock data into.")
    args = parser.parse_args()
    args.destdir = os.path.expandvars(args.destdir)

    return args


def generate_files(output_files, output_folder):
    # prepare output folder
    mkdir_p(os.path.join(output_folder, __DATA_DIR))

    for k, v in output_files.items():
        product_id = create_product_id(k)
        filename = create_file_name(k)

        xml_file_name = k + v['extension']
        xml_data = META_DATA_XML

        data_file_name = filename + v['extension']
        data_size = int(v['data_size'])

        xml_data = xml_data % (product_id, data_file_name)

        # write xml file
        print("writing %s..." % xml_file_name,)
        xml_path = os.path.join(output_folder, xml_file_name)

        with open(xml_path, 'w') as outfile:
            outfile.write(xml_data)

        print("writing data file %s..." % data_file_name)
        data_path = os.path.join(output_folder, __DATA_DIR, data_file_name)

        with open(data_path, 'w') as outfile:
            outfile.write(bytearray(data_size))


if __name__ == '__main__':
    print("Mock Generator Script")

    args = parse_cmd_args()

    read_data()
    generate_files(files, args.destdir)
    print("all files (%s) generated!" % len(files.keys()))
