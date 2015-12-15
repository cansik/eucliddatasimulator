import json
import os
import pickle
import argparse
import errno

files = {}

__DATA_DIR = 'data'


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
    files = pickle.loads(u"""{{files}}""")


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
        xml_file_name = k
        xml_data = v['xml_data'].decode("hex")
        data_file_name = v['data_file_name']
        data_size = int(v['data_size'])

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
