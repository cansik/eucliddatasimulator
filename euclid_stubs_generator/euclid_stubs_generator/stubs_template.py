import os
import pickle
import random
import math
from multiprocessing import Process
from time import sleep

import argparse
import uuid

__author__ = 'cansik'

command = ''
input_names = []
output_names = []
resources = None
executable = None

inputs = {}
outputs = {}

workdir = ''


def read_data():
    global command, input_names, output_names, resources, executable
    executable = pickle.loads("""{{executable}}""")

    # link vars
    command = executable.command
    input_names = map(lambda x: x.name, executable.inputs)
    output_names = map(lambda x: x.name, executable.outputs)
    resources = executable.resources


def print_info():
    print('Name: %s' % command)
    print('In: %s' % ', '.join(input_names))
    print('Out: %s' % ', '.join(output_names))
    print('Resources: %s' % executable.resources)


def workload_run():
    r = RessourceUser()
    r.use_cpu(resources.cores, int(resources.walltime * 60))


def read_input_files():
    pass


def write_output_files():
    for output_name, reloutpath in outputs.items():
        productid = create_product_id(output_name)
        filename = create_file_name()
        absoutpath = os.path.join(workdir, reloutpath)
        parentdir = os.path.dirname(absoutpath)

        if not os.path.exists(parentdir):
            os.makedirs(parentdir)

        # write xml
        with open(absoutpath, 'w') as outfile:
            outfile.write(META_DATA_XML % (productid, filename))

        # write data file
        datadir = os.path.join(workdir, 'data')

        if not os.path.exists(datadir):
            os.makedirs(datadir)

        datadir = os.path.join(datadir, filename)

        with open(datadir, 'w') as outfile:
            outfile.write('HELLO WORLD FROM %s' % output_name)
            for inputname, relinpath in inputs.iteritems():
                outfile.write("    (%s,%s)\n" % (inputname, relinpath))


def create_product_id(outputname):
    return "P_" + outputname + "_" + str(uuid.uuid4())


def create_file_name():
    return "FN_" + command + "_" + str(uuid.uuid4())


def parse_cmd_args():
    parser = argparse.ArgumentParser(
        description="Test Stub for Executable %s." % command)
    parser.add_argument("--workdir", help="Workdir.", default=".")
    parser.add_argument("--logdir", help="Logdir.", default="./logdir")

    for inputname in input_names:
        parser.add_argument("--%s" % inputname,
                            help="Relative path to input (%s) to tester." % inputname)
    for outputname in output_names:
        parser.add_argument("--%s" % outputname,
                            help="relative path to output (%s) to tester." % outputname)

    return parser.parse_args()


class RessourceUser(object):
    def __init__(self):
        pass

    def use_cpu(self, cores, seconds):
        print("CPU start")
        procs = []

        for c in range(cores):
            p = Process(target=self.__cpu_calculator)
            procs.append(p)
            p.start()

        sleep(seconds)

        print("walltime end")

        for p in procs:
            p.terminate()
        print("CPU end")

    def use_memory(self, ram_size, seconds):
        self.use_m(ram_size, seconds)

    def _create_string_with_size(self, ram_size):
        print("Starting RAM")
        s = u""
        roundedInt = math.floor(ram_size * 131072)
        for i in range(0, int(roundedInt)):
            s += "a"

        print self.utf8len(s)
        print "Created a string with: " + (
            self.utf8len(s) / 131072).__str__() + " Mb!"

    def use_m(self, memory, seconds):
        print("Starting RAM")
        data = bytearray(memory * 1024 * 1024)
        sleep(seconds)
        print("End RAM")

    def use_io(self, file_size, file_count):
        pass

    def utf8len(self, s):
        return len(s.encode('utf-8'))

    def __cpu_calculator(self):
        while True:
            for k in range(1024 * 1024):
                r = random.randint(0, 9)
                i = r + r


META_DATA_XML = """<?xml version="1.0" encoding="UTF-8"?>
<TestDataFiles>
    <Id>%s</Id>
    <Files>
        <DataContainer filestatus='COMMITTED'>
            <FileName>%s</FileName>
        </DataContainer>
    </Files>
</TestDataFiles>"""

if __name__ == '__main__':
    read_data()
    args = parse_cmd_args()

    inputs = {value: getattr(args, value) for value in input_names}
    outputs = {value: getattr(args, value) for value in output_names}

    workdir = args.workdir

    print_info()

    # test
    print("reading files...")
    read_input_files()

    print("startig workload test...")
    # workload_run()

    print("writing output files...")
    write_output_files()

    print("finished!")
