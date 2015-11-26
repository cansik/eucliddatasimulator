import os
import pickle
import random
import tempfile
import threading
from multiprocessing import Process
from time import sleep

import argparse
import sys
import uuid
from xml.dom import minidom

__author__ = 'cansik'

command = ''
input_names = []
output_names = []
resources = None
executable = None

inputs = {}
outputs = {}

junk_files = []

workdir = ''
file_data_dir = 'data'


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

    r.use_cpu(resources.cores)
    r.use_memory(resources.mem)

    # todo: include io
    r.use_io(0, 0)

    r.start(resources.walltime)


def read_input_files():
    for input_name, rel_path in inputs.items():
        absolute_path = os.path.join(workdir, rel_path)

        # read xml file
        xmldoc = minidom.parse(absolute_path)
        file_list = xmldoc.getElementsByTagName('FileName')
        file_names = map(lambda e: e.firstChild.data, file_list)

        for file_name in file_names:
            data_path = os.path.join(workdir, file_data_dir, file_name)
            data = ''
            with open(data_path, mode='rb') as blob:
                data = blob.read()

            print("read %s (%s bytes)" % (file_name, sys.getsizeof(data)))
            junk_files.append(data)


def write_output_files():
    for output_name, rel_path in outputs.items():
        product_id = create_product_id(output_name)
        filename = create_file_name()
        absolute_path = os.path.join(workdir, rel_path)
        parent_dir = os.path.dirname(absolute_path)

        if not os.path.exists(parent_dir):
            os.makedirs(parent_dir)

        # write xml
        with open(absolute_path, 'w') as outfile:
            outfile.write(META_DATA_XML % (product_id, filename))

        # write data file
        data_dir = os.path.join(workdir, file_data_dir)

        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        data_dir = os.path.join(data_dir, filename)

        with open(data_dir, 'w') as outfile:
            outfile.write('HELLO WORLD FROM %s' % output_name)
            for input_name, rel_input_path in inputs.iteritems():
                outfile.write("    (%s,%s)\n" % (input_name, rel_input_path))


def create_product_id(output_name):
    return "P_" + output_name + "_" + str(uuid.uuid4())


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


class RessourceUser(object):
    def __init__(self):
        self.wall_time = 0
        self.workload_threads = []
        self.timer_thread = None

        # default values
        self.cores = 1
        self.ram = 50
        self.file_size = 50
        self.file_count = 2

    def start(self, wall_time, is_blocking=True):
        self.wall_time = wall_time

        # start threads
        self.workload_threads.append(
            WorkloadThread(self.__use_cpu, self.cores))
        self.workload_threads.append(
            WorkloadThread(self.__use_memory, self.ram))
        self.workload_threads.append(
            WorkloadThread(self.__use_io, self.file_size, self.file_count))

        for t in self.workload_threads:
            t.start()

        # start timer thread
        self.timer_thread = WorkloadThread(self.__non_blocking_sleep,
                                           self.wall_time,
                                           self.workload_threads)

        if is_blocking:
            # blocking
            sleep(self.wall_time)
            self.__cleanup_threads(self.workload_threads)
        else:
            # non-blocking
            self.timer_thread.start()

    # ------ SETTER ------

    def use_cpu(self, cores):
        self.cores = cores

    def use_memory(self, ram):
        self.ram = ram

    def use_io(self, file_size, file_count):
        self.file_size = file_size
        self.file_count = file_count

    # ------ WORKLOAD METHODS ------

    @staticmethod
    def __use_memory(thread, ram_size):
        print("RAM start")
        data = bytearray(ram_size * 1024 * 1024)
        thread.wait_on_terminate()
        print("RAM end")

    @staticmethod
    def __use_io(thread, file_size, file_count):
        print("IO start")
        temp_files = []

        for i in range(file_count):
            temp = tempfile.NamedTemporaryFile()
            temp.write(bytearray(file_size * 1024 * 1024))
            temp.flush()
            temp_files.append(temp)

        thread.wait_on_terminate()

        for tmp in temp_files:
            tmp.close()
        print("IO end")

    @staticmethod
    def __use_cpu(thread, cores):
        print("CPU start")
        procs = []

        for c in range(cores):
            p = Process(target=RessourceUser.__cpu_calculator)
            procs.append(p)
            p.start()

        thread.wait_on_terminate()

        for p in procs:
            p.terminate()
        print("CPU end")

    @staticmethod
    def __cpu_calculator():
        while True:
            for k in range(1024 * 1024):
                r = random.randint(0, 9)
                i = r + r

    # ------ THREAD CONTROL ------

    @staticmethod
    def __non_blocking_sleep(thread, wall_time, thread_list):
        sleep(wall_time)
        # shutdown all threads
        RessourceUser.__cleanup_threads(thread_list)

    @staticmethod
    def __cleanup_threads(thread_list):
        # shutdown threads
        for t in thread_list:
            t.terminate()

        # wait on end
        for t in thread_list:
            t.join()


class WorkloadThread(threading.Thread):
    def __init__(self, function, *args, **kwargs):
        super(WorkloadThread, self).__init__()
        self.should_terminate = threading.Event()

        self.function = function
        self.args = args
        self.kwargs = kwargs

    def terminate(self):
        self.should_terminate.set()

    def wait_on_terminate(self):
        while not self.should_terminate.isSet():
            sleep(0.10)

    def run(self):
        self.function(self, *self.args, **self.kwargs)
