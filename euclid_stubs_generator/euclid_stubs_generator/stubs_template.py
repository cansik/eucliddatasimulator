#!/usr/bin/env python
# -*- coding: utf-8 -*-

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

stub_info = None

junk_files = []

output_names = []

inputs = {}
outputs = {}

workdir = ''
file_data_dir = 'data'
extension = '.dat'


# dummy class for loading stub info
class Struct:
    def __init__(self, **entries):
        self.__dict__.update(entries)


class ResourceUser(object):
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
            p = Process(target=ResourceUser.__cpu_calculator)
            procs.append(p)
            p.start()

        thread.wait_on_terminate()

        for p in procs:
            p.terminate()
        print("CPU end")

    @staticmethod
    def __cpu_calculator():
        while True:
            for k in xrange(1024 * 1024):
                r = random.randint(0, 9)
                i = r + r

    # ------ THREAD CONTROL ------

    @staticmethod
    def __non_blocking_sleep(thread, wall_time, thread_list):
        sleep(wall_time)
        # shutdown all threads
        ResourceUser.__cleanup_threads(thread_list)

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


def read_data():
    global stub_info, output_names
    info_dict = pickle.loads("""{{stub_info}}""")
    stub_info = Struct(**info_dict)

    output_names = map(lambda x: x[0], stub_info.outputfiles)


def print_info():
    print('Name: %s' % stub_info.command)
    print('In: %s' % ', '.join(stub_info.inputfiles))
    print('Out: %s' % ', '.join(output_names))
    print('Resources:\nCores: %s\nWalltime: %s\nRam: %s' % (stub_info.cores, stub_info.walltime, stub_info.ram))
    print('Is Parallel Split: %s' % stub_info.isParallelSplit)


def workload_run():
    r = ResourceUser()

    r.use_cpu(int(stub_info.cores))
    r.use_memory(int(stub_info.ram))

    # todo: include io
    r.use_io(0, 0)

    r.start(stub_info.walltime)


def read_input_files():
    for input_name, rel_path in inputs.items():
        absolute_path = os.path.join(workdir, rel_path)

        # read xml file if is valid
        try:
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
        except Exception, e:
            # read just the file as junk
            with open(absolute_path, mode='rb') as blob:
                data = blob.read()

            print("read blob %s (%s bytes)" % (absolute_path, sys.getsizeof(data)))
            junk_files.append(data)


def write_output_files():
    counter = 0
    for output_name, rel_path in outputs.items():
        product_id = create_product_id(output_name)
        filename = create_file_name(stub_info.command) + extension
        absolute_path = os.path.join(workdir, rel_path)
        parent_dir = os.path.dirname(absolute_path)

        file_size = stub_info.outputfiles[counter][1]

        if not os.path.exists(parent_dir):
            os.makedirs(parent_dir)

        xml_output = create_xml_output(product_id, [filename])

        # write xml
        with open(absolute_path, 'w') as outfile:
            outfile.write(xml_output.toprettyxml(indent="    ", encoding="utf-8"))

        # write data file
        data_dir = os.path.join(workdir, file_data_dir)

        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        data_dir = os.path.join(data_dir, filename)

        with open(data_dir, 'wb') as outfile:
            outfile.write(bytearray(file_size * 1000 * 1000))
        counter += 1


def write_split_output():
    # calculate input size
    total_input_size = 0

    for junk in junk_files:
        total_input_size += sys.getsizeof(junk)

    part_size = int(total_input_size / stub_info.split_parts)

    print('part size: %s bytes' % part_size)

    # create parts
    split_part_list = []

    for i in range(stub_info.split_parts):
        # write data file
        data_dir = os.path.join(workdir, file_data_dir)

        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        filename = create_file_name('%s_PART_%s' % (stub_info.command, i)) + extension

        data_dir = os.path.join(data_dir, filename)

        relative_output_path = os.path.join(file_data_dir, filename)
        split_part_list.append(relative_output_path)

        with open(data_dir, 'wb') as outfile:
            outfile.write(bytearray(part_size))

    # write pickled list
    # todo: assumption that only one file is output for split!
    list_file_name = outputs.items()[0][0]
    list_path = os.path.join(workdir, list_file_name) + extension

    # write list file
    with open(list_path, 'w') as outfile:
        outfile.write(pickle.dumps(split_part_list))


def create_xml_output(product_id, file_list):
    xmldoc = minidom.Document()

    # create root elements
    root = xmldoc.createElement('TestDataFiles')

    id_element = xmldoc.createElement('Id')
    id_content = xmldoc.createTextNode(product_id)
    id_element.appendChild(id_content)

    files_element = xmldoc.createElement('Files')

    # create file elements
    for file_name in file_list:
        data_container = xmldoc.createElement('DataContainer')
        data_container.setAttribute('filestatus', 'COMMITTED')

        file_name_element = xmldoc.createElement('FileName')
        file_name_content = xmldoc.createTextNode(file_name)
        file_name_element.appendChild(file_name_content)

        data_container.appendChild(file_name_element)
        files_element.appendChild(data_container)

    # append elements
    root.appendChild(id_element)
    root.appendChild(files_element)
    xmldoc.appendChild(root)
    return xmldoc


def create_product_id(output_name):
    return "P_" + output_name + "_" + str(uuid.uuid4())


def create_file_name(output_name):
    return "FN_" + output_name + "_" + str(uuid.uuid4())


def parse_cmd_args():
    parser = argparse.ArgumentParser(
            description="Test Stub for Executable %s." % stub_info.command)
    parser.add_argument("--workdir", help="Workdir.", default=".")
    parser.add_argument("--logdir", help="Logdir.", default="./logdir")

    for inputname in stub_info.inputfiles:
        parser.add_argument("--%s" % inputname,
                            help="Relative path to input (%s) to tester." % inputname)
    for outputname in output_names:
        parser.add_argument("--%s" % outputname,
                            help="relative path to output (%s) to tester." % outputname)

    return parser.parse_args()


if __name__ == '__main__':
    read_data()
    args = parse_cmd_args()

    inputs = {value: getattr(args, value) for value in stub_info.inputfiles}
    outputs = {value: getattr(args, value) for value in output_names}

    workdir = args.workdir

    print_info()

    # test
    print("reading files...")
    read_input_files()

    print("startig workload test...")
    workload_run()

    if stub_info.isParallelSplit:
        print("creating split...")
        write_split_output()
    else:
        print("writing output files...")
        write_output_files()

    print("finished!")
