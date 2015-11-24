import pickle
import random
import math
from multiprocessing import Process
from time import sleep

__author__ = 'cansik'

command = ''
input_files = []
output_files = []
resources = None
executable = None


def read_data():
    global command, input_files, output_files, resources, executable
    executable = pickle.loads("""{{executable}}""")

    # link vars
    command = executable.command
    input_files = map(lambda x: x.name, executable.inputs)
    output_files = map(lambda x: x.name, executable.outputs)
    resources = executable.resources


def print_info():
    print('Name: %s' % command)
    print('In: %s' % ', '.join(input_files))
    print('Out: %s' % ', '.join(output_files))
    print('Resources: %s' % executable.resources)


def workload_run():
    r = RessourceUser()
    r.use_cpu(resources.cores, int(resources.walltime*60))


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


if __name__ == '__main__':
    read_data()
    print_info()
    workload_run()
