# -*- coding: utf-8 -*-
import random
import tempfile
import threading
from multiprocessing import Process
from time import sleep


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
