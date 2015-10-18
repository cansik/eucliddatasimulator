import random
from multiprocessing import Process


class RessourceUser(object):
    def __init__(self):
        pass

    def use_cpu(self, cores):
        procs = []

        for c in range(cores):
            p = Process(target=self.__cpu_calculator)
            procs.append(p)
            p.start()

        for p in procs:
            p.join()

    def use_memory(self, ram_size):
        pass

    def use_io(self, file_size, file_count):
        pass

    @staticmethod
    def __cpu_calculator():
        for n in range(1024 * 1024):
            r = random.randint(0, 9)
            i = pow(r, r, r)