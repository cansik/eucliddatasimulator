# -*- coding: utf-8 -*-
import random
import math
from multiprocessing import Process
from time import sleep


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
        self.test = True

        print("walltime end")

        for p in procs:
            p.terminate()
        print("CPU end")

    def use_memory(self, ram_size, seconds):
        self.abc(ram_size,seconds)

    def createStringWithSize(self, ram_size):
        print("Starting RAM")
        s = u""
        roundedInt = math.floor(ram_size*131072)
        for i in range(0, int(roundedInt)):
            s += "a"

        print self.utf8len(s)
        print "Created a string with: "+ (self.utf8len(s)/131072).__str__() +" Mb!"


    def abc(self, memory, seconds):
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
            print("forloop start")
            #for n in range(1024 * 1024):
            #   for j in range(1024*1024):
            for k in range(1024*1024):
                r = random.randint(0, 9)
                i = r+r
            print("forloop finish")

