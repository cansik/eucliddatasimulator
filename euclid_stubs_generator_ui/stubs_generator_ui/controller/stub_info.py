import json

class StubInfo():
    def __init__(self, command, executable):
        self.command = command
        self.cores = executable.resources.cores
        self.ram = executable.resources.ram
        self.walltime = executable.resources.walltime

