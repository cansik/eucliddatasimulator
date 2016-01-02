from enum import Enum


class NodeType(Enum):
    normal = 1
    nested = 2
    split = 3


class StubInfo:
    def __init__(self, command, nodeType=NodeType.normal, isParallelSplit=False):
        self.command = command
        self.nodeType = nodeType
        self.isParallelSplit = isParallelSplit
        self.cores = int()
        self.ram = int()
        self.walltime = int()
        self.outputfiles = list()
        self.inputfiles = list()
        # todo: set right split part
        self.split_parts = 2

    def __eq__(self, other):
        return self.command == other.command

    def __hash__(self):
        return hash(self.command)
