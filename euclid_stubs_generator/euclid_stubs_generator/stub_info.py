class StubInfo:
    def __init__(self, command, isParallelSplit=False):
        self.command = command
        self.isParallelSplit = isParallelSplit
        self.cores = int()
        self.ram = float()
        self.walltime = int()
        self.outputfiles = list()
        self.inputfiles = list()
        # todo: set right split part
        self.split_parts = 2

    def __eq__(self, other):
        return self.command == other.command

    def __hash__(self):
        return hash(self.command)
