class StubInfo:
    def __init__(self, command, isParallelSplit=False):
        self.command = command
        self.isParallelSplit = isParallelSplit
        self.cores = int()
        self.ram = float()
        self.walltime = int()
        self.outputfiles = list()
        self.inputfiles = list()
