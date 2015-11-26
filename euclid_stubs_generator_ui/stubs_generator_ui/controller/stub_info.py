from euclidwf.framework.taskdefs import ComputingResources


class StubInfo():
    def __init__(self, command, executable):
        self.command = command
        self.inputs = executable.inputs
        self.outputs = executable.outputs
        self.resources = executable.resources

