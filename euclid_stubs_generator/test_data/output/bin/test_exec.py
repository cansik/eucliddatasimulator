import os


class TestExec(object):

    def __init__(self, executable, package, attributes=None, *params):
        self.executable = executable
        self.package = package
        self.attributes = attributes

    def execute(self, workdir, logdir, inputs, outputs, details=True):
        for outputname, reloutpath in outputs.iteritems():
            absoutpath = os.path.join(workdir, reloutpath)
            parentdir = os.path.dirname(absoutpath)
            if not os.path.exists(parentdir):
                os.makedirs(parentdir)
            with open(absoutpath, 'w') as outfile:
                outfile.write("**************************************************************************\n")
                outfile.write("TEST STUB FOR %s - OUTPUT PORT: %s.\n" % (self.executable, outputname))
                outfile.write("**************************************************************************\n")
                outfile.write("Workdir: %s\n" % workdir)
                outfile.write("Logdir: %s\n" % logdir)
                outfile.write("Inputs:\n")
                for inputname, relinpath in inputs.iteritems():
                    outfile.write("   (%s,%s)\n" % (inputname, relinpath))
                if details:
                    outfile.write("Details:\n")
                    for inputname, relinpath in inputs.iteritems():
                        absinpath = os.path.join(workdir, relinpath)
                        with open(absinpath, 'r') as infile:
                            _append_to_file(outfile, inputname, infile)