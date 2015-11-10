'''
Created on Apr 25, 2015

@author: martin.melchior
'''
from euclidwf.framework.workflow_dsl import invoke_task
import inspect
from euclidwf.utilities.error_handling import PipelineSpecificationError

class Package():
    def __init__(self, pkgname):
        self.pkgname=pkgname

    def __repr__(self):
        return "PACKAGE %s"%self.pkgname

    def __hash__(self):
        return hash(self.pkgname)

    def __eq__(self, other):
        if not isinstance(other, Package):
            return False
        if set(self.__dict__.keys()) != set(other.__dict__.keys()):
            return False
        for k,v in self.__dict__.iteritems():
            if v!=other.__dict__[k]:
                return False
        return True

    def __neq__(self,other):
        return not self.__eq__(other)

class ComputingResources():
    def __init__(self, cores=1, ram=1.0, walltime=1.0):
        self.cores=cores
        self.ram=ram
        self.walltime=walltime

    def __repr__(self):
        return "CORES=%s | RAM=%s | WALLTIME=%s"%(self.cores, self.ram, self.walltime)


class Executable():
    def __init__(self, command, inputs=[], outputs=[], resources=ComputingResources()):
        self.command=command
        self.inputs=self.input_array(inputs)
        self.outputs=self.output_array(outputs)
        self.resources=resources
        pkgname,pkgfile=_pkgname()
        self.pkgname=pkgname
        self.pkgfile=pkgfile


    def input_array(self, inputs):
        arr=[]
        for inp in inputs:
            if not isinstance(inp, Input) and isinstance(inp, basestring):
                arr.append(Input(inp))
            elif isinstance(inp, Input):
                arr.append(inp)
            else:
                raise PipelineSpecificationError("Inputs in executable definition must be either of type string or Input.")
        return arr


    def output_array(self, outputs):
        arr=[]
        for inp in outputs:
            if not isinstance(inp, Output) and isinstance(inp, basestring):
                arr.append(Output(inp))
            elif isinstance(inp, Output):
                arr.append(inp)
            else:
                raise PipelineSpecificationError("Outputs in Executable definition must be either of type string or Input.")
        return arr


    def __call__(self, **kwargs):
            inputnames=[inp.name for inp in self.inputs]
            outputnames=[out.name for out in self.outputs]
            props=TaskProperties(command=self.command, package=Package(self.pkgname))
            return invoke_task(props, inputnames, outputnames, **kwargs)


def _pkgname():
    frm = inspect.stack()[2]
    mod = inspect.getmodule(frm[0])
    return mod.__name__,mod.__file__


MIME_XML="xml"
MIME_TXT="txt"
TYPE_FILE="file"
TYPE_LISTFILE="listfile"

class Input():
    def __init__(self, inputname, dm_type=None, content_type=TYPE_FILE):
        self.name=inputname
        self.dm_type=dm_type

class Output():
    def __init__(self, outputname, dm_type=None, mime_type=MIME_XML, content_type=TYPE_FILE):
        self.name=outputname
        self.dm_type=dm_type
        self.mime_type=mime_type
        self.content_type=content_type

class TaskProperties(object):

    def __init__(self, command, package, **kwargs):
        self.command=command
        self.package=package
        for k,v in kwargs.iteritems():
            setattr(self,k,v)

    def __hash__(self):
        return hash(self.command)+hash(self.package)

    def __eq__(self, other):
        if not isinstance(other, TaskProperties):
            return False
        if set(self.__dict__.keys()) != set(other.__dict__.keys()):
            return False
        for k,v in self.__dict__.iteritems():
            if v!=other.__dict__[k]:
                return False
        return True

    def __neq__(self,other):
        return not self.__eq__(other)




