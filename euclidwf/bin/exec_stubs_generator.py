#!/usr/bin/env python
'''
Created on Apr 25, 2015

@author: martin.melchior
'''
import argparse
import os
import pickle
import shutil
import stat
import uuid

from shutil import copytree, ignore_patterns, rmtree
from os.path import dirname, join, isdir, isfile

from euclidwf.framework.taskdefs import TYPE_LISTFILE
from euclidwf.utilities import exec_loader
import euclidwf


def parse_cmd_args():
    parser = argparse.ArgumentParser(description="Utility generating test stubs for executables.")
    parser.add_argument("--pkgdefs", help="Path to folder that contains the package definitions (package repository).")
    parser.add_argument("--destdir", help="Directory to write the test stubs to.")
    parser.add_argument("--xml", dest='xml', action='store_true', help="Specify flag to generate xml output (otherwise text is produced; note that lists are always pickled).")
    args = parser.parse_args()
    args.pkgdefs=os.path.expandvars(args.pkgdefs)
    args.destdir=os.path.expandvars(args.destdir)
    
    return args


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


OUTPUTLISTLENGTH = 3

class TestExecListOutput(object):
    
    def __init__(self, executable, package, attributes=None):
        self.executable = executable
        self.package = package
        self.attributes = attributes
        
    def execute(self, workdir, logdir, inputs, outputs, details=True):
        _, reloutpath = outputs.iteritems().next()
        workdir = os.path.abspath(workdir)
        listoutpath = os.path.join(workdir, reloutpath)
        parentdir = os.path.dirname(listoutpath)
        if not os.path.exists(parentdir):
            os.makedirs(parentdir)
        relpath, _ = os.path.splitext(reloutpath)
        abspath = os.path.join(workdir, relpath)

        outputlist = []
        for i in range(OUTPUTLISTLENGTH):
            outputlist.append("%s_%i" % (relpath, i + 1))

        with open(listoutpath, "w") as listfile:
            pickle.dump(outputlist, listfile)
        
        for i in range(OUTPUTLISTLENGTH):        
            with open("%s_%i" % (abspath, i + 1), 'w') as outfile:
                outfile.write("**************************************************************************\n")
                outfile.write("TEST DATA SPLIT BY TEST STUB %s.\n" % self.executable)
                outfile.write("ELEMENT: %i\n" % (i + 1))
                outfile.write("Referenced within the listfile %s.\n" % listoutpath)
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


class TestExecXmlOutput(object):
    
    def __init__(self, executable, package, attributes=None):
        self.executable = executable
        self.package = package
        self.attributes = attributes
        
    def execute(self, workdir, logdir, inputs, outputs, details=True):
        for outputname, reloutpath in outputs.iteritems():
            productid = self.createProductId(outputname)
            filename = self.createFileName()            
            absoutpath = os.path.join(workdir, reloutpath)
            parentdir = os.path.dirname(absoutpath)
            if not os.path.exists(parentdir):
                os.makedirs(parentdir)
            with open(absoutpath, 'w') as outfile:
                outfile.write(XMLOUTPUT % (productid, filename))
            datadir = os.path.join(workdir, 'data')            
            if not os.path.exists(datadir):
                os.makedirs(datadir)
            with open(os.path.join(datadir, filename), 'w') as outfile:
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


    def createProductId(self, outputname):
        return "P_xxx_" + outputname + "_" + str(uuid.uuid4())
    
    def createFileName(self):
        return "FN_xxx_" + self.executable + "_" + str(uuid.uuid4())
        

XMLOUTPUT = '''<?xml version="1.0" encoding="UTF-8"?>
<TestDataFiles xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <Id>%s</Id>
  <Files>
    <DataContainer filestatus='COMMITTED'>
       <FileName>%s</FileName>
    </DataContainer>
  </Files>
</TestDataFiles>
'''
                
def _append_to_file(outfile, inputname, infile):
    outfile.write("    -------------------------------------\n")
    outfile.write("    Input (port: %s):\n" % inputname)
    outfile.write("    -------------------------------------\n")
    outfile.write("    -------------------------------------\n")
    for line in infile:
        outfile.write("        %s" % line)
        

def create_test_exec(executable, outputpath, testexec='TestExec'):
    command = executable.command
    inputnames = tuple(["%s" % _input.name for _input in executable.inputs])
    outputnames = tuple(["%s" % _output.name for _output in executable.outputs])
    # todo: What is testexec?
    content = TEST_EXEC_TEMPLATE % ("'%s'" % command, "'%s'" % executable.pkgname, inputnames, outputnames, testexec)
    with open(outputpath, 'w') as outputfile:
        outputfile.write(content)
    os.chmod(outputpath, stat.S_IRWXU)
    

def copy_modules_recursive(destdir, module, *ignore):
    srcdir = dirname(module.__file__)
    destdir = join(destdir, module.__name__)
    ignorepattern = ignore_patterns(ignore)
    # Since copytree tries to create the destdir and fails with an error if it exists
    if isdir(destdir):
        rmtree(destdir)
    copytree(srcdir, destdir, ignorepattern)


def copy_generators(destdir):
    destdir = destdir + "/euclidwf/bin"
    srcdir = os.path.dirname(__file__)
    if isdir(destdir):
        rmtree(destdir)
    copytree(srcdir, destdir)
    init_file = destdir + "/__init__.py"
    if not isfile(init_file):
        with open(init_file,'w') as f:
            f.write("# generated by EPR")


def copy_module(module_name, module_path, destdir):
    abspath = module_path
    relpath = module_name.replace(".", "/") + ".py"
    destpath = os.path.join(destdir, relpath)
    dest_parent = os.path.dirname(destpath)
    if not os.path.exists(dest_parent):
        os.makedirs(dest_parent)
    shutil.copy2(abspath, destpath)


def check_output_for_list(_exec):
    list_outputs = filter(lambda o: o.content_type == TYPE_LISTFILE, _exec.outputs)

    # todo: What does this line mean?
    if list_outputs and len(_exec.outputs) > 1:
        raise ValueError("No stub can be generated for executables that have more than one output \n" +
                            " and one of the outputs is a list.")
    else:
        return len(list_outputs) > 0 
    

def main():

    # todo:
    # destination dir vorbereiten (kopiere generatoren und dateien vom euclidwf)
    # executables laden und parsen
    # fuer jedes executable einen ordner machen
    # erstelle test exec Datei

    args = parse_cmd_args()
    destdir = args.destdir
    executables = exec_loader.get_all_executables(args.pkgdefs)
    # Be aware that copy_modules_recursive will remove the destdir if it exists

    # copy euclidwf source files to destination dir
    copy_modules_recursive(destdir, euclidwf, "*.pyc", ".svn")
    # todo: Why all generators?
    copy_generators(destdir)
    for command, executable in executables.iteritems():
        execpath = os.path.join(destdir, command)            
        if not os.path.exists(destdir):
            os.makedirs(destdir)
        if check_output_for_list(executable):
            create_test_exec(executable, execpath, 'TestExecListOutput')
        else:
            tester = 'TestExecXmlOutput' if args.xml else 'TestExec'
            create_test_exec(executable, execpath, tester)


TEST_EXEC_TEMPLATE = \
'''#!/usr/bin/env python
\'''
Test wrapper generated from package definition.
Version: 0.1
\''' 
import argparse
import os
from euclidwf.bin.exec_stubs_generator import TestExec, TestExecXmlOutput, TestExecListOutput

command=%s
pkgname=%s
inputnames=%s
outputnames=%s

def parse_cmd_args():
    parser = argparse.ArgumentParser(description="Test Stub for Executable %%s."%%command)
    parser.add_argument("--workdir", help="Workdir.", default=".")
    parser.add_argument("--logdir", help="Logdir.", default="./logdir")
    for inputname in inputnames:
        parser.add_argument("--%%s"%%inputname, help="Relative path to input (%%s) to tester."%%inputname)
    for outputname in outputnames:
        parser.add_argument("--%%s"%%outputname, help="relative path to output (%%s) to tester."%%outputname)
    args = parser.parse_args()    
    return args

def main():
    args = parse_cmd_args()
    tester=%s(command, pkgname)
    inputs={}
    for inputname in inputnames:
        inputs[inputname]=getattr(args,inputname)
    outputs={}
    for outputname in outputnames:
        outputs[outputname]=getattr(args,outputname)
    tester.execute(args.workdir, args.logdir, inputs, outputs)


if __name__ == '__main__':
    main()

'''
   

if __name__ == '__main__':
    main()
