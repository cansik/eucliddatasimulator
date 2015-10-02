'''
Created on Apr 28, 2015

@author: martin.melchior
'''
import sys
import inspect
from euclidwf.framework import taskdefs
from euclidwf.utilities.error_handling import ConfigurationError
import os

def load_executable(taskname, pkgname):
    if pkgname in sys.modules.keys():
        pkg=sys.modules[pkgname]
    else:
        pkg = load_package(pkgname)
        
    if not hasattr(pkg, taskname):
        raise ConfigurationError("No executable with name %s found in package %s."%(taskname, pkgname))

    return getattr(pkg, taskname)


def load_package(pkgname, doreload=True):
    if pkgname in sys.modules.keys():
        pkg = sys.modules[pkgname]
        if not reload:
            return pkg
        else:
            return reload(pkg)
    try:
        return __import__(pkgname)
    except:
        raise ConfigurationError("Package with name %s not found - maybe the package repository is not on the python path."%(pkgname))


def get_all_executables(pkgrepos):
    if not '__init__.py' in os.listdir(pkgrepos):
        initpath = os.path.join(pkgrepos,'__init__.py')
        with open(initpath, 'w') as initfile:
            initfile.write("# generated by pipeline framework")
    if not pkgrepos in sys.path:
        sys.path.append(pkgrepos)
    executables = {}
    for f in os.listdir(pkgrepos):
        fpath=os.path.join(pkgrepos,f)
        if os.path.isfile(fpath) and f.endswith('.py'):
            pkgname=f[:-3]
            pkg=load_package(pkgname)
            for _name,_obj in inspect.getmembers(pkg):
                if isinstance(_obj,taskdefs.Executable):
                    executables[_name]=_obj
    return executables    
    
    
if __name__ == '__main__':
    pkgrepos="/Users/martinm/Projects/euclid/pipeline_framework/prototype/wfm/trunk/euclidwf_examples/packages/pkgdefs"
    execs=get_all_executables(pkgrepos)
    for k,v in execs.iteritems():
        print "Pkg %s: %s (%s)"%(v.pkgname, v.command, v.pkgfile)
