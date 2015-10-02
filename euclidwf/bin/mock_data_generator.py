#!/usr/bin/env python
'''
Created on Apr 25, 2015

@author: martin.melchior
'''
import argparse
import os
import sys

from pydron.dataflow.graph import START_TICK

from euclidwf.framework.context import CONTEXT
from euclidwf.framework.graph_builder import PydronGraphBuilder
from euclidwf.framework import workflow_dsl


def parse_cmd_args():
    parser = argparse.ArgumentParser(description="Utility generating mock data for testing.")
    parser.add_argument("--pipelinefile", help="Path to the file with the pipeline specification.")
    parser.add_argument("--pkgdefs", help="Directory with the Package Definitions (will be added to PYTHONPATH).")
    parser.add_argument("--destdir", help="Directory to write the test data to.")
    parser.add_argument("--workdir", help="Workdir mentioned in the pipeline input data file.")
    args = parser.parse_args()
    args.pipelinefile=os.path.expandvars(args.pipelinefile)
    args.pkgdefs=os.path.expandvars(args.pkgdefs)
    args.destdir=os.path.expandvars(args.destdir)
    
    return args

def create_mock_data(inputname, consumers, destdir):
    if not os.path.exists(destdir):
        os.makedirs(destdir)
    inputfilename=inputname+".dat"
    inputfilepath=os.path.join(destdir, inputfilename)
    with open(inputfilepath, 'w') as inputfile:
        inputfile.write("Mock data for input port %s.\n"%inputname)
        for portname,nodepath in consumers:
            inputfile.write("Consumed by node %s at port %s.\n"%(nodepath,portname))
    return inputfilename
            

def get_mock_inputs(pipeline_spec, destdir):
    pipeline_spec.isroot=True
    builder=PydronGraphBuilder(pipeline_spec)
    builder.build()
    inputs={}
    for source,dest in builder.graph.get_out_connections(START_TICK):
        if source.port==CONTEXT:
            continue
        if source.port not in inputs.keys():
            inputs[source.port]=[]
        nodepath=builder.graph.get_task_properties(dest.tick)['path']
        inputs[source.port].append((dest.port,nodepath))
    
    for inputname, consumers in inputs.iteritems():
        inputs[inputname]=create_mock_data(inputname, consumers, destdir)
    
    return inputs


def create_pipeline_data_file(inputs, filename, destdir, workdir):
    inputfilepath=os.path.join(destdir,filename)+".dat"
    with open(inputfilepath, 'w') as inputfile:
        inputfile.write("workdir=%s\n"%workdir)
        inputfile.write("logdir=logdir\n")
        for inputname, fn in inputs.iteritems():
            inputfile.write("%s=%s\n"%(inputname,fn))
            

def add_to_path(locations):
    if not locations:
        return
    for loc in locations:
        sys.path.append(loc)


def main():
    args = parse_cmd_args()
    pipelinedir=os.path.dirname(args.pipelinefile)
    pipelinename,_=os.path.splitext(os.path.basename(args.pipelinefile))
    add_to_path([args.pkgdefs,pipelinedir])
    spec=workflow_dsl.load_pipeline_from_file(args.pipelinefile)
    inputs=get_mock_inputs(spec, args.destdir)
    create_pipeline_data_file(inputs, pipelinename, args.destdir, args.workdir)
    


if __name__ == '__main__':
    main()
