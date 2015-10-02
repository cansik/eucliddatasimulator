'''
Created on Apr 16, 2015

@author: martin.melchior
'''

import datetime
import logging
import os
import sys
import traceback

from pydron.dataflow import graph
from pydron.interpreter.traverser import Traverser

from euclidwf.framework import context
from euclidwf.framework.context import CONTEXT, serializable, WORKDIR, LOGDIR
from euclidwf.framework.graph_builder import build_graph
from euclidwf.framework.node_callbacks import NodeCallbacks
from euclidwf.framework.workflow_dsl import load_pipeline_from_file
from euclidwf.utilities.error_handling import PipelineFrameworkError
from euclidwf.server.server_model import JobStatus, PipelineTaskRun, RunOutput

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


RUNID='runid'
STATUS='status'
CONFIG='config'
SUBMITTED='submitted'
PIPELINE='pipeline'
REPORT='report'
INPUTS='inputs'
OUTPUTS='outputs'
ERRORLOG='errlog'

EXECSTATUS_PENDING = 'PENDING'
EXECSTATUS_EXECUTING = 'EXECUTING'
EXECSTATUS_ABORTED = 'ABORTED'
EXECSTATUS_COMPLETED = 'COMPLETED'
EXECSTATUS_ERROR = 'ERROR'

class PipelineExecution():
    """
    Object to keep a reference to all information needed to perform a pipeline run,
    to launch and check status of the pipeline run and to inspect the reports on the results.
    """
    def __init__(self, runConfig, config, status=EXECSTATUS_PENDING):        
        self.config=config
        self.credentials=runConfig.credentials # object of type RunServerConfiguration
        self.runid=runConfig.runid
        self.pipelineScript=runConfig.pipelineScript
        self.pipelineDir=runConfig.pipelineDir
        self.path_to_script=os.path.join(runConfig.pipelineDir,self.pipelineScript)
        self.pkgRepository=runConfig.pkgRepository
        self.workdir=runConfig.workdir
        self.logdir=runConfig.logdir
        self.data=runConfig.inputDataPaths        
        self.status=status
        self.pipeline=None # will be set as the (top-level) function that defines the pipeline
        self.dataflow=None # will become the (static) dataflow graph representing the pipeline processing 
        self.outputs=None # will become a map providing the output portnames as keys and the paths to the ouput product files as values
        self.report=None
        self.stacktrace=None
        self.created=datetime.datetime.now()
        
        
    def initialize(self):
        '''
        First, configures PYTHONPATH so that the pipeline specification can be parsed.
        Then, loads pipeline and creates the design time graph.
        Finally, it prepares all for executing the pipeline by creating a runtime context
        and instantiating a graph traverser. 
        '''
        add_to_path([self.pkgRepository, self.pipelineDir])
        self.pipeline=load_pipeline_from_file(self.path_to_script)
        
        # build the design time dataflow graph
        self.dataflow = build_graph(self.pipeline)   
        
        # initialize the context
        self.data[CONTEXT]=context.create_context(self)
        
        # instantiate the traverser
        self.callbacks=NodeCallbacks(self.config, self.credentials, self.pkgRepository)
        self.traverser=Traverser(self.callbacks.schedule_refinement, self.callbacks.submit_task)

                
    def start(self):
        self.status=EXECSTATUS_EXECUTING
        d = self.traverser.execute(self.dataflow, self.data)
            
        def finalize(outputs):
            self.status=EXECSTATUS_COMPLETED
            aliases=self.dataflow.get_task_properties(graph.FINAL_TICK)['aliases']
            self.outputs={}
            for _name,_alias in aliases.iteritems():
                self.outputs[_alias]=outputs[_name]
            self.report=summary(self.traverser.get_graph())
            
        def failed(reason):
            self.status=EXECSTATUS_ERROR
            self.report=summary(self.traverser.get_graph())
            self.stacktrace=reason.getTraceback()
    
        d.addCallback(finalize)
        d.addErrback(failed)
        return d
   
   
    def get_status(self):
        return self.status
    
   
    def cancel(self):
        raise NotImplementedError("Cancel method not yet implemented.")


    def reset(self): 
        self.cancel()
        self.initialize()
        self.start()

           
    def todict(self):
        try:
            _dict = {RUNID:self.runid, 
                     CONFIG: self.config,
                     STATUS: str(self.status),
                     SUBMITTED: self.created.strftime("%A, %d. %B %Y %I:%M%p"),
                     PIPELINE: 
                        {'name': self.pipeline.func_name,
                         'version':'n/a',
                         'file':self.path_to_script},
                     REPORT:summary(self.traverser.get_graph())  
                    }
            if self.data:
                data = {k:v for k,v in self.data.iteritems() if k != CONTEXT}
                if CONTEXT in self.data.keys():
                    data[CONTEXT]=serializable(self.data[CONTEXT])
                _dict[INPUTS]=data
            if self.outputs:
                self.outputs[WORKDIR]=self.data[CONTEXT][WORKDIR]
                self.outputs[LOGDIR]=self.data[CONTEXT][LOGDIR]
                _dict[OUTPUTS]=self.outputs
            if self.stacktrace:
                _dict[ERRORLOG]=self.stacktrace
            return _dict
        except:
            _, _, exc_traceback = sys.exc_info()
            exc_msg=repr(traceback.extract_tb(exc_traceback))            
            logger.warn("Exception while dumping run object to dict - stacktrace: \n%s"%exc_msg)
            return None
        
        
    def get_jobs_status(self):
        if not self.traverser and not self.traverser.get_graph():
            return None
        graph = self.traverser.get_graph()
        jobs=[]
        for tick in sorted(graph.get_all_ticks()):
            props = graph.get_task_properties(tick)
            if 'summary' in props.keys() and props['summary']:
                try:
                    jobs.append(JobStatus(str(tick), props['summary'].status))
                except:
                    pass
        return jobs
    
    def get_task_runs(self):
        if not self.traverser and not self.traverser.get_graph():
            return None
        graph = self.traverser.get_graph()
        taskruns=[]
        for tick in sorted(graph.get_all_ticks()):
            props = graph.get_task_properties(tick)
            if 'summary' in props.keys() and props['summary'].workdir:
                summary=props['summary']
                taskrun=PipelineTaskRun("TODO:command", str(tick), summary.dfpath, summary.pid, 
                                        summary.status, os.path.join(summary.workdir, summary.dfpath), 
                                        "TODO:stdout", "TODO:stderr", "TODO:pkgreposid")
                taskruns.append(taskrun)
        return taskruns
    
    def get_outputs(self):
        if self.outputs:
            outputs = []
            for portname, datapath in self.outputs.iteritems():
                outputs.append(RunOutput(portname, "n/a", datapath))
            return outputs
        else:
            return None


    
    @classmethod
    def fromdict(cls, _dict):
        if RUNID not in _dict.keys():
            raise PipelineFrameworkError("Cannot load PipelineExecution object - runid not defined!")
        runid=_dict[RUNID]
        if PIPELINE not in _dict.keys():
            raise PipelineFrameworkError("Cannot load PipelineExecution object - pipeline script not defined!")
        script=_dict[PIPELINE]
        if CONFIG not in _dict.keys():
            raise PipelineFrameworkError("Cannot load PipelineExecution object - no configuration provided!")
        config=_dict[CONFIG]

        data={}
        if INPUTS in _dict.keys():
            data=_dict[INPUTS]
        if STATUS in _dict.keys():
            status = _dict[STATUS]
        else:
            status = EXECSTATUS_PENDING
        return PipelineExecution(runid, script, data, config, status)
    

def add_to_path(locations):
    if not locations:
        return
    for loc in locations:
        if os.path.exists(loc):
            if not '__init__.py' in os.listdir(loc):
                initpath = os.path.join(loc,'__init__.py')
                with open(initpath, 'w') as initfile:
                    initfile.write("# generated by pipeline framework")
            sys.path.append(loc)


def summary(graph):
    return [summary_entries(tick, graph) for tick in sorted(graph.get_all_ticks())]

        
def summary_entries(tick, graph):
    props = graph.get_task_properties(tick)
    if 'summary' in props.keys() and props['summary'].workdir:
        summary=props['summary']
        path=summary.dfpath
        status=summary.status
        time=summary.lapse_time
        pid=summary.pid
        wd=os.path.join(summary.workdir,path)
        return { 'tick': str(tick), 'path':path, 'pid':pid, 'status':status, 'time':time, 'workdir':wd }
    else:
        return { 'tick': str(tick), 'path':props['path'], 'pid':'n/a', 'status':'n/a', 'time':0.0, 'workdir':'n/a' }
        
