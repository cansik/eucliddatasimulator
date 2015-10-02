'''
Created on Jun 25, 2015

@author: martin.melchior
'''
import tempfile
from inspect import isfunction
import os
import sys
import unittest

from twisted.internet import defer
from twisted.python.failure import Failure
from pydron.dataflow.graph import Graph

from euclidwf.framework.context import CONTEXT
from euclidwf.framework.drm_access import JOB_EXECUTING, JOB_COMPLETED, JOB_ERROR
from euclidwf.framework.runner import PipelineExecution, RUNID, REPORT, PIPELINE,\
    SUBMITTED, STATUS, CONFIG, INPUTS, OUTPUTS
from euclidwf.utilities.cmd_executor import AbstractCmdExecutor
from euclidwf.server.server_model import RunConfiguration, LOGDIR, WORKDIR,\
    DRM_CONFIGURE_CMD, DRM_SUBMIT_CMD, DRM_CHECKSTATUS_CMD, DRM_CLEANUP_CMD,\
    DRM_DELETE_CMD, DRM_STATUSCHECK_POLLTIME, DRM_STATUSCHECK_TIMEOUT, DRM_HOST,\
    DRM_PORT, CONFIG_DRM, WS_HOST, WS_PORT, CONFIG_WS, CONFIG_LOCALCACHE,\
    CONFIG_PROXYFCTS_DIR, PKG_REPOSITORY, PIPELINE_DIR, RunServerConfiguration,\
    WS_USERNAME, WS_PASSWORD, DRM_USERNAME, DRM_PASSWORD, INPUTDATA_PATHS, \
    PIPELINE_SCRIPT, CREDENTIALS, WS_ROOT, DRM_PROTOCOL, WS_PROTOCOL
from euclidwf.framework import node_callbacks, drm_access, drm_access2
import json


class TestRunner(unittest.TestCase):

    def setUp(self):
        # config
        self.config = _testconfig()
        # input data dict
        inputDataPaths={'a' : 'a.xml', 'b' : 'b.xml'}
        data={RUNID:"1", WORKDIR:'testdir', LOGDIR:'logs', PIPELINE_SCRIPT:'testpipeline.py',
              PIPELINE_DIR:self.config.pipelineDir, PKG_REPOSITORY: self.config.pkgRepository, 
              INPUTDATA_PATHS:inputDataPaths, CREDENTIALS:self.config.credentials}
        self.inputs = RunConfiguration(data)

        workdir=os.path.join(self.config.wsConfig.workspaceRoot, self.inputs.workdir)
        os.makedirs(workdir)
        with open(os.path.join(workdir,self.inputs.inputDataPaths['a']),'w') as filea:
            filea.write("Dummy file for input a.")
        with open(os.path.join(workdir,self.inputs.inputDataPaths['b']),'w') as fileb:
            fileb.write("Dummy file for input b.")
        # script
        self.script = 'testpipeline.py'
        with open(os.path.join(self.config.pipelineDir,self.script),'w') as pipelinefile:
            pipelinefile.write(TESTSCRIPT)
        
        # create the packagefile
        with open(os.path.join(self.config.pkgRepository,'testpkg.py'),'w') as pkgfile:
            pkgfile.write(TESTPKG)
            


    def test_initialize(self):
        execution = PipelineExecution(self.inputs, self.config)
        execution.initialize()
        self.assertTrue(execution.pipelineDir in sys.path)
        self.assertTrue(execution.pipeline and isfunction(execution.pipeline))
        self.assertIsInstance(execution.dataflow, Graph)
        self.assertEquals(1,len(execution.dataflow.get_all_ticks()))
        self.assertTrue(execution.data[CONTEXT])
        self.assertTrue(execution.traverser)
        
        
    def test_start_success(self):
        execution = PipelineExecution(self.inputs, self.config)
        execution.initialize()
        test_executor=TestCmdExecutor(self.config, "1", False, 0, False, JOB_COMPLETED)
        execution.callbacks._cmd_executor=test_executor
        execution.start()
        self.assertTrue(execution.outputs)
        self.assertEquals('test_exec/c.xml', execution.outputs['c'])
        self.assertEquals(1, len(execution.report))
        self.assertEquals(JOB_COMPLETED, execution.report[0]['status'])
        self.assertEquals('1', execution.report[0]['tick'])
        self.assertEquals('1', execution.report[0]['pid'])
        self.assertEquals('test_exec', execution.report[0]['path'])
        
        result_dict=PipelineExecution.todict(execution)
        self.assertEquals("1",result_dict[RUNID]) 
        self.assertTrue(result_dict[CONFIG])
        self.assertEquals("COMPLETED",result_dict[STATUS])
        self.assertTrue(result_dict[SUBMITTED])
        self.assertEquals("testpipeline",result_dict[PIPELINE]['name'])
        self.assertTrue(result_dict[REPORT])
        self.assertTrue(result_dict[INPUTS])
        self.assertTrue(result_dict[OUTPUTS])
        self.assertTrue(result_dict[OUTPUTS][WORKDIR])
            

    def test_start_error(self):
        node_callbacks.DRM=drm_access
        execution = PipelineExecution(self.inputs, self.config)
        execution.initialize()
        test_executor=TestCmdExecutor(self.config, "1", False, 0, False, JOB_ERROR)
        execution.callbacks._cmd_executor=test_executor
        execution.start()
        self.assertFalse(execution.outputs)
        self.assertEquals(1, len(execution.report))
        self.assertEquals(JOB_ERROR, execution.report[0]['status'])
        self.assertEquals('1', execution.report[0]['tick'])
        self.assertEquals('1', execution.report[0]['pid'])
        self.assertEquals('test_exec', execution.report[0]['path'])
    
    def test_start_error_drm2(self):
        node_callbacks.DRM=drm_access2
        execution = PipelineExecution(self.inputs, self.config)
        execution.initialize()
        test_executor=TestCmdExecutor2(self.config, "1", False, 0, False, JOB_ERROR)
        execution.callbacks._cmd_executor=test_executor
        execution.start()
        self.assertFalse(execution.outputs)
        self.assertEquals(1, len(execution.report))
        self.assertEquals(JOB_ERROR, execution.report[0]['status'])
        self.assertEquals('1', execution.report[0]['tick'])
        self.assertEquals('1', execution.report[0]['pid'])
        self.assertEquals('test_exec', execution.report[0]['path'])
    


    def test_start_failed_submit(self):
        node_callbacks.DRM=drm_access
        execution = PipelineExecution(self.inputs, self.config)
        execution.initialize()
        test_executor=TestCmdExecutor(self.config, "1", True, 0, False, JOB_ERROR)
        execution.callbacks._cmd_executor=test_executor
        execution.start()
        self.assertFalse(execution.outputs)
        self.assertEquals(1, len(execution.report))
        self.assertEquals('n/a', execution.report[0]['status'])
        self.assertEquals('1', execution.report[0]['tick'])
        self.assertEquals('n/a', execution.report[0]['pid'])
        self.assertEquals('test_exec', execution.report[0]['path'])
        self.assertTrue(execution.stacktrace)

def _testconfig():
    config={}
    tmpdir=tempfile.mkdtemp()
    drm={}
    drm[DRM_CONFIGURE_CMD]="configure"
    drm[DRM_SUBMIT_CMD]="submit"
    drm[DRM_CHECKSTATUS_CMD]="checkstatus"
    drm[DRM_CLEANUP_CMD]="cleanup"
    drm[DRM_DELETE_CMD]="delete"
    drm[DRM_STATUSCHECK_POLLTIME]=2
    drm[DRM_STATUSCHECK_TIMEOUT]=100000000
    drm[DRM_PROTOCOL]="local"
    drm[DRM_HOST]="localhost"
    drm[DRM_PORT]=""
    config[CONFIG_DRM]=drm

    ws={}
    ws[WS_PROTOCOL]="file"
    ws[WS_ROOT]=os.path.join(tmpdir,"workspace")
    ws[WS_HOST]=""
    ws[WS_PORT]=""
    config[CONFIG_WS]=ws
    config[CONFIG_LOCALCACHE]=os.path.join(tmpdir,"localcache")

    config[CONFIG_PROXYFCTS_DIR]=os.path.join(tmpdir,"code","proxyfcts")
    config[PKG_REPOSITORY]=os.path.join(tmpdir,"code","pkgdefs","testpkg")
    config[PIPELINE_DIR]=os.path.join(tmpdir,"code","scripts")
    config[CREDENTIALS]={WS_USERNAME:"euclid", WS_PASSWORD:"euclid",DRM_USERNAME:"euclid",DRM_PASSWORD:"euclid"}
    os.makedirs(config[CONFIG_WS][WS_ROOT])    
    os.makedirs(config[CONFIG_LOCALCACHE])
    os.makedirs(config[CONFIG_PROXYFCTS_DIR])
    os.makedirs(config[PKG_REPOSITORY])
    os.makedirs(config[PIPELINE_DIR])

    configuration=RunServerConfiguration(config)
    configuration.pkgRepository=config[PKG_REPOSITORY]
    configuration.pipelineDir=config[PIPELINE_DIR]
    return configuration



TESTPKG='''
from euclidwf.framework.taskdefs import Executable, Input, Output, ComputingResources

test_exec=Executable("test_exec",
       inputs=[Input("a"), Input("b")], 
       outputs=[Output("c")],
       resources=ComputingResources(cores=1, ram=1.0, walltime=0.0001)
    )

'''

TESTSCRIPT='''
from euclidwf.framework.workflow_dsl import pipeline
from testpkg import test_exec

@pipeline(outputs=('c'))        
def testpipeline(a,b):
    return test_exec(a=a,b=b)
'''

class TestCmdExecutor(AbstractCmdExecutor):

    def __init__(self, config, pid, fail_submit, numofchecks, fail_check, final_status):
        self.config=config
        self.cachedir=config.localcache
        self.pid=pid
        self.submit_process=self._get_submit_process(fail_submit)
        self.checkstatus_process_pending=self._get_checkstatus(JOB_EXECUTING, fail_check)
        self.checkstatus_process_final=self._get_checkstatus(final_status, fail_check)
        self.checked=0
        self.numofchecks=numofchecks
        
            
    def _get_submit_process(self, fail):
        if not fail:
            return MockProcess("job_id=%s"%self.pid, "", None)
        else:
            return MockProcess("job_id=%s"%self.pid, "Exception occurred.", Failure(ValueError("Exception occurred")))


    def _get_checkstatus(self, status, fail):
        if not fail:
            return MockProcess("status=%s"%status, "", None)
        else:
            return MockProcess("status=%s"%status, "Exception occurred.", Failure(ValueError("Exception occurred")))


    def execute(self, command):
        cmd0=command[0]
        if cmd0.startswith('submit'):
            return self._handle_submit(command)
        elif cmd0.startswith('checkstatus'):
            return self._handle_checkstatus(command)

    def _handle_checkstatus(self, cmdArray):
        self.checked+=1
        d=defer.Deferred()
        if self.checked<=self.numofchecks:
            d.callback(self.checkstatus_process_pending)
        else:
            d.callback(self.checkstatus_process_final)
        return d            


    def _handle_submit(self, cmdArray):
        taskname=cmdArray[1][len("--task="):]
        workdir=cmdArray[2][len("--workdir="):]
        inputs=cmdArray[3][len("--inputs="):]
        outputs=eval(cmdArray[4][len("--outputs="):])
        logdir=cmdArray[5][len("--logdir="):]
        outputfilepath=os.path.join(self.config.wsConfig.workspaceRoot,workdir, outputs['c'])
        os.makedirs(os.path.dirname(outputfilepath))
        with open(outputfilepath, 'w') as outputfile:
            outputfile.write("%s\n"%taskname)
            outputfile.write("%s\n"%workdir)
            outputfile.write("%s\n"%inputs)
            outputfile.write("%s\n"%logdir)
        d = defer.Deferred()
        d.callback(self.submit_process)
        return d
        

class TestCmdExecutor2(AbstractCmdExecutor):

    def __init__(self, config, pid, fail_submit, numofchecks, fail_check, final_status):
        self.config=config
        self.cachedir=config.localcache
        self.pid=pid
        self.submit_process=self._get_submit_process(fail_submit)
        self.checkstatus_process_pending=self._get_checkstatus(JOB_EXECUTING, fail_check)
        self.checkstatus_process_final=self._get_checkstatus(final_status, fail_check)
        self.checked=0
        self.numofchecks=numofchecks
        
            
    def _get_submit_process(self, fail):
        response_dict={"job_id":self.pid}
        if not fail:
            return MockProcess(json.dumps(response_dict), "", None)
        else:
            return MockProcess(json.dumps(response_dict), "Exception occurred.", Failure(ValueError("Exception occurred")))


    def _get_checkstatus(self, status, fail):
        response_dict=[{"job_id":self.pid,"status":status}]
        if not fail:
            return MockProcess(json.dumps(response_dict), "", None)
        else:
            return MockProcess(json.dumps(response_dict), "Exception occurred.", Failure(ValueError("Exception occurred")))


    def execute(self, command):
        cmd0=command[0]
        if cmd0.startswith('submit'):
            return self._handle_submit(command)
        elif cmd0.startswith('checkstatus'):
            return self._handle_checkstatus(command)

    def _handle_checkstatus(self, cmdArray):
        self.checked+=1
        d=defer.Deferred()
        if self.checked<=self.numofchecks:
            d.callback(self.checkstatus_process_pending)
        else:
            d.callback(self.checkstatus_process_final)
        return d            


    def _handle_submit(self, cmdArray):
        dictasstring=cmdArray[2].replace("'","\"")
        dictionary=json.loads(dictasstring)
        taskname=dictionary["task"]
        workdir=dictionary["workdir"]
        logdir=dictionary["logdir"]
        inputs=dictionary["inputs"]
        outputs=dictionary["outputs"]
        outputfilepath=os.path.join(self.config.wsConfig.workspaceRoot, workdir, outputs['c'])
        os.makedirs(os.path.dirname(outputfilepath))
        with open(outputfilepath, 'w') as outputfile:
            outputfile.write("%s\n"%taskname)
            outputfile.write("%s\n"%workdir)
            outputfile.write("%s\n"%inputs)
            outputfile.write("%s\n"%logdir)
        d = defer.Deferred()
        d.callback(self.submit_process)
        return d


class MockProcess():
    
    def __init__(self, stdoutmsg, stderrmsg, reason):
        self.stdout=MockResponse(stdoutmsg, None)
        self.stderr=MockResponse(stderrmsg, None)
        self.exited=MockResponse(None, reason)
        
        
class MockResponse():
    
    def __init__(self, response, reason):
        self.response=response
        self.reason=reason
        
    def add_callback(self, method):
        method(self.response)

    def next_event(self):
        return self.reason
