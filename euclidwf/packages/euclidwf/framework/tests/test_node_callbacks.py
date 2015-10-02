'''
Created on Jun 25, 2015

@author: martin.melchior
'''
from euclidwf.framework.node_callbacks import NodeCallbacks
import tempfile
import os
from euclidwf.utilities.cmd_executor import AbstractCmdExecutor
from euclidwf.framework.taskprops import PackageSource
from euclidwf.framework.workflow_dsl import invoke_task, pipeline
from euclidwf.framework.graph_builder import build_graph
from euclidwf.framework.graph_tasks import ExecTask
from euclidwf.framework.context import CONTEXT, CHECKSTATUS_TIMEOUT,\
    CHECKSTATUS_TIME, WORKDIR, LOGDIR
import unittest
from twisted.internet import defer
from twisted.python.failure import Failure
from euclidwf.framework.drm_access import JOB_EXECUTING, JOB_COMPLETED, JOB_ERROR
from pydron.dataflow.graph import Tick
from utwist._utwist import with_reactor
import datetime
from euclidwf.server.server_model import RunServerConfiguration, PKG_REPOSITORY,\
    WS_ROOT, DRM_CONFIGURE_CMD, DRM_SUBMIT_CMD, DRM_CHECKSTATUS_CMD,\
    DRM_CLEANUP_CMD, DRM_DELETE_CMD, DRM_STATUSCHECK_POLLTIME,\
    DRM_STATUSCHECK_TIMEOUT, DRM_PROTOCOL, WS_PROTOCOL, CONFIG_LOCALCACHE,\
    CONFIG_PROXYFCTS_DIR, PIPELINE_DIR, WS_USERNAME, WS_PASSWORD, DRM_USERNAME,\
    DRM_PASSWORD, CONFIG_DRM, CONFIG_WS, DRM_HOST, DRM_PORT, WS_HOST, WS_PORT,\
    HpcAccessCredentials
from euclidwf.framework.taskdefs import TaskProperties
import sys


class TestNodeCallbacks(unittest.TestCase):

    def setUp(self):
        self.config = _testconfig()
        _prepare_testpkgrepos(self.config.pkgRepository)
        self.node_callbacks=NodeCallbacks(self.config, self.config.credentials, self.config.pkgRepository)
        self.g = _create_graph(self.config.pkgRepository)
        execname='test_exec'
        self.task = ExecTask(execname, PackageSource("testpkg", "pkgdefs"), ("a","b"), ("c",))
        context={CHECKSTATUS_TIMEOUT : self.config.drmConfig.statusCheckTimeout, CHECKSTATUS_TIME : self.config.drmConfig.statusCheckPollTime}
        context[WORKDIR]=os.path.join(self.config.wsConfig.workspaceRoot,"testrun")
        context[LOGDIR]=os.path.join(self.config.wsConfig.workspaceRoot,"testrun","logs")        
        self.inputs={ "a":"dummypath_a", "b":"dummypath_b", CONTEXT:context}    
        
        
    @with_reactor
    def test_submit_completed_after_some_checks(self):
        num_checks=2
        test_executor=TestCmdExecutor(self.config, "1", False, 2, False, JOB_COMPLETED)
        self.node_callbacks._cmd_executor=test_executor
        tick=Tick.parse_tick(1)
        t1=datetime.datetime.now()
        d=self.node_callbacks.submit_task(self.g, tick, self.task, self.inputs)
        expected={'c':'test_exec/c.xml'}
        
        def check_success(_):
            t2=datetime.datetime.now()
            self.assertTrue((t2-t1).total_seconds()>num_checks*2.0)
            self.assertEqual(expected, d.result.result)        
            tick=Tick.parse_tick(1)
            taskprops=self.g.get_task_properties(tick)
            summary=taskprops['summary']
            self.assertEqual("test_exec",summary.dfpath)
            self.assertEqual("test_exec",summary.outdir)
            self.assertEqual("1",summary.pid)
            self.assertEqual(JOB_COMPLETED,summary.status)      
        
        d.addCallback(check_success)
        return d
     

    def test_submit_completed(self):
        test_executor=TestCmdExecutor(self.config, "1", False, 0, False, JOB_COMPLETED)
        self.node_callbacks._cmd_executor=test_executor
        tick=Tick.parse_tick(1)
        d=self.node_callbacks.submit_task(self.g, tick, self.task, self.inputs)
        expected={'c':'test_exec/c.xml'}
        self.assertEqual(expected, d.result.result)
        
        tick=Tick.parse_tick(1)
        taskprops=self.g.get_task_properties(tick)
        summary=taskprops['summary']
        self.assertEqual("test_exec",summary.dfpath)
        self.assertEqual("test_exec",summary.outdir)
        self.assertEqual("1",summary.pid)
        self.assertEqual(JOB_COMPLETED,summary.status)
        

    def test_submit_error(self):
        test_executor=TestCmdExecutor(self.config, "1", False, 0, False, JOB_ERROR)
        self.node_callbacks._cmd_executor=test_executor
        tick=Tick.parse_tick(1)
        d=self.node_callbacks.submit_task(self.g, tick, self.task, self.inputs)
        
        def expected_error(fail):
            if isinstance(fail, Failure):
                self.assertTrue(True)
            else:
                self.fail("Type of return value not as expected: %s"%type(fail))
        
        def unexpected_success(value):
            self.fail("Return value not as expected: %s of type %s"%(str(value),type(value)))
        
        d.addCallbacks(unexpected_success, expected_error)
        
        tick=Tick.parse_tick(1)
        taskprops=self.g.get_task_properties(tick)
        summary=taskprops['summary']
        self.assertEqual("test_exec",summary.dfpath)
        self.assertEqual("test_exec",summary.outdir)
        self.assertEqual("1",summary.pid)
        self.assertEqual(JOB_ERROR,summary.status)


    @with_reactor
    def test_submit_error_after_some_checks(self):
        num_checks=2
        test_executor=TestCmdExecutor(self.config, "1", False, 2, False, JOB_ERROR)
        self.node_callbacks._cmd_executor=test_executor
        tick=Tick.parse_tick(1)
        t1=datetime.datetime.now()
        d=self.node_callbacks.submit_task(self.g, tick, self.task, self.inputs)
        
        def expected_error(fail):
            if isinstance(fail, Failure):
                self.assertTrue(True)
                t2=datetime.datetime.now()
                self.assertTrue((t2-t1).total_seconds()>num_checks*2.0)
                tick=Tick.parse_tick(1)
                taskprops=self.g.get_task_properties(tick)
                summary=taskprops['summary']
                self.assertEqual("test_exec",summary.dfpath)
                self.assertEqual("test_exec",summary.outdir)
                self.assertEqual("1",summary.pid)
                self.assertEqual(JOB_ERROR,summary.status)
            else:
                self.fail("Type of return value not as expected: %s"%type(fail))
        
        def unexpected_success(value):
            self.fail("Return value not as expected: %s of type %s"%(str(value),type(value)))
        
        d.addCallbacks(unexpected_success, expected_error)
        return d
        

    def test_submit_failed_submit(self):
        test_executor=TestCmdExecutor(self.config, "1", True, 0, False, JOB_COMPLETED)
        self.node_callbacks._cmd_executor=test_executor
        tick=Tick.parse_tick(1)
        d=self.node_callbacks.submit_task(self.g, tick, self.task, self.inputs)
        
        def expected_error(failure):
            self.assertIsInstance(failure, Failure)
            self.assertTrue(failure.value.message.startswith("Exception at submit with reason"))
            return "Exception has occurred as expected."
        
        def unexpected_success(value):
            return Failure(ValueError("Exception did not occur as expected. Value %s found"%str(value)))

        d.addCallback(unexpected_success)
        d.addErrback(expected_error)

        tick=Tick.parse_tick(1)
        taskprops=self.g.get_task_properties(tick)
        self.assertFalse("summary" in taskprops.keys())
        self.assertEqual("test_exec",taskprops['path'])
        self.assertEqual("test_exec",taskprops['name'])


    def test_submit_failed_check(self):
        test_executor=TestCmdExecutor(self.config, "1", False, 0, True, JOB_COMPLETED)
        self.node_callbacks._cmd_executor=test_executor
        tick=Tick.parse_tick(1)
        d=self.node_callbacks.submit_task(self.g, tick, self.task, self.inputs)
        
        def expected_error(fail):
            self.assertIsInstance(fail, Failure)
            self.assertTrue(fail.value.message.startswith("Exception at check_status with reason"))
        
        def unexpected_success(value):
            self.fail("Unexpected success - failure expected. Return value: %s of type %s"%(str(value),type(value)))

        d.addCallbacks(unexpected_success, expected_error)
        tick=Tick.parse_tick(1)
        taskprops=self.g.get_task_properties(tick)
        self.assertFalse("summary" in taskprops.keys())
        self.assertEqual("test_exec",taskprops['path'])
        self.assertEqual("test_exec",taskprops['name'])



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
    config[PKG_REPOSITORY]=os.path.join(tmpdir,"code","pkgdefs")
    config[PIPELINE_DIR]=os.path.join(tmpdir,"code","scripts")
    
    os.makedirs(config[CONFIG_WS][WS_ROOT])    
    os.makedirs(config[CONFIG_LOCALCACHE])
    os.makedirs(config[CONFIG_PROXYFCTS_DIR])
    os.makedirs(config[PKG_REPOSITORY])
    os.makedirs(config[PIPELINE_DIR])

    configuration=RunServerConfiguration(config)
    configuration.credentials=HpcAccessCredentials({WS_USERNAME:"euclid", WS_PASSWORD:"euclid",DRM_USERNAME:"euclid",DRM_PASSWORD:"euclid"})
    configuration.pkgRepository=config[PKG_REPOSITORY]
    return configuration


TESTPKG='''
from euclidwf.framework.taskdefs import Executable, Input, Output, ComputingResources

test_exec=Executable("test_exec",
       inputs=[Input("a"), Input("b")], 
       outputs=[Output("c")],
       resources=ComputingResources(cores=1, ram=1.0, walltime=0.0001)
    )

'''

def _prepare_testpkgrepos(repospath):
    pkgfilepath=os.path.join(repospath,'testpkg.py')
    with open(pkgfilepath,'w') as pkgfile:
        pkgfile.write(TESTPKG)
    if not '__init__.py' in os.listdir(repospath):
        initpath = os.path.join(repospath,'__init__.py')
        with open(initpath, 'w') as initfile:
            initfile.write("# generated by pipeline framework")
    sys.path.append(repospath)
    



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
        outputfilepath=os.path.join(workdir, outputs['c'])
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

def _create_graph(pkgrepos):

    @pipeline(outputs=('c'))        
    def testpipeline(a,b):
        return test_exec(a=a,b=b)
    
    def test_exec(**kwargs):
        inputnames=("a","b")
        outputnames=("c")
        pkgsource=PackageSource("testpkg", pkgrepos)
        taskprops=TaskProperties("test_exec", pkgsource)
        return invoke_task(taskprops, inputnames, outputnames, **kwargs)
    
    return build_graph(testpipeline)