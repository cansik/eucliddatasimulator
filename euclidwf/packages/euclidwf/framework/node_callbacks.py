'''
Created on Apr 15, 2015

@author: martin.melchior
'''

import datetime
import logging
import os
from twisted.internet import defer, task
from twisted.python.failure import Failure
from pydron.interpreter.traverser import EvalResult

from euclidwf.framework.context import CONTEXT, WORKDIR, LOGDIR, CHECKSTATUS_TIME, CHECKSTATUS_TIMEOUT
from euclidwf.framework.graph_tasks import HelperTask
from euclidwf.utilities import cmd_executor
from euclidwf.utilities.error_handling import ProcessingError
from euclidwf.framework.drm_access import JOB_PENDING, JOB_ABORTED
from euclidwf.utilities.exec_loader import load_executable

from euclidwf.framework import drm_access, drm_access2
DRM=drm_access

logger = logging.getLogger(__name__)  

class NodeCallbacks(object):
    """
    This component provides callback functions for processing. The tasks are identified by :class:`Traverser` either 
    * as ready for execution or 
    * as ready for refinement. 
    Ready for execution means that jobs are configured and submitted to the DRM; ready for refinement means that 
    the graph is being modified for data that has become available at runtime.
    """    
    def __init__(self, configuration, credentials, pkgdefs):
        """
        :param configuration: Configuration which contains the the details of
           * the local cache to be used to store fetched data
           * the workspace on the HPC submission host  
        """
        self._configuration = configuration
        self._drm_methods = _DRMMethods(configuration.drmConfig)
        self._cmd_executor = cmd_executor.create(configuration.drmConfig, configuration.localcache, credentials.drmUsername, credentials.drmPassword)
        self._job_queue = set()
        self._currently_running = {}
    
    
    def _check_configuration(self):
        pass
    
    
    def schedule_refinement(self, g, tick, task, inputs):
        """
        Refine the graph.
        :param g: Graph to be refined.
        :param tick: Tick of the task being refined.
        :param task: Task being refined.
        :param inputs: portname, value for the refiner ports.
        """
        logger.debug("Refining task with tick %s." % tick)
        return task.refine(g, tick, inputs)
    
    
    def submit_task(self, g, tick, task, inputs):
        """
        Submit a task to the DRM.        
        :param g: Graph containing the task
        :param tick: Tick of the task being evaluated.
        :param task: Task being evaluated.
        :param inputs: port -> valueref for the input ports.
        """
        if isinstance(task,HelperTask):
            dfpath=g.get_task_properties(tick)['path']
            g.set_task_property(tick, 'summary', _InPlaceExecSummary(tick, dfpath))
            return task.evaluate(g, tick, task, inputs)
        else:
            executable = load_executable(task.command, task.package.pkgname)
            job = _Job(self, g, tick, task, executable, inputs)
            logger.debug("Job added to queue: %r" % job)
            self._job_queue.add(job)
            self._submit_jobs()
            return job.result
    
    
    def _submit_jobs(self):
        """
        Call this whenever a new job is added to the queue.
        """
        to_remove=[]
        for job in self._job_queue:
            logger.debug("Job %r submitted." %job)
            self._submit(job)
            to_remove.append(job)
        for job in to_remove:
            self._job_queue.remove(job)


    def _check_status(self, job):
        checkstatus_method=self._drm_methods.check_status
        check_cmd=DRM.create_checkstatus_command(checkstatus_method, [job.pid])
        d = self._cmd_executor.execute(check_cmd)
        data=[]
        
        def on_executed(process):
            process.stdout.add_callback(data.append)
            process.stderr.add_callback(data.append)
            return process.exited.next_event()

        def on_finished(reason):
            if reason:
                failure = Failure(ValueError("Exception occurred while checking job status. Reason: %s \n Stdout: %s"%(reason.getTraceback(), ''.join(data))))
                job.result.errback(failure)
                return 

            now = datetime.datetime.now()
            istimedout = job.is_timed_out(now)
            response=DRM.read_checkstatus_response(data)[0]
            if not response.status:
                failure = Failure(ValueError("Exception occurred while checking job status. No status information provided."))
                job.result.errback(failure)
                return
            status= response.status if not istimedout else JOB_ABORTED
            if DRM.wait_for_job(status):
                job.status=status
                return 

            job.check_status_loop.stop()
            job.end_time=now
            job.status=status
            job.g.set_task_property(job.tick, 'summary', _JobExecSummary(job))
            if istimedout:
                logger.info("Job %s aborted by pipeline run service due to a timeout."%str(job.tick))
                failure=Failure(ProcessingError("Job %s aborted by pipeline run service due to a timeout."%(str(job.tick))))
                job.result.callback(failure)
                #return failure
            elif DRM.job_failed(status):
                logger.info("Job %s failed."%(str(job.tick)))
                logger.info("STDOUT: %s \nSTDERR: %s."%(response.stdout,response.stderr))
                failure=Failure(ProcessingError("Processing of the job %s failed with status %s. \nSTDOUT: %s \nSTDERR: %s"%(str(job.tick),status, response.stdout, response.stderr)))
                job.result.callback(failure)
                #return failure
            elif DRM.job_completed(status):
                logger.info("Job %s succeeded."%str(job.tick))
                result=self._collect_results(job)
                job.result.callback(result)
                #return result
                
        def on_failure(reason):
            check_status_loop=job.check_status_loop
            if check_status_loop.running:
                check_status_loop.stop()
            failure=Failure(ValueError("Exception at check_status with reason: %s \n stdout: %s"%(reason.getTraceback(), ''.join(data))))
            job.result.errback(failure)
        
        d.addCallback(on_executed)
        d.addCallback(on_finished)
        d.addErrback(on_failure)
        return d


    def _collect_results(self, job):
        return EvalResult(job.outputs)
        

    def _create_input_dict(self, job):        
        input_dict={}
        for inputname in job.task.inputnames:
            input_dict[inputname]=job.inputs[inputname]
        return input_dict


    def _create_output_dict(self, job): 
        outdir=job.outdir
        output_dict={}
        from_pkgdef={ o.name : o for o in job.executable.outputs }
        for outputname in job.task.outputnames:
            output=from_pkgdef[outputname]
            output_dict[outputname]="%s.%s"%(os.path.join(outdir, outputname), output.mime_type)
        return output_dict


    def _submit(self, job):
        submit_method=self._drm_methods.submit
        context=job.inputs[CONTEXT]
        inputs=self._create_input_dict(job)
        outputs=self._create_output_dict(job)
        workdir=context[WORKDIR]
        logdir=os.path.join(context[LOGDIR],job.outdir)
        job.outputs=outputs
        command=job.task.command
        submit_cmd=DRM.create_submit_command(submit_method, 
                                             command, 
                                             inputs, 
                                             outputs, 
                                             workdir, 
                                             logdir, 
                                             job.executable.resources)
        job.cmd=submit_cmd
        job.submit_time=datetime.datetime.now()
        logger.info("Submitting job to DRM with command:\n %s"%' '.join(job.cmd))
        logger.info("Resources requested:\n %s"%str(job.executable.resources))
        d = self._cmd_executor.execute(submit_cmd)
        data=[]
        
        def on_executed(process):
            process.stdout.add_callback(data.append)
            process.stderr.add_callback(data.append)
            return process.exited.next_event()

        def on_finished(reason):
            if reason:
                return Failure(ValueError("Exception occurred while submitting job. Reason: %s \ Stdout: %s"%(reason.getTraceback(), ''.join(data))))
            else:
                response = DRM.read_submit_response(data)
                if not response.jobid:
                    return Failure(ValueError("pid of the submitted task could not be resolved from the stdout.\nStdout: %s"%''.join(data)))
                job.pid=response.jobid
                check_status_loop=task.LoopingCall(self._check_status, job)
                job.check_status_loop=check_status_loop
                return check_status_loop.start(context[CHECKSTATUS_TIME], True)

        def on_failure(reason):
            failure = Failure(ValueError("Exception at submit with reason: %s \n stdout: %s"%(reason.getTraceback(), ''.join(data))))
            job.result.errback(failure)
            return failure
                    
        d.addCallback(on_executed)
        d.addCallback(on_finished)
        d.addErrback(on_failure)
        

    def _cancel_job(self, job):
        if job in self._job_queue:
            self._job_queue.remove(job)
        elif job in self._currently_running:
            self._currently_running[job].cancel()
            del self._currently_running[job]
       
       
class _Job(object):
    def __init__(self, callbacks, g, tick, task, executable, inputs):
        self.callbacks = callbacks
        self.g = g
        self.tick = tick
        self.task = task
        self.executable=executable
        self.outdir=self._outdir_path()
        self.inputs = inputs
        self.result = defer.Deferred(self._cancel)
        self.cmd = None
        self.submit_time = None
        self.end_time = None
        self.timeout = inputs[CONTEXT][CHECKSTATUS_TIMEOUT]*executable.resources.walltime*3600
        self.status = JOB_PENDING
        self.pid = None
        
    def _cancel(self, d):
        self.callbacks._cancel_job(self)
        
    def __repr__(self):
        return "Job(%r)" % self.tick
    
    def _outdir_path(self):
        taskname=self.g.get_task_properties(self.tick)['path']
        return taskname
    
    def is_timed_out(self, now):
        return get_timediff(self.submit_time,now) > self.timeout
        


class _JobExecSummary(object):
    def __init__(self, job):
        self.tick = job.tick
        self.dfpath = job.g.get_task_properties(job.tick)['path']
        self.workdir=job.inputs[CONTEXT][WORKDIR]
        self.logdir=job.inputs[CONTEXT][LOGDIR]
        self.outdir=job.outdir
        self.cmd = job.cmd
        self.submit_time = job.submit_time
        self.end_time = job.end_time
        self.status = job.status
        self.pid = job.pid
        self.lapse_time = get_timediff(job.submit_time,job.end_time)


class _InPlaceExecSummary(object):
    def __init__(self, tick, path):
        self.tick = tick
        self.dfpath = path
        self.workdir=None
        self.submit_time = datetime.datetime.now()
        self.status = "n/a"
        self.pid = None
        self.lapse_time = None
        

def get_timediff(dt1, dt2):
    return (dt2-dt1).total_seconds()


class _DRMMethods(object):
    def __init__(self, config):
        self.submit=config.submitCmd
        self.check_status=config.checkStatusCmd
        


