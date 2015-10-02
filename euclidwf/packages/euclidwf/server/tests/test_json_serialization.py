'''
Created on Sep 14, 2015

@author: martinm
'''
from euclidwf.server.server_model import RunDetailedStatus, STATUS_RESPONSE_OK,\
    JOB_COMPLETED, JobStatus, PRSJsonEncoder, RunOutput, PipelineTaskRun,\
    RunReport
import json
import unittest

class TestGraphBuilder(unittest.TestCase):

    def test_detailed_status(self):
        runid='1'
        responseStatus=STATUS_RESPONSE_OK
        executionStatus=JOB_COMPLETED
        message="Test"
        job1=JobStatus("tick1", JOB_COMPLETED)
        job2=JobStatus("tick2", JOB_COMPLETED)
        jobs=[job1, job2]
        detailedStatus=RunDetailedStatus(runid, responseStatus, executionStatus, jobs, message)
        try:
            json.dumps(detailedStatus.__dict__, cls=PRSJsonEncoder)
        except:
            self.fail("RunDetailedStatus cannot be serialized to JSON.")
            

    def test_run_report(self):
        runid='1'
        status=JOB_COMPLETED
        task1=PipelineTaskRun("exec1", "1", "task1", "1", JOB_COMPLETED, "task1", "all ok", "", "repos")
        task2=PipelineTaskRun("exec2", "2", "task2", "2", JOB_COMPLETED, "task2", "all ok", "", "repos")
        task3=PipelineTaskRun("exec3", "3", "task3", "3", JOB_COMPLETED, "task3", "all ok", "", "repos")
        tasks=[task1,task2,task3]
        taskrun1=RunOutput("port1", "type1", "/home/euclid/ws/task1/port1.xml")
        taskrun2=RunOutput("port2", "type2", "/home/euclid/ws/task2/port2.xml")
        outputs=[taskrun1,taskrun2]
        responseStatus=STATUS_RESPONSE_OK
        message="Test"
        report=RunReport(runid, responseStatus, message, status, tasks, outputs)
        try:
            json.dumps(report.__dict__, cls=PRSJsonEncoder)
        except:
            self.fail("RunDetailedStatus cannot be serialized to JSON.")
    