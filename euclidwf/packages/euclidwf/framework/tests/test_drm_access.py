'''
Created on May 18, 2015

@author: martin.melchior
'''
import unittest
from euclidwf.framework import drm_access2
from euclidwf.framework.taskdefs import ComputingResources


class TestDrmAccess(unittest.TestCase):

    def test_create_submit_command(self):
        inputs={'input1':'inputs/inputpath1.xml', 'input2':'inputs/inputpath2.xml'}
        outputs={'output1':'task/out1.xml', 'output2':'task/out2.xml'}
        task='taskname'
        workdir='/workspace/runxxx'
        logdir='logs'
        resources=ComputingResources(2, 5.0, 1.0)        
        cmdname='SUBMITCMD'
        cmd=drm_access2.create_submit_command(cmdname, task, inputs, outputs, workdir, logdir, resources)
        self.assertEquals(3, len(cmd))
        print cmd
        

    def test_create_checkstatus_command(self):
        ids=['1','2','3','29839']
        cmdname='CHECKSTSTATUSCMD'
        cmd=drm_access2.create_checkstatus_command(cmdname, ids)
        self.assertEquals(3, len(cmd))
        print cmd
        
        