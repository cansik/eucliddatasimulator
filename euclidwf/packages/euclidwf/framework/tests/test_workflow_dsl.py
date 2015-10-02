'''
Created on May 18, 2015

@author: martin.melchior
'''
import inspect
import unittest
from euclidwf.framework.runner import load_pipeline_from_file
from euclidwf.framework.taskdefs import TaskProperties
from euclidwf.framework.tests import pipeline_for_test
from euclidwf.framework.workflow_dsl import invoke_task, invoke_pipeline, nested, parallel,\
        pipeline, get_portnames, MethodInvocation, TaskInvocation, ParallelSplit, Source
from euclidwf.utilities.error_handling import PipelineSpecificationError


class TestWorkflowDSL(unittest.TestCase):

    def test_load_pipeline_from_file(self):
        pipeline_file=inspect.getsourcefile(pipeline_for_test)
        pipeline_func=load_pipeline_from_file(pipeline_file)
        self.assertEqual("testpipe_parallel", pipeline_func.func_name)
        invocation=invoke_pipeline(pipeline_func)
        self.assertTrue(isinstance(invocation,MethodInvocation))
        self.assertTrue(invocation.ispipeline)
        
        
    def test_get_portnames(self):    
        pipeline_file=inspect.getsourcefile(pipeline_for_test)
        portnames=get_portnames(pipeline_file)
        self.assertEquals(tuple(['x','y']), portnames['inputs'])
        self.assertEquals(tuple(['u']), portnames['outputs'])
    
    

    def test_simple_task(self):
        """
        A simple one-step pipeline 
        """
        @pipeline(outputs=('u','v'))
        def testpipe(x,y):
            u,v=test_exec(a=x, b=y)
            return u,v
        
        invocation=invoke_pipeline(testpipe)
        self.assertTrue(isinstance(invocation,MethodInvocation))
        self.assertTrue(invocation.ispipeline)
        
        # there is just a single task
        self.assertEqual(1, len(invocation.tasks))
        task=invocation.tasks[0]
        self.assertTrue(isinstance(task,TaskInvocation))
        self.assertEqual('test_exec',task.name)
        
        # at task level:
        # inputs at task level are source objects without a parent
        self.assertEqual(2,len(task.inputs))
        s_a=task.inputs["a"]
        s_b=task.inputs["b"]
        self.assertEqual(Source(s_a.name,None,None),s_a)
        self.assertEqual(Source(s_b.name,None,None),s_b)
        # outputs are source objects with a parent - but at task level without a reference
        self.assertEqual(2,len(task.outputs))
        s_c=[s for s in task.outputs if s.name=="c"][0]
        s_d=[s for s in task.outputs if s.name=="d"][0]
        self.assertIsNone(s_c.ref)
        self.assertIsNone(s_d.ref)
        self.assertEqual(task, s_c.parent)
        self.assertEqual(task, s_d.parent)
        self.assertEqual(['c', 'test_exec'], s_c.path)
        self.assertEqual(['d', 'test_exec'], s_d.path)

        # at pipeline level ('invocation'):        
        # inputs
        self.assertEqual(2,len(invocation.inputs))
        s_x=invocation.inputs["x"]
        s_y=invocation.inputs["y"]
        self.assertEqual(Source(s_x.name,None,None),s_x)
        self.assertEqual(Source(s_y.name,None,None),s_y)
        # outputs are source that at this level have a ref - the source objects created at task level
        self.assertEqual(2,len(invocation.outputs))
        s_c_outer=[s for s in invocation.outputs if s.name=="c"][0]
        s_d_outer=[s for s in invocation.outputs if s.name=="d"][0]
        self.assertEqual(s_c, s_c_outer.ref)
        self.assertEqual(s_d, s_d_outer.ref)
        self.assertEqual(invocation, s_c_outer.parent)
        self.assertEqual(invocation, s_d_outer.parent)
        self.assertEqual(['c', 'test_exec','testpipe'], s_c_outer.path)
        self.assertEqual(['d', 'test_exec','testpipe'], s_d_outer.path)
        self.assertEqual('u', s_c_outer.alias)
        self.assertEqual('v', s_d_outer.alias)


    def test_wrong_outputs(self):
        '''
        Wrong number of output names specified in the pipeline decorator.
        '''
        @pipeline(outputs=('u'))
        def testpipe(x,y):
            u,v=test_exec(a=x, b=y)
            return u,v        
        try:
            invoke_pipeline(testpipe)
            self.fail()
        except PipelineSpecificationError:
            self.assertTrue(True)


    def test_two_steps(self):
        '''
        Two steps with the same 'executable' underlying: tests whether the different node names are properly handled. 
        Furthermore, outputs of one of the tasks are passed as inputs to the second.
        Finally, three output variables are declared.
        '''
        @pipeline(outputs=('u','w','z'))
        def testpipe(x,y):
            u,v=test_exec(a=x, b=y)
            w,z=test_exec(name="test_exec2", a=u, b=v)
            return u,w,z
        
        invocation=invoke_pipeline(testpipe)
        self.assertTrue(isinstance(invocation,MethodInvocation))
        self.assertEqual(2, len(invocation.tasks))

        # test task 1
        task1=invocation.tasks[0]
        self.assertTrue(isinstance(task1,TaskInvocation))
        self.assertEqual('test_exec',task1.name)
        self.assertEqual(2,len(task1.inputs))
        s_a_1=task1.inputs["a"]
        s_b_1=task1.inputs["b"]
        self.assertEqual(Source(s_a_1.name,None,None),s_a_1)
        self.assertEqual(Source(s_b_1.name,None,None),s_b_1)
        self.assertEqual(2,len(task1.outputs))
        s_c_1=[s for s in task1.outputs if s.name=="c"][0]
        s_d_1=[s for s in task1.outputs if s.name=="d"][0]
        self.assertIsNone(s_c_1.ref)
        self.assertIsNone(s_d_1.ref)
        self.assertEqual(task1, s_c_1.parent)
        self.assertEqual(task1, s_d_1.parent)

        # test task 2
        task2=invocation.tasks[1]
        self.assertTrue(isinstance(task2,TaskInvocation))
        self.assertEqual('test_exec2',task2.name)
        self.assertEqual(2,len(task2.inputs))
        s_a_2=task2.inputs["a"]
        s_b_2=task2.inputs["b"]
        self.assertEqual(Source(s_c_1.name,task1,None),s_a_2)
        self.assertEqual(Source(s_d_1.name,task1,None),s_b_2)
        self.assertEqual(2,len(task2.outputs))
        s_c_2=[s for s in task2.outputs if s.name=="c"][0]
        s_d_2=[s for s in task2.outputs if s.name=="d"][0]
        self.assertIsNone(s_c_2.ref)
        self.assertIsNone(s_d_2.ref)
        self.assertEqual(task2, s_c_2.parent)
        self.assertEqual(task2, s_d_2.parent)
        
        self.assertEqual(3,len(invocation.outputs))
        s_c_outer1=[s for s in invocation.outputs if s.name=="c" and s.ref.parent==task1][0]
        s_c_outer2=[s for s in invocation.outputs if s.name=="c" and s.ref.parent==task2][0]
        s_d_outer=[s for s in invocation.outputs if s.name=="d"][0]
        self.assertEqual(s_c_1, s_c_outer1.ref)
        self.assertEqual(s_c_2, s_c_outer2.ref)
        self.assertEqual(s_d_2, s_d_outer.ref)
        self.assertEqual(invocation, s_c_outer1.parent)
        self.assertEqual(invocation, s_c_outer2.parent)
        self.assertEqual(invocation, s_d_outer.parent)
        self.assertEqual('u', s_c_outer1.alias)
        self.assertEqual('w', s_c_outer2.alias)
        self.assertEqual('z', s_d_outer.alias)


    def test_two_steps_same_name(self):
        '''
        Same as above (test_two_steps) - except for that names are not set explicitly
        '''
        @pipeline(outputs=('u','w','z'))
        def testpipe(x,y):
            u,v=test_exec(a=x, b=y)
            w,z=test_exec(a=u, b=v)
            return u,w,z
        
        invocation=invoke_pipeline(testpipe)
        self.assertTrue(isinstance(invocation,MethodInvocation))
        self.assertEqual(2, len(invocation.tasks))

        # test task 1
        task1=invocation.tasks[0]
        self.assertTrue(isinstance(task1,TaskInvocation))
        self.assertEqual('test_exec_1',task1.name)
        self.assertEqual(2,len(task1.inputs))
        s_a_1=task1.inputs["a"]
        s_b_1=task1.inputs["b"]
        self.assertEqual(Source(s_a_1.name,None,None),s_a_1)
        self.assertEqual(Source(s_b_1.name,None,None),s_b_1)
        self.assertEqual(2,len(task1.outputs))
        s_c_1=[s for s in task1.outputs if s.name=="c"][0]
        s_d_1=[s for s in task1.outputs if s.name=="d"][0]
        self.assertIsNone(s_c_1.ref)
        self.assertIsNone(s_d_1.ref)
        self.assertEqual(task1, s_c_1.parent)
        self.assertEqual(task1, s_d_1.parent)
        self.assertEqual(['c','test_exec_1'], s_c_1.path)
        self.assertEqual(['d','test_exec_1'], s_d_1.path)

        # test task 2
        task2=invocation.tasks[1]
        self.assertTrue(isinstance(task2,TaskInvocation))
        self.assertEqual('test_exec_2',task2.name)
        self.assertEqual(2,len(task2.inputs))
        s_a_2=task2.inputs["a"]
        s_b_2=task2.inputs["b"]
        self.assertEqual(Source(s_c_1.name,task1,None),s_a_2)
        self.assertEqual(Source(s_d_1.name,task1,None),s_b_2)
        self.assertEqual(2,len(task2.outputs))
        s_c_2=[s for s in task2.outputs if s.name=="c"][0]
        s_d_2=[s for s in task2.outputs if s.name=="d"][0]
        self.assertIsNone(s_c_2.ref)
        self.assertIsNone(s_d_2.ref)
        self.assertEqual(task2, s_c_2.parent)
        self.assertEqual(task2, s_d_2.parent)
        self.assertEqual(['c','test_exec_2'], s_c_2.path)
        self.assertEqual(['d','test_exec_2'], s_d_2.path)
        
        self.assertEqual(3,len(invocation.outputs))
        s_c_outer1=[s for s in invocation.outputs if s.name=="c" and s.ref.parent==task1][0]
        s_c_outer2=[s for s in invocation.outputs if s.name=="c" and s.ref.parent==task2][0]
        s_d_outer=[s for s in invocation.outputs if s.name=="d"][0]
        self.assertEqual(s_c_1, s_c_outer1.ref)
        self.assertEqual(s_c_2, s_c_outer2.ref)
        self.assertEqual(s_d_2, s_d_outer.ref)
        self.assertEqual(invocation, s_c_outer1.parent)
        self.assertEqual(invocation, s_c_outer2.parent)
        self.assertEqual(invocation, s_d_outer.parent)
        self.assertEqual('u', s_c_outer1.alias)
        self.assertEqual('w', s_c_outer2.alias)
        self.assertEqual('z', s_d_outer.alias)


    def test_inline(self):
        '''
        Functions without decorator are just inlined.
        A first test with testpipe just tests whether the expression tree turns out to be flat.
        A second test with testpipe2 checks whether the "node" names are properly adjusted if reusing the same method twice. 
        '''
        @pipeline(outputs=('u','w','z'))
        def testpipe(x,y):
            return test_task(x=x,y=y)

        @pipeline(outputs=('u','v','w','z'))
        def testpipe2(x,y):
            a,b,c=test_task(x=x,y=y)
            _,e,f=test_task(x=x,y=a)
            return b,c,e,f        
        
        def test_task(x,y):
            u,v=test_exec(a=x, b=y)
            w,z=test_exec(name="test_exec2", a=u, b=v)
            return u,w,z

        #######################        
        # Test with pipeline 1
        #######################
        invocation=invoke_pipeline(testpipe)
        self.assertTrue(isinstance(invocation,MethodInvocation))
        self.assertEqual(2, len(invocation.tasks))

        # test task 1
        task1=invocation.tasks[0]
        self.assertTrue(isinstance(task1,TaskInvocation))
        self.assertEqual('test_exec',task1.name)
        self.assertEqual(2,len(task1.inputs))
        s_a_1=task1.inputs["a"]
        s_b_1=task1.inputs["b"]
        self.assertEqual(Source(s_a_1.name,None,None),s_a_1)
        self.assertEqual(Source(s_b_1.name,None,None),s_b_1)
        self.assertEqual(2,len(task1.outputs))
        s_c_1=[s for s in task1.outputs if s.name=="c"][0]
        s_d_1=[s for s in task1.outputs if s.name=="d"][0]
        self.assertIsNone(s_c_1.ref)
        self.assertIsNone(s_d_1.ref)
        self.assertEqual(task1, s_c_1.parent)
        self.assertEqual(task1, s_d_1.parent)
        
        # test task 2
        task2=invocation.tasks[1]
        self.assertTrue(isinstance(task2,TaskInvocation))
        self.assertEqual('test_exec2',task2.name)
        self.assertEqual(2,len(task2.inputs))
        s_a_2=task2.inputs["a"]
        s_b_2=task2.inputs["b"]
        self.assertEqual(Source(s_c_1.name,task1,None),s_a_2)
        self.assertEqual(Source(s_d_1.name,task1,None),s_b_2)
        self.assertEqual(2,len(task2.outputs))
        s_c_2=[s for s in task2.outputs if s.name=="c"][0]
        s_d_2=[s for s in task2.outputs if s.name=="d"][0]
        self.assertIsNone(s_c_2.ref)
        self.assertIsNone(s_d_2.ref)
        self.assertEqual(task2, s_c_2.parent)
        self.assertEqual(task2, s_d_2.parent)
        
        self.assertEqual(3,len(invocation.outputs))
        s_c_outer1=[s for s in invocation.outputs if s.name=="c" and s.ref.parent==task1][0]
        s_c_outer2=[s for s in invocation.outputs if s.name=="c" and s.ref.parent==task2][0]
        s_d_outer=[s for s in invocation.outputs if s.name=="d"][0]
        self.assertEqual(s_c_1, s_c_outer1.ref)
        self.assertEqual(s_c_2, s_c_outer2.ref)
        self.assertEqual(s_d_2, s_d_outer.ref)
        self.assertEqual(invocation, s_c_outer1.parent)
        self.assertEqual(invocation, s_c_outer2.parent)
        self.assertEqual(invocation, s_d_outer.parent)
        self.assertEqual('u', s_c_outer1.alias)
        self.assertEqual('w', s_c_outer2.alias)
        self.assertEqual('z', s_d_outer.alias)

        #######################
        # Test with pipeline 2
        #######################
        invocation=invoke_pipeline(testpipe2)
        self.assertTrue(isinstance(invocation,MethodInvocation))
        self.assertEqual(4, len(invocation.tasks))

        self.assertEqual(4,len(invocation.outputs))
        s_c_outer_u=[s for s in invocation.outputs if s.name=="c" and s.ref.parent.name=="test_exec2_1" ][0]
        s_d_outer_v=[s for s in invocation.outputs if s.name=="d" and s.ref.parent.name=="test_exec2_1" ][0]
        s_c_outer_w=[s for s in invocation.outputs if s.name=="c" and s.ref.parent.name=="test_exec2_2" ][0]
        s_d_outer_z=[s for s in invocation.outputs if s.name=="d" and s.ref.parent.name=="test_exec2_2" ][0]
        self.assertEqual(invocation, s_c_outer_u.parent)
        self.assertEqual(invocation, s_d_outer_v.parent)
        self.assertEqual(invocation, s_c_outer_w.parent)
        self.assertEqual(invocation, s_d_outer_z.parent)
        self.assertEqual('u', s_c_outer_u.alias)
        self.assertEqual('v', s_d_outer_v.alias)
        self.assertEqual('w', s_c_outer_w.alias)
        self.assertEqual('z', s_d_outer_z.alias)


    def test_nested(self):
        
        @nested()
        def test_task_nested(x,y):
            u,v=test_exec(a=x, b=y)
            w,z=test_exec(name="test_exec2", a=u, b=v)
            return u,w,z
        
        def testpipe_nested(x,y):
            return test_task_nested(x=x,y=y)

        invocation=invoke_pipeline(testpipe_nested)
        self.assertTrue(isinstance(invocation,MethodInvocation))
        self.assertEqual(1, len(invocation.tasks))

        nested_wf=invocation.tasks[0]
        self.assertTrue(isinstance(nested_wf,MethodInvocation))
        self.assertEqual('test_task_nested',nested_wf.name)
        self.assertEqual(2,len(nested_wf.inputs))
        s_a_n=nested_wf.inputs["x"]
        s_b_n=nested_wf.inputs["y"]
        self.assertEqual(Source(s_a_n.name,None,None),s_a_n)
        self.assertEqual(Source(s_b_n.name,None,None),s_b_n)
        self.assertEqual(3,len(nested_wf.outputs))
        s_c_n=[s for s in nested_wf.outputs if s.name=="c"]
        s_d_n=[s for s in nested_wf.outputs if s.name=="d"][0]
        self.assertEqual(2,len(s_c_n))
        self.assertIsNotNone(s_c_n[0].ref)
        self.assertEqual(nested_wf, s_c_n[0].parent)
        self.assertIsNotNone(s_c_n[1].ref)
        self.assertEqual(nested_wf, s_c_n[1].parent)
        self.assertIsNotNone(s_d_n.ref)
        self.assertEqual(nested_wf, s_d_n.parent)

        self.assertEqual(2,len(nested_wf.tasks))
        
        # test task 1
        task1=nested_wf.tasks[0]
        self.assertTrue(isinstance(task1,TaskInvocation))
        self.assertEqual('test_exec',task1.name)
        self.assertEqual(2,len(task1.inputs))
        s_a_1=task1.inputs["a"]
        s_b_1=task1.inputs["b"]
        self.assertEqual(Source(s_a_1.name,None,None),s_a_1)
        self.assertEqual(Source(s_b_1.name,None,None),s_b_1)
        self.assertEqual(2,len(task1.outputs))
        s_c_1=[s for s in task1.outputs if s.name=="c"][0]
        s_d_1=[s for s in task1.outputs if s.name=="d"][0]
        self.assertIsNone(s_c_1.ref)
        self.assertIsNone(s_d_1.ref)
        self.assertEqual(task1, s_c_1.parent)
        self.assertEqual(task1, s_d_1.parent)
        
        # test task 2
        task2=nested_wf.tasks[1]
        self.assertTrue(isinstance(task2,TaskInvocation))
        self.assertEqual('test_exec2',task2.name)
        self.assertEqual(2,len(task2.inputs))
        s_a_2=task2.inputs["a"]
        s_b_2=task2.inputs["b"]
        self.assertEqual(Source(s_c_1.name,task1,None),s_a_2)
        self.assertEqual(Source(s_d_1.name,task1,None),s_b_2)
        self.assertEqual(2,len(task2.outputs))
        s_c_2=[s for s in task2.outputs if s.name=="c"][0]
        s_d_2=[s for s in task2.outputs if s.name=="d"][0]
        self.assertIsNone(s_c_2.ref)
        self.assertIsNone(s_d_2.ref)
        self.assertEqual(task2, s_c_2.parent)
        self.assertEqual(task2, s_d_2.parent)
        
        # output: nested
        self.assertEqual(3,len(nested_wf.outputs))
        s_c_nested1=[s for s in nested_wf.outputs if s.name=="c" and s.ref.parent==task1][0]
        s_c_nested2=[s for s in nested_wf.outputs if s.name=="c" and s.ref.parent==task2][0]
        s_d_nested=[s for s in nested_wf.outputs if s.name=="d"][0]
        self.assertEqual(s_c_1, s_c_nested1.ref)
        self.assertEqual(s_c_2, s_c_nested2.ref)
        self.assertEqual(s_d_2, s_d_nested.ref)
        self.assertEqual(nested_wf, s_c_nested1.parent)
        self.assertEqual(nested_wf, s_c_nested2.parent)
        self.assertEqual(nested_wf, s_d_nested.parent)
        
        self.assertEqual(3,len(invocation.outputs))
        s_c_outer1=[s for s in invocation.outputs if s.name=="c" and s.ref.ref.parent==task1][0]
        s_c_outer2=[s for s in invocation.outputs if s.name=="c" and s.ref.ref.parent==task2][0]
        s_d_outer=[s for s in invocation.outputs if s.name=="d"][0]
        self.assertEqual(s_c_nested1, s_c_outer1.ref)
        self.assertEqual(s_c_nested2, s_c_outer2.ref)
        self.assertEqual(s_d_nested, s_d_outer.ref)
        self.assertEqual(invocation, s_c_outer1.parent)
        self.assertEqual(invocation, s_c_outer2.parent)
        self.assertEqual(invocation, s_d_outer.parent)


    def test_parallel(self):

        @parallel(iterable='x')
        def test_task_parallel(x,y):
            u,v=test_exec(a=x, b=y)
            w,z=test_exec(name="test_exec2", a=u, b=v)
            return u,w,z

        @pipeline(outputs=('u')) # a single output is returned!
        def testpipe_parallel(x,y):
            return test_task_parallel(x=x,y=y)
        
        invocation=invoke_pipeline(testpipe_parallel)
        self.assertTrue(isinstance(invocation,MethodInvocation))
        self.assertEqual(1, len(invocation.tasks))

        parallel_wf=invocation.tasks[0]
        self.assertTrue(isinstance(parallel_wf,ParallelSplit))
        self.assertEqual('test_task_parallel',parallel_wf.name)
        self.assertEqual(2,len(parallel_wf.inputs))
        s_a_n=parallel_wf.inputs["x"]
        s_b_n=parallel_wf.inputs["y"]
        self.assertEqual(Source(s_a_n.name,None,None),s_a_n)
        self.assertEqual(Source(s_b_n.name,None,None),s_b_n)
        self.assertEqual(1,len(parallel_wf.outputs))
        tuplelist=parallel_wf.outputs[0]
        self.assertEqual("tuplelist",tuplelist.name)
        self.assertEqual(parallel_wf, tuplelist.parent)
        self.assertIsNone(tuplelist.ref)

        self.assertIsNotNone(parallel_wf.body)
        self.assertTrue(isinstance(parallel_wf.body,MethodInvocation))
        nested=parallel_wf.body
        
        # test task 1
        task1=nested.tasks[0]
        self.assertTrue(isinstance(task1,TaskInvocation))
        self.assertEqual('test_exec',task1.name)
        self.assertEqual(2,len(task1.inputs))
        s_a_1=task1.inputs["a"]
        s_b_1=task1.inputs["b"]
        self.assertEqual(Source(s_a_1.name,None,None),s_a_1)
        self.assertEqual(Source(s_b_1.name,None,None),s_b_1)
        self.assertEqual(2,len(task1.outputs))
        s_c_1=[s for s in task1.outputs if s.name=="c"][0]
        s_d_1=[s for s in task1.outputs if s.name=="d"][0]
        self.assertIsNone(s_c_1.ref)
        self.assertIsNone(s_d_1.ref)
        self.assertEqual(task1, s_c_1.parent)
        self.assertEqual(task1, s_d_1.parent)
        
        # test task 2
        task2=nested.tasks[1]
        self.assertTrue(isinstance(task2,TaskInvocation))
        self.assertEqual('test_exec2',task2.name)
        self.assertEqual(2,len(task2.inputs))
        s_a_2=task2.inputs["a"]
        s_b_2=task2.inputs["b"]
        self.assertEqual(Source(s_c_1.name,task1,None),s_a_2)
        self.assertEqual(Source(s_d_1.name,task1,None),s_b_2)
        self.assertEqual(2,len(task2.outputs))
        s_c_2=[s for s in task2.outputs if s.name=="c"][0]
        s_d_2=[s for s in task2.outputs if s.name=="d"][0]
        self.assertIsNone(s_c_2.ref)
        self.assertIsNone(s_d_2.ref)
        self.assertEqual(task2, s_c_2.parent)
        self.assertEqual(task2, s_d_2.parent)
        
        # output: nested
        self.assertEqual(3,len(nested.outputs))
        s_c_nested1=[s for s in nested.outputs if s.name=="c" and s.ref.parent==task1][0]
        s_c_nested2=[s for s in nested.outputs if s.name=="c" and s.ref.parent==task2][0]
        s_d_nested=[s for s in nested.outputs if s.name=="d"][0]
        self.assertEqual(s_c_1, s_c_nested1.ref)
        self.assertEqual(s_c_2, s_c_nested2.ref)
        self.assertEqual(s_d_2, s_d_nested.ref)
        self.assertEqual(nested, s_c_nested1.parent)
        self.assertEqual(nested, s_c_nested2.parent)
        self.assertEqual(nested, s_d_nested.parent)
                
        self.assertEqual(1,len(invocation.outputs))
        tuplelist_outer=invocation.outputs[0]
        self.assertEqual("tuplelist",tuplelist_outer.name)
        self.assertEqual(invocation, tuplelist_outer.parent)
        self.assertEqual(tuplelist_outer.ref, tuplelist)


def test_exec(**kwargs):
    inputnames=("a","b")
    outputnames=("c","d")
    taskprops=TaskProperties("test_exec", "test")
    return invoke_task(taskprops, inputnames, outputnames, **kwargs)
            

