'''
Created on May 18, 2015

@author: martin.melchior
'''
import unittest
from pydron.dataflow.graph import Graph, G, C, T, START_TICK, FINAL_TICK
from pydron.dataflow import utils
from euclidwf.framework.graph_builder import build_graph, PydronGraphBuilder
from euclidwf.framework.graph_tasks import ExecTask, NestedGraphTask, ParallelSplitTask
from euclidwf.utilities.error_handling import PipelineGraphError
from euclidwf.framework.taskdefs import TaskProperties, Package
from euclidwf.framework.workflow_dsl import invoke_task, nested, parallel, Invocation


class TestGraphBuilder(unittest.TestCase):

    def test_simple_task(self):
        """
        A simple one-step pipeline 
        """
        def testpipe(x,y):
            u,v=test_exec(a=x, b=y)
            return u,v
        
        graph=build_graph(testpipe)
        self.assertTrue(isinstance(graph,Graph))
        
        # there is just a single task - hence just a single node / tick
        self.assertEqual(1, len(graph.get_all_ticks()))
        task=graph.get_task(graph.get_all_ticks()[0])
        self.assertTrue(isinstance(task,ExecTask))
        self.assertEqual('test_exec',task.command)
        self.assertEqual(set(['a','b']),set(task.inputnames))
        self.assertEqual(set(['c','d']),set(task.outputnames))

        expected =  G(
            C(START_TICK, 'a', 1,'a'),
            C(START_TICK, 'b', 1,'b'),
            C(START_TICK, 'context', 1,'context'),
            T(1, task, {'name': 'test_exec', 'path':'test_exec'}),
            C(1, 'c', FINAL_TICK,'test_exec.c'),
            C(1, 'd', FINAL_TICK,'test_exec.d')
        )
        expected.set_task_property(FINAL_TICK,'aliases', 
            {'test_exec.c':'test_exec.c', 'test_exec.d':'test_exec.d'})
        
        utils.assert_graph_equal(expected, graph)


    def test_two_steps(self):
        
        def testpipe(x,y):
            u,v=test_exec(a=x, b=y)
            w,z=test_exec(name="test_exec2", a=u, b=v)
            return u,w,z
        
        graph=build_graph(testpipe)
        self.assertTrue(isinstance(graph,Graph))        
        
        # there are two tasks
        self.assertEqual(2, len(graph.get_all_ticks()))
        ticks=sorted(graph.get_all_ticks())        
        task1=graph.get_task(ticks[0])
        task2=graph.get_task(ticks[1])

        self.assertTrue(isinstance(task1,ExecTask))
        self.assertEqual('test_exec',task1.command)
        self.assertEqual(set(['a','b']),set(task1.inputnames))
        self.assertEqual(set(['c','d']),set(task1.outputnames))

        self.assertTrue(isinstance(task2,ExecTask))
        self.assertEqual('test_exec',task2.command)
        self.assertEqual(set(['a','b']),set(task2.inputnames))
        self.assertEqual(set(['c','d']),set(task2.outputnames))

        expected =  G(
            C(START_TICK, 'a', 1,'a'),
            C(START_TICK, 'b', 1,'b'),
            C(START_TICK, 'context', 1,'context'),
            C(START_TICK, 'context', 2,'context'),
            T(1, task1, {'name': 'test_exec', 'path':'test_exec'}),
            C(1, 'c', 2, 'a'),
            C(1, 'd', 2, 'b'),
            T(2, task2, {'name': 'test_exec2', 'path':'test_exec2'}),
            C(1, 'c', FINAL_TICK,'test_exec.c'),
            C(2, 'c', FINAL_TICK,'test_exec2.c'),
            C(2, 'd', FINAL_TICK,'test_exec2.d')
        )
        expected.set_task_property(FINAL_TICK, 'aliases', 
            {'test_exec.c':'test_exec.c', 'test_exec2.c':'test_exec2.c', 'test_exec2.d':'test_exec2.d'})

        utils.assert_graph_equal(expected, graph)


    def test_inline(self):

        def testpipe(x,y):
            return test_task(x=x,y=y)

        def test_task(x,y):
            u,v=test_exec(a=x, b=y)
            w,z=test_exec(name="test_exec2", a=u, b=v)
            return u,w,z
        
        graph = build_graph(testpipe)

        # there are two tasks
        self.assertEqual(2, len(graph.get_all_ticks()))
        ticks=sorted(graph.get_all_ticks())        
        task1=graph.get_task(ticks[0])
        task2=graph.get_task(ticks[1])

        self.assertTrue(isinstance(task1,ExecTask))
        self.assertEqual('test_exec',task1.command)
        self.assertEqual(set(['a','b']),set(task1.inputnames))
        self.assertEqual(set(['c','d']),set(task1.outputnames))

        self.assertTrue(isinstance(task2,ExecTask))
        self.assertEqual('test_exec',task2.command)
        self.assertEqual(set(['a','b']),set(task2.inputnames))
        self.assertEqual(set(['c','d']),set(task2.outputnames))

        expected =  G(
            C(START_TICK, 'a', 1,'a'),
            C(START_TICK, 'b', 1,'b'),
            C(START_TICK, 'context', 1,'context'),
            C(START_TICK, 'context', 2,'context'),
            T(1, task1, {'name': 'test_exec', 'path':'test_exec'}),
            C(1, 'c', 2, 'a'),
            C(1, 'd', 2, 'b'),
            T(2, task2, {'name': 'test_exec2', 'path':'test_exec2'}),
            C(1, 'c', FINAL_TICK,'test_exec.c'),
            C(2, 'c', FINAL_TICK,'test_exec2.c'),
            C(2, 'd', FINAL_TICK,'test_exec2.d')
        )
        expected.set_task_property(FINAL_TICK,'aliases', 
            {'test_exec.c':'test_exec.c', 'test_exec2.c':'test_exec2.c', 'test_exec2.d':'test_exec2.d'})
        
        utils.assert_graph_equal(expected, graph)


    def test_nested(self):
        
        graph = build_graph(testpipe_nested)

        # there are two tasks
        self.assertEqual(1, len(graph.get_all_ticks()))
        nested=graph.get_task(graph.get_all_ticks()[0])

        self.assertTrue(isinstance(nested,NestedGraphTask))
        self.assertEqual('test_task_nested',nested.name)

        task1=ExecTask('test_exec', Package('test'), ['a','b'], ['c','d'])
        task2=ExecTask('test_exec', Package('test'), ['a','b'], ['c','d'])

        subgraph =  G(
            C(START_TICK, 'a', 1,'a'),
            C(START_TICK, 'b', 1,'b'),
            C(START_TICK, 'context', 1,'context'),
            C(START_TICK, 'context', 2,'context'),
            T(1, task1, {'name': 'test_exec', 'path':'test_exec'}),
            C(1, 'c', 2, 'a'),
            C(1, 'd', 2, 'b'),
            T(2, task2, {'name': 'test_exec2', 'path':'test_exec2'}),
            C(1, 'c', FINAL_TICK,'test_exec.c'),
            C(2, 'c', FINAL_TICK,'test_exec2.c'),
            C(2, 'd', FINAL_TICK,'test_exec2.d')
        )
        subgraph.set_task_property(FINAL_TICK,'aliases', 
            {'test_exec.c':'test_exec.c', 'test_exec2.c':'test_exec2.c', 'test_exec2.d':'test_exec2.d'})
        utils.assert_graph_equal(subgraph, nested.body_graph)
        
        expected =  G(
            C(START_TICK, 'x', 1,'x'),
            C(START_TICK, 'y', 1,'y'),
            C(START_TICK, 'context', 1,'context'),
            T(1, nested, {'name': 'test_task_nested', 'path':'test_task_nested'}),
            C(1, 'test_exec.c', FINAL_TICK,'test_task_nested.test_exec.c'),
            C(1, 'test_exec2.c', FINAL_TICK,'test_task_nested.test_exec2.c'),
            C(1, 'test_exec2.d', FINAL_TICK,'test_task_nested.test_exec2.d')
        )
        expected.set_task_property(FINAL_TICK,'aliases',
            {'test_task_nested.test_exec.c':'test_task_nested.test_exec.c', 
             'test_task_nested.test_exec2.c':'test_task_nested.test_exec2.c',
             'test_task_nested.test_exec2.d':'test_task_nested.test_exec2.d'})

        utils.assert_graph_equal(expected, graph)

        

    def test_parallel(self):
        
        graph = build_graph(testpipe_parallel)

        # there are two tasks
        self.assertEqual(1, len(graph.get_all_ticks()))
        parallel=graph.get_task(graph.get_all_ticks()[0])

        self.assertTrue(isinstance(parallel,ParallelSplitTask))
        self.assertEqual('test_task_parallel',parallel.name)

        task1=ExecTask('test_exec', Package('test'), ['a','b'], ['c','d'])
        task2=ExecTask('test_exec', Package('test'), ['a','b'], ['c','d'])

        subgraph =  G(
            C(START_TICK, 'a', 1,'a'),
            C(START_TICK, 'b', 1,'b'),
            C(START_TICK, 'context', 1,'context'),
            C(START_TICK, 'context', 2,'context'),
            T(1, task1, {'name': 'test_exec', 'path':'test_exec'}),
            C(1, 'c', 2, 'a'),
            C(1, 'd', 2, 'b'),
            T(2, task2, {'name': 'test_exec2', 'path':'test_exec2'}),
            C(1, 'c', FINAL_TICK,'test_exec.c'),
            C(2, 'c', FINAL_TICK,'test_exec2.c'),
            C(2, 'd', FINAL_TICK,'test_exec2.d')
        )
        subgraph.set_task_property(FINAL_TICK,'aliases',
            {'test_exec.c':'test_exec.c', 'test_exec2.c':'test_exec2.c', 'test_exec2.d':'test_exec2.d'})
        utils.assert_graph_equal(subgraph, parallel.body_graph)
        
        expected =  G(
            C(START_TICK, 'x', 1,'x'),
            C(START_TICK, 'y', 1,'y'),
            C(START_TICK, 'context', 1,'context'),
            T(1, parallel, {'name': 'test_task_parallel', 'path':'test_task_parallel'}),
            C(1, 'tuplelist', FINAL_TICK,'test_task_parallel.tuplelist'),
        )
        expected.set_task_property(FINAL_TICK,'aliases',{'test_task_parallel.tuplelist':'test_task_parallel.tuplelist'})
        utils.assert_graph_equal(expected, graph)


    def test_unsupported_invocation_type(self):
        def testpipe(x,y):
            u,v=test_exec(a=x, b=y)
            return u,v
        
        class FakeInvocation(Invocation):
            def __init__(self, name):
                Invocation.__init__(self, name)
            
        builder=PydronGraphBuilder(testpipe)
        builder.build()
        ticks=sorted(builder.graph.get_all_ticks())
        last_tick=ticks[-1]
        try:
            builder.add_task(last_tick, FakeInvocation('fake'))
            self.fail("Unsupported Invocations should be caught.")
        except PipelineGraphError:
            pass


testpkg=Package(pkgname='test')

def test_exec(**kwargs):
    inputnames=("a","b")
    outputnames=("c","d")
    taskprops=TaskProperties("test_exec", testpkg)
    return invoke_task(taskprops, inputnames, outputnames, **kwargs)
            
@nested()
def test_task_nested(x,y):
    u,v=test_exec(a=x, b=y)
    w,z=test_exec(name="test_exec2", a=u, b=v)
    return u,w,z

def testpipe_nested(x,y):
    return test_task_nested(x=x,y=y)


@parallel(iterable='x')
def test_task_parallel(x,y):
    u,v=test_exec(a=x, b=y)
    w,z=test_exec(name="test_exec2", a=u, b=v)
    return u,w,z

def testpipe_parallel(x,y):
    return test_task_parallel(x=x,y=y)


