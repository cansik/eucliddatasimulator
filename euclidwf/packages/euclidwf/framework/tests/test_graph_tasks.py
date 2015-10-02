'''
Created on May 18, 2015

@author: martin.melchior
'''
import os
import pickle
import tempfile
import unittest

from pydron.dataflow import utils
from pydron.dataflow.graph import G, C, T, START_TICK, FINAL_TICK, Graph, Endpoint, Tick

from euclidwf.framework.graph_tasks import NestedGraphTask, ExecTask, get_ticks_by_property,\
    ParallelSplitTask, _mapPortsTask, _reducePortsTask
from euclidwf.framework.taskprops import PackageSource
from euclidwf.utilities.file_transporter import LocalFileTransporter


class TestGraphTasks(unittest.TestCase):

    def test_nested_task(self):
        pkg=PackageSource('testpkg', 'pkgfile', 'NONE')
        exec_task = ExecTask("exec_name", pkg, ('a',), ('b',))
        subgraph =  G(
            C(START_TICK, 'a', 1,'a'),
            C(START_TICK, 'context', 1,'context'),
            T(1, exec_task, {'name': 'test_exec', 'path':'test_exec'}),
            C(1, 'b', FINAL_TICK,'b'),
        )
        
        subgraph_task_tick=subgraph.get_all_ticks()[0]
        graph=Graph()
        task=NestedGraphTask(subgraph, 'test_nested')
        tick=START_TICK+1
        graph.add_task(tick, task, {'name':'test_nested','path':'test_nested'})
        source=Endpoint(START_TICK, 'a')
        dest=Endpoint(tick, 'a')
        graph.connect(source,dest)
        source=Endpoint(START_TICK, 'context')
        dest=Endpoint(tick, 'context')
        graph.connect(source,dest)
        source=Endpoint(tick, 'b')
        dest=Endpoint(FINAL_TICK, 'b')
        graph.connect(source,dest)

        task.refine(graph, Tick.parse_tick(1), {'context':{},'a':{}})

        expected_graph_task_tick=subgraph_task_tick<<Tick.parse_tick(1)        
        expected =  G(
            C(START_TICK, 'a', expected_graph_task_tick,'a'),
            C(START_TICK, 'context', expected_graph_task_tick,'context'),
            T(expected_graph_task_tick, exec_task, {'name': 'test_exec', 'path':'test_nested.test_exec'}),
            C(expected_graph_task_tick, 'b', FINAL_TICK,'b'),
        )
        utils.assert_graph_equal(expected, graph)
        
    
    def test_parallel_split(self):
        pkg=PackageSource('testpkg', 'pkgfile', 'NONE')
        exec_task = ExecTask("exec_name", pkg, ('a','b'), ('c','d'))
        subgraph =  G(
            C(START_TICK, 'a', 1,'a'),
            C(START_TICK, 'b', 1,'b'),
            C(START_TICK, 'context', 1,'context'),
            T(1, exec_task, {'name': 'test_exec', 'path':'test_exec'}),
            C(1, 'c', FINAL_TICK,'c'),
            C(1, 'd', FINAL_TICK,'d'),
        )
        
        subgraph_task_tick=subgraph.get_all_ticks()[0]
        graph=Graph()
        task=ParallelSplitTask(subgraph, 'b', 'cd_list', 'ps')
        tick=START_TICK+1
        graph.add_task(tick, task, {'name':'ps','path':'ps'})
        source=Endpoint(START_TICK, 'a')
        dest=Endpoint(tick, 'a')
        graph.connect(source,dest)
        source=Endpoint(START_TICK, 'b')
        dest=Endpoint(tick, 'b')
        graph.connect(source,dest)
        source=Endpoint(START_TICK, 'context')
        dest=Endpoint(tick, 'context')
        graph.connect(source,dest)
        source=Endpoint(tick, 'cd_list')
        dest=Endpoint(FINAL_TICK, 'cd_list')
        graph.connect(source,dest)

        testdir=tempfile.mkdtemp()
        workdir=os.path.join(testdir,'workdir')
        os.mkdir(workdir)
        testfile=os.path.join(workdir, 'testfile.pickle')
        local_workdir=os.path.join(testdir,'local_workdir')
        os.mkdir(local_workdir)
        with open(testfile,'wb') as f:
            pickle.dump(['x','y','z'],f)
        context={'transporter': LocalFileTransporter(), 'workdir':workdir, 'local_workdir':local_workdir}
        task.refine(graph, Tick.parse_tick(1), {'b':'testfile.pickle', 'context':context})

        mapTask=_mapPortsTask('b', ['x','y','z'])
        map_task_tick=START_TICK+1<<tick

        iterations_tick=map_task_tick+1
        it_1 = START_TICK + 1 << iterations_tick
        it_2 = START_TICK + 2 << iterations_tick
        it_3 = START_TICK + 3 << iterations_tick
        task_tick_1=subgraph_task_tick<<it_1
        task_tick_2=subgraph_task_tick<<it_2
        task_tick_3=subgraph_task_tick<<it_3
                
        reduceTask=_reducePortsTask(3, ['c','d'], 'cd_list')
        reduce_tick=iterations_tick+1
        
        expected =  G(
            C(START_TICK, 'context', map_task_tick,'context'),
            C(START_TICK, 'a', task_tick_1,'a'),
            C(START_TICK, 'a', task_tick_2,'a'),
            C(START_TICK, 'a', task_tick_3,'a'),
            C(START_TICK, 'b', map_task_tick,'inputlist'),
            C(START_TICK, 'context', task_tick_1,'context'),
            C(START_TICK, 'context', task_tick_2,'context'),
            C(START_TICK, 'context', task_tick_3,'context'),
            T(map_task_tick, mapTask, {'name': 'map', 'path':'ps.map'}),
            C(map_task_tick, 'b_1', task_tick_1,'b'),
            C(map_task_tick, 'b_2', task_tick_2,'b'),
            C(map_task_tick, 'b_3', task_tick_3,'b'),
            T(task_tick_1, exec_task, {'name': 'test_exec', 'path':'ps.iterations.1.test_exec'}),
            T(task_tick_2, exec_task, {'name': 'test_exec', 'path':'ps.iterations.2.test_exec'}),
            T(task_tick_3, exec_task, {'name': 'test_exec', 'path':'ps.iterations.3.test_exec'}),
            C(task_tick_1, 'c', reduce_tick,'c_1'),
            C(task_tick_2, 'c', reduce_tick,'c_2'),
            C(task_tick_3, 'c', reduce_tick,'c_3'),
            C(task_tick_1, 'd', reduce_tick,'d_1'),
            C(task_tick_2, 'd', reduce_tick,'d_2'),
            C(task_tick_3, 'd', reduce_tick,'d_3'),
            C(START_TICK, 'context', reduce_tick,'context'),
            T(reduce_tick, reduceTask, {'name': 'reduce', 'path':'ps.reduce'}),
            C(reduce_tick, 'cd_list', FINAL_TICK,'cd_list'),
        )
        utils.assert_graph_equal(expected, graph)        
        self.assertTrue(os.path.exists(os.path.join(local_workdir,'testfile.pickle')))
        
        
    def test_evaluate_reduce_task(self):
        testdir=tempfile.mkdtemp()
        workdir=os.path.join(testdir,'workdir')
        os.mkdir(workdir)
        local_workdir=os.path.join(testdir,'local_workdir')
        os.makedirs(os.path.join(local_workdir,'ps.reduce'))
        testfile=os.path.join(local_workdir, 'ps.reduce','cd_list')
        with open(testfile,'wb') as f:
            pickle.dump([('x_1','y_1'), ('x_2','y_2'), ('x_3','y_3')],f)
        context={'transporter': LocalFileTransporter(), 'workdir':workdir, 'local_workdir':local_workdir}

        reduce_task=_reducePortsTask(3, ['c','d'], 'cd_list')
        reduce_tick=START_TICK+3<<Tick.parse_tick(1)        
        g =  G(
            C(START_TICK, 'context', reduce_tick,'context'),
            T(reduce_tick, reduce_task, {'name': 'reduce', 'path':'ps.reduce'}),
            C(reduce_tick, 'cd_list', FINAL_TICK,'cd_list'),
        )
        
        inputs={}
        inputs['context']=context
        inputs['c_1']='x_1'
        inputs['d_1']='y_1'
        inputs['c_2']='x_2'
        inputs['d_2']='y_2'
        inputs['c_3']='x_3'
        inputs['d_3']='y_3'                
        result=[('x_1','y_1'),('x_2','y_2'),('x_3','y_3')]
        reduce_task.evaluate(g, reduce_tick, reduce_task, inputs)
        outputfile=os.path.join(workdir,'ps.reduce','cd_list')
        self.assertTrue(os.path.exists(outputfile))
        with open(outputfile,'r') as f:
            output=pickle.load(f)
            self.assertEqual(result, output)
            
    
    def test_task_by_property(self):
        pkg=PackageSource('testpkg', 'pkgfile', 'NONE')
        exec_task1 = ExecTask("exec_name1", pkg, ('a',), ('b',))
        exec_task2 = ExecTask("exec_name2", pkg, ('a',), ('b',))
        exec_task3 = ExecTask("exec_name3", pkg, ('a',), ('b',))
        graph =  G(
            C(START_TICK, 'a', 1,'a'),
            C(START_TICK, 'context', 1,'context'),
            T(1, exec_task1, {'name': 'test_exec1', 'path':'test_exec1', 'prop1':'a'}),
            T(2, exec_task2, {'name': 'test_exec2', 'path':'test_exec2', 'prop1':'a'}),
            T(3, exec_task3, {'name': 'test_exec3', 'path':'test_exec3', 'prop1':'b'}),
            C(1, 'b', FINAL_TICK,'b'),
        )
        self.assertEquals([Tick.parse_tick(1)], get_ticks_by_property(graph, 'name', 'test_exec1'))
        self.assertEquals([Tick.parse_tick(1), Tick.parse_tick(2)], get_ticks_by_property(graph, 'prop1', 'a'))
        self.assertEquals([], get_ticks_by_property(graph, 'prop2', 'a'))
        
        
    def test_mapPortsTask(self):
        # TODO
        pass
    
    def test_reducePortsTask(self):
        # TODO
        pass
