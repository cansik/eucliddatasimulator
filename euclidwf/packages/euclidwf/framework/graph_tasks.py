'''
Provides task implementations appended to the nodes in the Pydron graph.
The different implementations are associated with the structural elements 
defined in the workflow language:
* ExecTask for a TaskInvocation
* NestedGraphTask for a (nested) MethodInvocation
* ParallelSplitTask for a ParallelSplit.

When traversing the Pydron graph, the (Pydron) traverser delegates 
the handling of the tasks at the nodes to handlers that submit jobs to the 
computing infrastructure or that 'refine' the graph - both, by using 
information and functionality included in the task. Refining a graph means 
that the graph is modified at runtime e.g. for the number of splits in a 
parallel split task.
      
Created on May 19, 2015

@author: martin.melchior
'''
import os
from twisted.internet import defer
from twisted.python.failure import Failure
from pydron.dataflow import graph
from pydron.dataflow.graph import START_TICK
from pydron.dataflow.refine import insert_subgraph
from pydron.dataflow.tasks import AbstractTask
from pydron.interpreter.traverser import EvalResult
from euclidwf.framework.context import WORKDIR, CONTEXT, TRANSPORTER, LOCALWORKDIR,\
    WSROOT
import pickle

EXECTASK_REPR="ExecTask (name=%s, pkg=%s)"

class ExecTask(AbstractTask):
    """
    Contains the information needed to describe a task to be executed on the computing 
    infrastructure. 
    """
    def __init__(self, command, package, inputnames, outputnames): 
        self.command=command
        self.package=package
        self.inputnames=inputnames
        self.outputnames=outputnames

    def evaluate(self, inputs):
        raise ValueError("Task cannot be executed - it just contains the information \
                                for performing a step in the pipeline.")
            
    def refine(self, g, tick, known_inputs):
        raise ValueError("No refinement in a exec task.")
        

    def __repr__(self):
        return EXECTASK_REPR%(self.command, self.package.pkgname)
    
    def __hash__(self):
        h = hash(self.command)+hash(self.package)
        for i in self.inputnames:
            h = h + hash(i)
        for o in self.outputnames:
            h = h + hash(o)
    
    def __eq__(self, other):
        return isinstance(other,ExecTask) and self.command==other.command and \
            self.package==other.package and set(self.inputnames)==set(other.inputnames) and \
            set(self.outputnames)==set(other.outputnames)
            
    def __neq__(self, other):
        return not self.__eq__(other)


class NestedGraphTask(AbstractTask):
    """
    Task associated with a nested workflow and specified by a NestedInvocation. 
    It contains a sub-graph ('body_graph') that will be integrated into the parent 
    graph when the refine operation is invoked.  
    """    
    def __init__(self, body_graph, name):
        """
        @param body_graph: The sub-graph that is, at design time, added to the graph in detached mode
        and, runtime, will be integrated in the parent graph when refining.  
        @param name: Name of the function in the original pipeline specification.
        """
        self.body_graph = body_graph
        self.name = name
        self.refiner_ports = [CONTEXT]  # we need this to let the traverser know that this is a task to be refined.

    def evaluate(self, inputs):
        raise ValueError("Should not be evaluated in a job - should have been refined i.e. replaced by a subgraph.")
    
    def refine(self, graph, tick, known_inputs):
        """
        Refines the graph - here, means that the sub-graph defined as body is connected with the parent graph.
        Then the original node with the given task is removed - i.e. the graph defined as body of the NestedGraphTask 
        replaces the node with the given task.  
        @param graph: parent graph
        @param tick: identifies the node the given NestedGraphTask is attached to.
        @param known_inputs: Known inputs are not needed at the moment - passed to comply with the general refine 
        method signature.   
        """
        parentpath =_dataflow_path(graph, tick)
        in_connections = self._in_connections(graph, tick)
        out_connections = self._out_connections(graph, tick)

        # remove task     
        self._remove_task(graph, tick)

        # insert the subgraph
        insert_subgraph(graph, self.body_graph, tick)
        for source, dest in in_connections:
            graph.connect(source, dest)            
        for source, dest in out_connections:
            graph.connect(source, dest)
            
        # adjust path elements in the tasks of the subgraph
        addedticks=_filter_for_common_parent(graph, tick)
        for t in addedticks:
            _extend_dataflow_path(graph, t, parentpath)
        return defer.succeed(None)


    def _in_connections(self, g, tick):
        '''
        Prepare the connections to hook up the subgraph's inputs.
        '''
        # connections for the inputs - 
        in_connections=[]  
        input_map = {dest.port: source for source, dest in g.get_in_connections(tick)}
        for source, dest in self.body_graph.get_out_connections(graph.START_TICK):
            in_source = input_map[source.port]
            in_dest = graph.Endpoint(dest.tick << tick, dest.port)
            in_connections.append((in_source, in_dest))
        
        return in_connections
                
                
    def _out_connections(self, g, tick):
        """
        Connect the outputs of the body graph (inputs to the FINAL_TICK) with the outputs of the task.
        """
        # outputs could be connected to many different input ports - this is not yet covered
        out_connections=[]
        output_map = {}
        # get the out connections of the given task
        for source,dest in g.get_out_connections(tick):
            if source.port not in output_map.keys():
                output_map[source.port]=[]
            output_map[source.port].append(dest)
        for source,dest in self.body_graph.get_in_connections(graph.FINAL_TICK):
            out_source=graph.Endpoint(source.tick << tick, source.port)
            portname=dest.port
            for out_dest in output_map[portname]:
                out_connections.append((out_source, out_dest))
        return out_connections


    def _remove_task(self, g, tick):    
        for source, dest in g.get_in_connections(tick):
            g.disconnect(source, dest)
        for source, dest in g.get_out_connections(tick):
            g.disconnect(source, dest)
        g.remove_task(tick)


    def __repr__(self):
        return "NestedGraphTask(%s): \n      %s" % (self.name, self.body_graph)


class HelperTask(AbstractTask):
    pass

class _mapPortsTask(HelperTask):
    """
    Helper task that is used to map the input list elements to individual ports that can 
    be connected to the different sub-processes of a parallel split task.  
    """    
    def __init__(self, inputname, array):
        self.num_splits = len(array)
        self.array = array
        self.inputname=inputname
                
    def evaluate(self, g, tick, task, inputs):
        result=EvalResult({"%s_%s"%(self.inputname,i+1) : self.array[i] for i in range(self.num_splits)})
        return defer.succeed(result)
    
    def refine(self, g, tick, known_inputs):
        raise ValueError("No refinement of a mapPortsTask.")
    
    def __repr__(self):
        return "mapToPortsTask: %i splits" % (self.num_splits)


class _reducePortsTask(HelperTask):
    """
    Helper task that is used to collect the outputs of all the different sub-processes of a parallel split 
    (its file paths) and compose a single file containing a list tuples - each tuple containing the 
    outputs (file paths) of a single sub-process. The file is then uploaded to the workspace asynchronously.
    a deferred object (see twisted) is returned. 
    The task is executed ('evaluate') locally - i.e. it is not submitted to the computing infrastructure.   
    """
    def __init__(self, num_splits, inputnames, outputlistname):
        self.num_splits=num_splits
        self.portnames=inputnames  # names of the input arguments of the body method
        self.outputlistname=outputlistname
        self.inputnames=["%s_%i"%(_name, i+1) for _name in inputnames for i in range(self.num_splits)]
        self.outputnames=[outputlistname]

    def evaluate(self, g, tick, task, inputs):
        """
        Creates a file (locally) with the list of output tuples and uploads this file to the workspace.
        The file is uploaded asynchronously and a deferred object (twisted) is returned.
        """
        # create the output file locally - relative output path is given by the model path of the given node. 
        context=inputs[CONTEXT]
        transporter=context[TRANSPORTER]
        parentdir=_dataflow_path(g, tick)
        localpath=os.path.join(context[LOCALWORKDIR], parentdir, self.outputlistname)
        if not os.path.exists(os.path.join(str(context[LOCALWORKDIR]), parentdir)):
            os.makedirs(os.path.join(str(context[LOCALWORKDIR]), parentdir))

        outputlist=[]    
        for i in range(self.num_splits):
            tuple_from_split=tuple([ inputs["%s_%i" % (name,i+1)] for name in self.portnames ])
            outputlist.append(tuple_from_split)
        with open(localpath, 'w') as localfile:
            pickle.dump(outputlist, localfile)
        
        # now compose the remote path where this file needs to be copied to
        # use the transporter obtained the context has been initialized with
        remotepath=os.path.join(str(context[WSROOT]), str(context[WORKDIR]), parentdir, self.outputlistname)
        relativepath = os.path.join(parentdir, self.outputlistname)
        d = transporter.upload_file(localpath, remotepath)
        d.addCallback(lambda _: relativepath)

        def on_success(_path):
            return EvalResult({self.outputlistname:_path})

        def on_failure(reason):
            return Failure(ValueError(("Exception while uploading file to %s. \n"
                                        + "Reason:%s.")%(remotepath, reason.getTraceback())))

        d.addCallback(on_success)
        d.addErrback(on_failure)
        return d
    
    def refine(self, g, tick, known_inputs):
        raise ValueError("No refinement of a reducePortsTask.")

    def __repr__(self):
        return "reducePortsTask: %i splits" % (self.num_splits)
 
 
class ParallelSplitTask(AbstractTask):
    """
    Task associated with a parallel split and specified by an invocation of type ParallelSplit.
    It defines a split of the processing into an arbitrary number of 'sub-processes' that can run 
    in parallel. The split is defined based on a dedicated input variable, referred to as 'iterable', 
    which is a file that contains a list. The task is then split into n sub-processes where n is the 
    number of elements in the list. In each sub-process, the logic defined in the 'body graph' is 
    applied. This can be an arbitrary graph that contains arbitrarily nested workflow structures. 
    The sub-graph is integrated into the parent graph when the refine operation is invoked. 
    As a result of applying the parallel split we obtain a file with a list of tuples - each tuple 
    containing the paths of the output files from each parallel-splitted sub-graph. 
    """
    def __init__(self, body_graph, iterable, outputname, name):
        """
        @param body_graph: The logic to be applied in each split.
        @param iterable: The name of the argument (input port) that defines the splits.
        @param outputname: The name of the outputlist (file containing the output list) of the parallel split. 
        @param name: The name of the body function defined in the original pipeline specification. 
        """
        self.body_graph = body_graph
        self.iterator_port = iterable
        self.refiner_ports = [iterable, CONTEXT] # we need these to let the traverser know that refinement is needed.
        self.name = name
        self.outputname=outputname

    def evaluate(self, inputs):
        raise ValueError("Should not be evaluated in a job - should have been refined i.e. replaced by a subgraph.")

    
    def refine(self, g, tick, known_inputs):
        """
        Refines the task by 
        * fetching and unpacking the 'iterable' file that contains the list of input elements 
        (over which the processing is split). 
        * creating n independent copies of the sub-graph (n the number of elements found in the list);
        * adding a helper node 'map' (with a mapPortsTask) that will provide the list of input elements 
        with the inputs to the n sub-graphs;
        * creating a helper node 'reduce' (with a reducePortsTask) that will allow to connect all the outputs 
        from all the sub-graphs and provides a list with these output tuples.
        * adding and connecting all nodes (of map, reduce and all the n sub-graphs) in the parent graph;
        * removing the original parallel split node from the graph.    
        """
        d = self._fetch_refiner_data(known_inputs)
        
        def refiner_data_local(input_values):        
            list_file=input_values[self.iterator_port]
            input_list=self._load_list_from_file(list_file)
            self.adjust_graph(g, tick, input_list)
        
        def on_failure(reason):
            failure=Failure(ValueError("Exception occurred while refining tick %s. Reason: %s \n"%(tick, reason.getTraceback())))
            d.errback(failure)            
        
        d.addCallback(refiner_data_local)
        d.addErrback(on_failure)
        return d

    
    def _load_list_from_file(self, fname):
        with open(fname,'r') as f:
            listobject = pickle.load(f) 
        return listobject
    
    
    def _fetch_refiner_data(self, known_inputs):
        context=known_inputs[CONTEXT]
        
        transporter = context[TRANSPORTER]
        transfer_file_deferreds=[]
        inputs_to_fetch={port:filepath for port,filepath in known_inputs.iteritems() if port != CONTEXT}
        for port,filepath in inputs_to_fetch.iteritems():
            remote_file = str(os.path.join(context[WSROOT],context[WORKDIR],filepath))
            local_file = str(os.path.join(context[LOCALWORKDIR],filepath))
            
            def transfer_result(_):
                return port, local_file # TODO: Check whether this is safe .... --> port value? 

            d = transporter.fetch_file(remote_file, local_file)
            d.addCallback(transfer_result)
            transfer_file_deferreds.append(d)

        def _on_all_transfers_completed(transfer_results):
            input_values = {}
            for _, (port,local_file) in transfer_results:                 
                input_values[port]=local_file
            return input_values            
            
        d=defer.DeferredList(transfer_file_deferreds, fireOnOneErrback=True)
        d.addCallback(_on_all_transfers_completed)
        return d


    def adjust_graph(self, g, tick, input_list):
        parentpath=_dataflow_path(g, tick)
        # task to read from list at tick: (tick,1) - 
        # no inputs are needed for this task since data passed at refinement time
        map_task=_mapPortsTask(self.iterator_port, input_list)
        map_task_tick=graph.START_TICK+1<<tick
        mappath="%s.map"%parentpath
        g.add_task(map_task_tick, map_task, {'name':'map', 'path':mappath})
        
        # iterator port data
        source, _ = [conn for conn in g.get_in_connections(tick) if conn[1].port == self.iterator_port][0]
        dest = graph.Endpoint(map_task_tick, 'inputlist')
        g.connect(source, dest)
        
        # connect CONTEXT
        source=graph.Endpoint(START_TICK, CONTEXT)
        dest=graph.Endpoint(map_task_tick, CONTEXT)
        g.connect(source, dest)
        
        # tick to extend for the iterations
        iterations_tick=map_task_tick+1

        # task to collect elements into list at tick: (tick,3) - 
        # no inputs are needed for this task since data are passed in at refinement time
        reduce_inputnames=[source.port for source,dest in self.body_graph.get_in_connections(graph.FINAL_TICK)]       
        _, reduce_out_dest = g.get_out_connections(tick)[0]
        reduce_task=_reducePortsTask(len(input_list), reduce_inputnames, self.outputname)
        reduce_tick=iterations_tick+1
        reducepath="%s.reduce"%parentpath        
        g.add_task(reduce_tick, reduce_task, {'name':'reduce', 'path':reducepath})
        
        # context
        source=graph.Endpoint(START_TICK, CONTEXT)
        dest=graph.Endpoint(reduce_tick, CONTEXT)
        g.connect(source, dest)
                
        reduce_task_source=graph.Endpoint(reduce_tick, self.outputname)   

        # iterations
        input_map = {dest.port: source for source, dest in g.get_in_connections(tick) if dest.port != self.iterator_port}
        for iteration_counter in range(len(input_list)):
            # tick to expand the sub-graph tasks from
            iteration_tick = graph.START_TICK + iteration_counter+1 << iterations_tick

            # connections for the inputs - 
            iterator_port_source=graph.Endpoint(map_task_tick, "%s_%s"%(self.iterator_port,iteration_counter+1))            
            in_connections = self._in_connections(iteration_tick, input_map, iterator_port_source)

            # connection for the output tuple
            out_connections=self._out_connections(iteration_tick, reduce_tick, iteration_counter)

            # insert the subgraph
            insert_subgraph(g, self.body_graph, iteration_tick)

            # add connections
            for source, dest in in_connections:
                g.connect(source, dest)
            for source, dest in out_connections:
                g.connect(source, dest)

            addedticks=_filter_for_common_parent(g, iteration_tick)
            for t in addedticks:                
                parentpath_it="%s.iterations.%i"%(parentpath,iteration_counter+1)
                _extend_dataflow_path(g, t, parentpath_it)
            
        self._remove_task(g, tick)
        g.connect(reduce_task_source,reduce_out_dest)                     

        
    def _in_connections(self, iteration_tick, input_map, iterator_port_source):
        '''
        Prepare the connections to hook up the subgraph's inputs.
        '''
        iteration_input={k:v for k,v in input_map.iteritems()}
        iteration_input.update({self.iterator_port : iterator_port_source})
            
        # connections for the inputs - 
        in_connections=[]  
        for source, dest in self.body_graph.get_out_connections(graph.START_TICK):
            if dest.tick == graph.FINAL_TICK:
                continue # direct connection are treated as output connections.
            else:
                task_input = iteration_input[source.port]
                subgraph_dest = graph.Endpoint(dest.tick << iteration_tick, dest.port)
                in_connections.append((task_input, subgraph_dest))
        
        return in_connections
                
                
    def _out_connections(self, iteration_tick, collector_tick, iteration):
        out_connections=[]
        for source,_ in self.body_graph.get_in_connections(graph.FINAL_TICK):
            out_source=graph.Endpoint(source.tick << iteration_tick, source.port)
            out_dest=graph.Endpoint(collector_tick, "%s_%s" % (source.port, iteration+1))
            out_connections.append((out_source, out_dest))
        return out_connections


    def _remove_task(self, g, tick):    
        for source, dest in g.get_in_connections(tick):
            g.disconnect(source, dest)
        for source, dest in g.get_out_connections(tick):
            g.disconnect(source, dest)
        g.remove_task(tick)

       
    def __repr__(self):
        return "ParallelSplitTask(%s): \n      %s" % (self.name, self.body_graph)
        

def get_ticks_by_property(graph, key, value):
    selected_ticks=[]
    for tick in graph.get_all_ticks():
        props=graph.get_task_properties(tick)
        if key in props.keys():
            if value == props[key]:
                selected_ticks.append(tick)
    return selected_ticks


def _dataflow_path(g, tick):    
    return g.get_task_properties(tick)['path']


def _extend_dataflow_path(g, tick, parentpath):
    path=g.get_task_properties(tick)['path']
    g.set_task_property(tick, 'path', parentpath+"."+path)


def _filter_for_common_parent(g, parenttick):
    ticks=[]
    parent_depth=len(parenttick._elements)
    for t in g.get_all_ticks():
        depth=len(t._elements)
        if depth>parent_depth:
            reftick=t>>depth-parent_depth
            if reftick==parenttick:
                ticks.append(t)
    return ticks