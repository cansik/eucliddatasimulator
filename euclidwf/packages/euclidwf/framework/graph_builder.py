"""Functionality to translate a pipeline specification into a Pydron graph. 

The pipeline specification is assumed to be specified by using the dataflow API
as described in the workflow_dsl module. The PydronGraphBuilder then uses the 
workflow_dsl module to parse the pipeline specification and, from the data 
structure provided (similar to an expression tree), generates a Pydron graph.  

The result is a static representation - with nested graphs added to the main graph 
in detached mode. Detached mode means that the inputs and outputs of the sub-graphs 
are not connected with the parent graph. This linking of sub-graphs into the parent
graph takes places at runtime when the graph is traversed.  
 
A special input that is not defined in the pipeline specification but that is added as 
input to all nodes of the graph is the runtime context or simply context. This will 
contain additional configuration information that is needed when traversing the graph 
and configuring jobs submitted to the computing infrastructure. Context information is 
not modified when processing the pipeline - hence it is never defined as output from 
tasks and handed over as input to other tasks, i.e. it has not impact on the graph 
structure.

Created on May 18, 2015

@author: martin.melchior (at) fhnw.ch
"""
from pydron.dataflow import graph
from pydron.dataflow.graph import START_TICK, FINAL_TICK
from euclidwf.framework.context import CONTEXT
from euclidwf.framework.graph_tasks import ExecTask, NestedGraphTask, ParallelSplitTask
from euclidwf.framework.workflow_dsl import MethodInvocation, ParallelSplit, invoke_pipeline,\
    TaskInvocation
from euclidwf.utilities.error_handling import PipelineGraphError
    

def build_graph(method):
    """
    Function to create a static Pydron graph from a pipeline specification.  
    """
    builder=PydronGraphBuilder(method)
    builder.build()
    return builder.graph


class PydronGraphBuilder(object):

    def __init__(self, method):
        """
        Initializes the builder with a method that defines the top-level pipeline workflow.
        It parses the pipeline specification and creates an expression tree (or the like) 
        which serves as a basis to build the graph - see build-method.
        @param method: pipeline function that describes the top-level pipeline workflow.
        """
        self.method=method
        self.invocation=invoke_pipeline(method)
        self.graph=graph.Graph()
        self.ticks={} # invocation as key, tick as value
    
    def build(self):
        """
        Translates the expression tree into a (static) Pydron graph. 
        Furthermore, it adds a 'context' variable to all tasks defined in the graph. This will
        be used to ingest runtime information to the nodes when traversing the graph.   
        """
        tick=START_TICK
        for t in self.invocation.tasks:
            tick=self.add_task(tick,t)
        self.add_outputs()
        self.add_context()


    def add_task(self, tick, invocation):
        """
        Adds for the given invocation an associated node/task to the graph.
        * for TaskInvocation: ExecTask
        * for MethodInvocation: NestedGraphTask
        * for ParallelSplit: ParallelSplitTask
        In addition, it sets the following properties in the graph:
        * name: name of the original function invoked in the pipeline script
        * path: path to the node for the given task within the graph; note that at design time the graph is
        flat, all paths just contain a single element and nested graphs are included in detached mode 
        with paths consisting of a single element.   
        """
        props={'name':invocation.name, 'path':invocation.name}
        tick=tick+1
        if isinstance(invocation, MethodInvocation):
            body_graph=build_graph(invocation.body_method)
            task = NestedGraphTask(body_graph, invocation.name)
        elif isinstance(invocation, ParallelSplit):
            body_graph=build_graph(invocation.body_method)            
            outputname = invocation.outputs[0].name
            task = ParallelSplitTask(body_graph, invocation.iterable, outputname, invocation.name)
        elif isinstance(invocation, TaskInvocation):
            command=invocation.properties['command']
            package=invocation.properties['package']
            inputnames=invocation.inputs.keys()
            outputnames=set([p.name for p in invocation.outputs])        
            task=ExecTask(command, package, inputnames, outputnames)
        else:
            raise PipelineGraphError("No task implementation available for invocation of type %s."%str(type(invocation)))
        self.graph.add_task(tick, task, props)
        self.ticks[invocation]=tick
        self.add_connections(invocation, tick)
        return tick
    
    
    def add_connections(self, task, tick):
        """
        Connects each input of a given task with suitable outputs of tasks (possibly of the task 
        with the start tick). From the source fed to the invocation we can find the output it needs 
        to connect to.
        Note that whereas for each input there is only a single connection - 
        an output may be connected to many inputs.          
        """
        for inputname, source in task.inputs.iteritems():
            endpoint=graph.Endpoint(tick, inputname)
            if source.parent:
                source_tick = self.ticks[source.parent]
                source_name = _portname_from_source(source)                     
            else: # in this case it can only be an input to the dataflow
                source_tick = START_TICK
                source_name = source.name
            startpoint=graph.Endpoint(source_tick, source_name)
            self.graph.connect(startpoint,endpoint)                
        

    def add_outputs(self):
        """
        Connects the outputs of node representing the pipeline function 
        with the final tick. Furthermore, it adds a 'aliases' property to
        the FINAL_TICK which contains the mapping of the outputs of the 
        dataflow graph to the portnames.  
        """
        aliases={}
        for s in self.invocation.outputs:
            modelpath=_portname_from_source(s)
            aliases[modelpath] = s.alias  if s.alias else modelpath
        self.graph.set_task_property(graph.FINAL_TICK, 'aliases', aliases)
        for source in self.invocation.outputs:
            endpoint=graph.Endpoint(FINAL_TICK, _portname_from_source(source))
            source_tick = self.ticks[source.ref.parent]
            source_port = _portname_from_source(source.ref)
            startpoint=graph.Endpoint(source_tick, source_port)
            self.graph.connect(startpoint,endpoint)


    def add_context(self):
        """
        Add an output to the start tick - with name 'context' and connects that to 
        an endpoint called 'context' defined at each node of the graph.
        """
        source=graph.Endpoint(START_TICK, CONTEXT)
        for _,tick in self.ticks.iteritems():
            dest=graph.Endpoint(tick, CONTEXT)
            self.graph.connect(source, dest)


def _portname_from_source(source):
    return '.'.join(reversed(source.path[:-1]))
        
    