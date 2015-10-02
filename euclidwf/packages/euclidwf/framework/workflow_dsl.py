"""Defines the workflow language elements used to specify Euclid pipeline workflow.

This module defines the elements that constitute the 'domain specific language' 
designed to specify the dataflow for Euclid pipelines. The following workflow 
patterns are supported:

 * Sequential execution of pipeline tasks
 * Nested dataflows  
 * Parallel split
 
The atomic steps within a pipeline consist in the execution of tasks on a 
computing infrastructure. In the pipeline specification, we do not invoke 
the task directly - rather, we specify the details about what task is to be 
executed, from where the inputs are provided (either as input to the pipeline 
or as output of a previously executed task) and where the outputs will be 
consumed. 
The execution of a task is specified by suitable proxy functions that are 
generated from the package definition files (@see: taskdef) and that 
 * have the same name as the original executable (but are located in a 
 different namespace),
 * accept arguments with identical names as the input ports defined for the 
 original task
 * provide a tuple of returns values in the order as specified in the taskdefs.

Sequential execution of tasks is specified by setting variables declared 
as outputs of tasks as inputs of subsequent tasks.

For convenience, you can specify functions in your pipeline script and use 
them in the definition of the dataflow by invoking them in exactly the same 
way as the task proxy functions. When interpreting the pipeline specification
the dataflow logic defined in these functions is inlined. Therefore, we call them 
inline functions.

When decorating such a function with @nested() a nested dataflow is defined. 
In contrast to the inline functions the sub-dataflow defined in by the method is 
added to the overall dataflow in 'detached mode' - i.e. inputs and outputs are 
connected with the parent dataflow only at runtime - when executing the dataflow. 
Nested dataflows could be used (in the future) to define dataflow levels and 
submit jobs that process sub-dataflows defined at a given level to a single compute 
node in the processing infrastructure.

Parallel splits are defined by setting the @parallel decorator to a function. 
The function defines the logic to be executed per split ('body'). As an argument to the 
@parallel decorator you need to specify the 'iterable' (i.e. @parallel(iterable=myarg)) 
- the name of one of the arguments defined in the signature of the function. The split 
will be defined on the basis of this 'iterable'. It means that as 'iterable' a 'list file' 
is passed into the parallel split. The function body then sees the individual elements 
of this list. This list is only available at runtime - it is a pickled python list of file
paths. At runtime, the output from the parallel split, a pickled list of output tuples will
be returned - each tuple containing the output returned from the body method.
    
A pipeline consists of a pipeline function - a python function decorated with
@pipeline which contains invocations of task proxy functions, inline functions, 
invocations of nested dataflows or parallel splits.

Remark: With the exception of the decorators and the task proxy methods we do not 
support invocation of functions defined in other modules. Hence: No imports from 
other namespaces with the exception of  ... (TODO).

For a textual representation of the workflow structure defined in the script 
(without the details of how the data flows) you can simple state:
print invoke_pipeline(pipeline_method)

This however is not yet brought into the graph structure which may be more intuitive to look at:


Created on May, 2015

@author: martin.melchior (at) fhnw.ch

"""
import imp
import inspect
import os
import traceback

from euclidwf.utilities import thread_local
from euclidwf.utilities.utils import flatten_tuple
from euclidwf.utilities.error_handling import ConfigurationError, PipelineFrameworkError, PipelineSpecificationError 

#--------------------------------------------------
# API Methods
#--------------------------------------------------
def invoke_task(taskprops, inputnames, outputnames, **kwargs):
    """ 
    Utility method used in the automatically generated task proxy functions.
    """
    invoke=TaskInvocation(taskprops, inputnames, outputnames, **kwargs)
    thread_local.add_item(invoke)
    return tuple(invoke.outputs)


def invoke_pipeline(pipeline_function):
    """
    Utility function to create for a given pipeline specification an 'expression tree' 
    consisting of the invocations of the tasks, nested dataflows, parallel splits.
    
    @param pipeline_function: The main function that declares the pipeline.
    """
    argnames=pipeline_function.func_code.co_varnames[ : pipeline_function.func_code.co_argcount]
    kwargs={name:None for name in argnames}
    pipeline_function.isroot=True
    invocation=MethodInvocation(pipeline_function, **kwargs)
    return invocation
    

def nested(outputs=None, properties={}):
    """
    Function defining the @nested() decorator used to declare nested dataflows.
    """
    def nested(function):
        def inner(**kwargs):
            function._outputs=outputs
            function._properties=properties
            wf = MethodInvocation(function, **kwargs)
            thread_local.add_item(wf)                
            return tuple(wf.outputs)
        return inner
    return nested


def parallel(iterable, outputs=("tuplelist",), properties={}):
    """
    Function defining the @parallel() decorator used to declare parallel splits.
    """    
    def nested(function):
        def inner(**kwargs):
            #function._outputs=outputs
            function._properties=properties                
            if not outputs:
                raise PipelineSpecificationError("Output of parallel split must not be None. By skipping the arguments \
                                                    'tuplelist' will be used by default.")
            if len(outputs) != 1:
                raise PipelineSpecificationError("Parallel split returns exactly one output variable. \
                                                    Accordingly, you must specify as outputs exactly one name. \
                                                    By skipping the arguments 'tuplelist' will be used by default.")                
            ps = ParallelSplit(function, iterable, outputs[0], **kwargs)
            thread_local.add_item(ps)
            return tuple(ps.outputs)
        return inner
    return nested


def pipeline(outputs, properties={}):
    """
    Function defining the @pipeline decorator used as a marker to declare the main pipeline function. 
    As required argument, you must specify the portnames associated with each of the output variables. 
    These portnames will then constitute the unique reference for the output metadata files. 
    """    
    def nested(function):
        function.ispipeline=True
        if not outputs:
            raise PipelineSpecificationError("Specify output names - to be associated with the variables returned by the pipeline function.")
        function._outputs=outputs
        function.properties=properties
        return function
    return nested

#--------------------------------------------------
# Utility methods 
#--------------------------------------------------
def load_pipeline_from_file(pipeline_file):
    if not os.path.exists(pipeline_file):
        raise ConfigurationError("The file path passed as pipeline specification does not exist: %s"%pipeline_file)
    filename=os.path.basename(pipeline_file)
    pipeline_module_name,_=os.path.splitext(filename)
    pipeline_module=imp.load_source(pipeline_module_name, pipeline_file)    
    pipeline_funcs=[fun for _,fun in inspect.getmembers(pipeline_module, inspect.isfunction) if hasattr(fun, 'ispipeline')]
    if len(pipeline_funcs)==0:
        raise PipelineSpecificationError("No function in the pipeline script is marked as pipeline - use @pipeline.")
    elif len(pipeline_funcs)>1:
        raise PipelineSpecificationError("More than one function in the pipeline script is marked as pipeline: %s"%(', '.join([f.name for f in pipeline_funcs])))
    else:
        return pipeline_funcs[0]


def get_portnames(pipeline_file):
    """
    Returns a map with the input and output port names for the pipeline specified in the given file. 
    """
    pipeline_function = load_pipeline_from_file(pipeline_file)
    inputnames=pipeline_function.func_code.co_varnames[ : pipeline_function.func_code.co_argcount]
    invocation=invoke_pipeline(pipeline_function)
    outputnames=tuple([s.alias for s in invocation.outputs])
    return { 'inputs':inputnames, 'outputs':outputnames }


def get_invoked_execs(pipeline_function):
    """
    Utility function that returns all tasks invoked in a pipeline specification.
    The tasks are returned as dictionary with the package as key and the list of 
    tasks defined in the given package as value.  

    @param pipeline_function: The main function that declares the pipeline.
    """        
    invocation=invoke_pipeline(pipeline_function)
    return _add_execs_to_dict(invocation)


def _add_execs_to_dict(invocation, execs_dict={}):
    if isinstance(invocation,TaskInvocation):
        props=invocation.properties
        if props['package'] not in execs_dict.keys():
            execs_dict[props['package']]=[]
        if props['command'] not in execs_dict[props['package']]:
            execs_dict[props['package']].append(props['command'])
        return execs_dict
    elif isinstance(invocation,MethodInvocation):
        for task in invocation.tasks:
            _add_execs_to_dict(task, execs_dict)
        return execs_dict
    elif isinstance(invocation, ParallelSplit):
        return _add_execs_to_dict(invocation.body, execs_dict)


def _get_frames(task_name, invoke_name):
    '''
    Return the frames with function names between task_name and invoke_name 
    (task_name inclusive and invoke_name exclusive)
    '''
    stack=inspect.stack()
    frames=[]
    index=0
    while stack[index][3] != task_name:
        index=index+1
    while stack[index][3] != invoke_name:
        frames.append(stack[index])
        index=index+1
    return frames


def _get_name_from_taskprops(taskprops):
    if '.' in taskprops.command:
        return taskprops.command.split('.')[0]
    else:
        return taskprops.command


def _indent_(s):
    lines = s.splitlines()
    return "\n   ".join(lines)


class Source(object):
    """
    Each output argument provided by the task proxy functions, the inline functions, the nested dataflows 
    or the parallel split is wrapped into a Source object. These source objects are then either consumed by 
    subsequent tasks or provided as output of the pipeline. Source objects allow to trace on which path 
    a 'data element' (argument) is transferred from its origin to the consumer or the outside world.
    We distinguish between  
    * direct source objects: source objects created directly by by some task proxy functions (or by some 
    workflow helper tasks such as in a parallel split) 
    * indirect source objects: source objects that just pass along source objects created by inner task proxy 
    functions.
    Indirect source objects carry the reference to the inner source ('ref') which may be another indirect source 
    or the direct source. Direct source objects have not reference.
    Each source object has a parent which is the invocation in which the source is defined as output. 
    An exception is the input sources: Arguments that are input to the pipeline are also wrapped as source objects.
    These source objects have no parent assigned - or we could say, the source is outside of the workflow. 
    """
    def __init__(self, name, parent, ref, alias=None):
        """
        @param name: name of the port
        @param ref: reference source object the given source object is created from.
        @param parent: invocation for which the source objects is defined as output.
        """
        if ref and not parent:
            raise PipelineSpecificationError("A source object with a reference must have a parent.")
        self.name=name
        self.alias=alias
        self.parent=parent
        self.ref=ref
        if ref: # the data originates from another (inner) ref component
            self.path=list(ref.path)
            self.path.append(parent.name)
        elif parent: # the data is originates from the given parent component
            self.path=[name, parent.name]
        else: # TODO: Check whether this case can occur (START or FINAL tick?)
            self.path=[name]
            
    def __hash__(self):
        return hash(self.name)+hash(self.parent)+hash(self.ref)

    def __eq__(self, other):
        if not isinstance(other,Source):
            return False
        
        check=other.name==self.name and other.parent==self.parent
        if self.ref==None and other.ref==None:
            return check
        elif self.ref != None and other.ref != None:
            return check and self.ref==other.ref
        else:
            return False
        
    def __neq__(self, other):
        return not self.__eq__(other)




class Invocation(object):
    """
    Base class for all invocations defined as basic element of the workflow language.
    Each invocation has a name, consumes inputs and produces outputs. 
    The inputs are added to the Invocation as a map - with the argnames as key and
    the source objects as values. The outputs are added as a list - with just the source
    objects as elements.
    """
    def __init__(self, name):
        self.name=name
        self.inputs={}
        self.outputs=[]
        self.properties={}
                
    def add_inputs(self, inputnames, **kwargs):
        for name,p in kwargs.iteritems():
            if name in inputnames:
                self.add_input(name, p)

    def add_input(self, name, p):
        """
        An input is either passed as input to the dataflow or originates as output of another task.
        """
        if isinstance(p, tuple):
            if len(p)>1:
                raise PipelineSpecificationError("A tuple (of length >1) is set for the input argument %s in the function named %s."%(name, self.name))
            p=p[0]
        
        if isinstance(p, Source):
            self.inputs[name]=p
        else:
            self.inputs[name]=Source(name, None, None) # for inputs to the dataflow (--> START_TICK)

    def add_output(self, p):
        if not isinstance(p, Source):
            raise PipelineFrameworkError("Output (%s) to be appended to %s should be an instance of Source."%(str(p),self.name))
        self.outputs.append(p)
        
    def rename(self, newname):
        self.name=newname
        for output in self.outputs:
            newpath=output.path[:-1]
            newpath.append(newname)
            output.path=newpath

    def __hash__(self):
        return 1
    
    def __eq__(self, other):
        inports1=set(self.inputs.keys())
        inports2=set(other.inputs.keys())
        outports1=set([source.name for source in self.outputs])
        outports2=set([source.name for source in other.outputs])
        return self.name==other.name and inports1==inports2 and outports1==outports2 

    def __neq__(self, other):
        return not self.__eq__(other)


class TaskInvocation(Invocation):
    """
    A task invocation represents an atomic pipeline step. In addition to the Invocation
    it contains the information about the executable. This information is passed to the 
    constructor in form of a TaskProperties object. The elements in the TaskProperties are 
    added to the properties dict.
    """
    def __init__(self, taskprops, inputnames, outputnames, **kwargs):
        # validate specific inputs - nothing to set here
        self._handle_specific_inputs(taskprops)
        
        # resolve name and invoke super constructor
        name=self._load_name(taskprops, **kwargs)
        super(TaskInvocation,self).__init__(name=name)

        # add specific properties to the generic properties variable
        self._add_properties()

        # add inputs and outputs
        self.add_inputs(inputnames, **kwargs)
        # add outputs - no reference source since source directly created by this component
        for name in outputnames:
            self.add_output(Source(name, self, None))

    def _handle_specific_inputs(self, taskprops):
        if not taskprops:
            raise PipelineFrameworkError("A TaskProperties object needs to be specified \
                                when instantiating a TaskInvocation - task %s. These are \
                                typically passed from within the task proxy functions."%self.name)
        if not taskprops.command:
            raise ConfigurationError("A task should be instantiated by passing command.")
        self.taskprops=taskprops

    def _load_name(self, taskprops, **kwargs):
        if 'name' in kwargs.keys():
            name=kwargs['name']
            del kwargs['name']
        else:
            name=_get_name_from_taskprops(taskprops)
        return name


    def _add_properties(self):
        for k, v in self.taskprops.__dict__.iteritems():
            self.properties[k]=v

    def __hash__(self):
        return 1

    def __eq__(self, other):
        if super(TaskInvocation,self).__eq__(other):
            return self.properties==other.properties
        return False

    def __neq__(self, other):
        return not self.__eq__(other)
    
    def __repr__(self):
        return "\nTask: %s"%self.name


class MethodInvocation(Invocation):
    """
    A MethodInvocation represents the main building block to define nested workflows.
    It is defined by a function which represents the logic of the nested part of the workflow.
    MethodInvocations have a list of tasks that can again contain MethodInvocations, etc. 
    """
    
    def __init__(self, body_method, **kwargs):
        if not body_method:
            raise RuntimeError("No body function has been specified when instantiating a MethodInvocation.")

        self.ispipeline=body_method.ispipeline if hasattr(body_method,'ispipeline') else False

        # list of tasks invoked in the body of the method
        self.tasks=[]
        
        # validate specific inputs and set the specific member variables
        self._handle_specific_inputs(body_method)

        # resolve name and invoke super constructor
        name=self._load_name(body_method, **kwargs)
        super(MethodInvocation,self).__init__(name=name)

        # add specific properties to the generic properties variable
        self._add_properties()
        
        # handle the body to resolve the tasks invoked in the body and to identify the output variables 
        body_output=self._handle_body(**kwargs)

        # add inputs and outputs
        self.add_inputs(kwargs.keys(), **kwargs)
        for source in self._create_output_sources(body_output):
            self.add_output(source)


    def _handle_specific_inputs(self, body_method):
        # check specific arguments - body method
        self.body_method=body_method
        self.isroot=hasattr(body_method, "isroot") and body_method.isroot
     

    def _load_name(self, body_method, **kwargs):
        if 'name' in kwargs.keys():
            name=kwargs['name']
            del kwargs['name']
        else:
            name=body_method.__name__
        return name
   

    def _add_properties(self):
        # set specific arguments and set properties
        if hasattr(self.body_method, '_properties') and self.body_method._properties:
            self.properties={k:v for k,v in self.body_method._properties.iteritems()}
        

    def _handle_body(self, **kwargs):
        previous=thread_local.get_current()        
        thread_local.init_list(self)
        try:
            body_output=self.body_method(**kwargs)
            self.tasks=thread_local.get_list(self)
            self._assure_unique_names()
            thread_local.set_current(previous)
            body_output=flatten_tuple(body_output)
            return body_output
        except PipelineSpecificationError as e1:
            raise e1
        except Exception:
            raise PipelineSpecificationError("Error occurred while parsing the function %s.\
                            Stacktrace: \n%s"%(self.name, '\n'.join(traceback.format_stack())))
    
    def _assure_unique_names(self):
        names={}
        for task in self.tasks:
            if task.name not in names.keys():
                names[task.name]=[]
            names[task.name].append(task)
        for _name, tasks in names.iteritems():
            if len(tasks)>1:
                index=1
                for task in tasks:
                    newname=_name+"_"+str(index)
                    task.rename(newname)
                    index=index+1


    def _create_output_sources(self, body_output):
        sources=[]
        outputnames=tuple([elm.name for elm in body_output])        
        if hasattr(self.body_method, '_outputs') and self.body_method._outputs:
            aliases=self.body_method._outputs
            if len(aliases) != len(body_output):
                raise PipelineSpecificationError("Number of output names specified in the nested decorator "+
                        "should correspond to the number of outputs returned by the method.")
            for i in range(len(body_output)):
                sources.append(Source(outputnames[i], self, body_output[i], aliases[i]))
        else:
            for i in range(len(body_output)):
                sources.append(Source(outputnames[i], self, body_output[i]))
        return tuple(sources)
    

    def __hash__(self):
        return  1
    
    def __eq__(self, other):
        if super(MethodInvocation,self).__eq__(other):
            if len(self.tasks)==len(other.tasks):
                for task in self.tasks:
                    othertasks=[t for t in other.tasks if t==task]
                    if len(othertasks)!=1:
                        return False
                return True
        return False

    def __neq__(self, other):
        return not self.__eq__(other)

        
    def __repr__(self):
        s = "\nFunction: %s"%self.name
        for task in self.tasks:
            s = s + _indent_(task.__repr__())
        return s


class ParallelSplit(Invocation):
    """
    The ParallelSplit is the building block foreseen to split the processing over a 
    dedicated input variable ('iterable') into sub-processes that can run in parallel
    and are defined by the logic defined in the body of the function. Note that the 
    other inputs are passed into each of the sub-processes.
    The result will be a list of tuples - each tuple resulting as output body function
    applied in the splits (with one of the input elements in the 'iterable').
    This class here just keeps track of the structural information. No split-up logic is 
    defined here nor referenced.
    """
    def __init__(self, body_method, iterable, outputlistname, **kwargs):
        # validate specific specific inputs and set the specific member variables (body_method, iterable)
        self._handle_specific_inputs(body_method, iterable, **kwargs)        

        # resolve name and invoke super constructor
        name=self._load_name(body_method, **kwargs)
        super(ParallelSplit,self).__init__(name=name)

        # add specific properties to the generic properties variable
        self._add_properties()
        
        # handle the body to resolve the tasks invoked in the body and to identify the output variables 
        kwargs_inner={key:None for key in kwargs.keys()}
        self.body=MethodInvocation(body_method, **kwargs_inner)

        # add inputs
        self.add_inputs(kwargs.keys(), **kwargs)
        # add outputs - only a single output with the list of output tuples
        # no reference source since list created as part of the parallel split
        self.add_output(Source(outputlistname, self, None))
                

    def _handle_specific_inputs(self, body_method, iterable, **kwargs):
        if not iterable:
            raise PipelineSpecificationError("Specify the name of the argument that allows to be iterated.")
        if iterable not in kwargs.keys():
            raise PipelineSpecificationError("Input data set as inputname_to_split %s not found in the input key/value pairs."%iterable)
        self.body_method=body_method
        self.iterable=iterable

    
    def _load_name(self, body_method, **kwargs):
        if 'name' in kwargs.keys():
            name=kwargs['name']
            del kwargs['name']
        else:
            name=body_method.__name__
        return name

    
    def _add_properties(self):    
        if hasattr(self.body_method, '_properties') and self.body_method._properties:
            self.properties={k:v for k,v in self.body_method._properties.iteritems()}

    def __hash__(self):
        return 1

    def __eq__(self, other):
        if super(ParallelSplit,self).__eq__(other):
            return self.iterable==other.iterable and \
                    self.body==other.body
        return False

    def __neq__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        s = "\nParallel Split: %s"%self.name
        s=s+_indent_(self.body.__repr__())
        return s
