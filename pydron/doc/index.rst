
Pydron - Semi-automatic parallelization
=============================================================

Pydron takes sequential python code and runs it in parallel
on multiple cores, clusters, or cloud infrastructures.

----------------------
Let's get going!
----------------------

.. code-block:: python

	import pydron
   
	@pydron.schedule
	def calibration_pipeline(inputs):
  		outputs = []
  		for input in inputs: 
  			output = process(input)
   			outputs = outputs + [output]
   		return outputs
   	
   	@pydron.functional
   	def process(input):
   		...
   	
This will run all `process` calls in parallel.



.. _api:

----------------------
API
----------------------

.. py:module:: pydron

Pydron's API consists of only two decorators:

.. py:decorator:: pydron.schedule

	Pydron will only parallelize code with this decorator.
	All other code will run as usual.
	
	The function may contain arbitrary code with the
	exception of `try except`, `try finally`, and `with` 
	statements that are not yet implemented.
	
	While arbitrary code is allowed and should
	(in theory at least, there are bugs) produce correct
	results, not every code can be made to run in
	parallel. See  :ref:`best-pratices` for guide lines.
	
.. py:decorator:: pydron.functional

	Pydron can only run function calls in parallel if the
	functions have this decorator. 
	
	The decorator defines a contract between the developer
	and Pydron. The developer may decorate a function
	with `functional` if the following conditions are met:
	
	* The function does not assign global variables or as
	  any other kind of side effects.
	
	* If the function reads global variables they must have
	  been unchanged since the module was loaded. This is
	  typically the case for classes and functions defined
	  in a module. What is excluded is access to dynamic
	  state stored globally.
	  
	* All arguments passed to the function must be serializable
	  with `pickle`.
	  
	* The return value or the thrown exception must be serializable
	  with `pickle`.
	  
	* The function does not modify the objects that are passed
	  as arguments. Often you can return modified copies instead.
	  
	

.. _best-pratices:

----------------------
Best practices
----------------------

Pydron can run two function calls in parallel if the following conditions
are met:

 * The return value of one is not an argument of the other, directly or
   indirectly.
   
 * Both have the :func:`pydron.functional` decorator.
 
 * The code which, in a sequential execution, would be executed between the
   two, must be free of `sync-points` (see below).
   
A `sync-point` is an operation that Pydron cannot reason about. It therefore
executes that operation at the same 'time' as it would in a sequential execution.
That is, every operation that comes before must have finished and
all operations that come afterwards have to wait. 

A single `sync-point` inside
a loop forces the iterations to run one after the other, making
parallelism impossible. Therefore `sync-point` should be avoided.

The following operations cause a `sync-point`:

 * Calls to functions without the :func:`pydron.functional` decorator.
   Currently, this includes pretty much all built-in functions
   and functions from libraries since I haven't populated the
   white-lists yet.
 
 * Operations that modify an object. These include:
 
   * Assigning an attribute: `obj.x = ...`
   
   * Assigning a subscript: `obj[i] = ...`
   
   * Augmented Assignment: `obj += ...`
   
   The last one might be a bit surprising since `x += 1`
   is often to be identical to `x = x + 1`. But this might not be
   the case. Some types, including `list` and `numpy.ndarray`, 
   perform the operation in-place, modifiying the object instead
   of creating a new one.




