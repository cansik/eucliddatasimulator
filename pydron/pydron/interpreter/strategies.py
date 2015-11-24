# Copyright (C) 2015 Stefan C. Mueller



import logging
import anycall
logger = logging.getLogger(__name__)
    
class SchedulingStrategy(object):

    def __init__(self):
        pass
    
    def execution_started(self, g):
        pass
    
    def execution_finished(self, g):
        pass
    
    def assign_jobs_to_workers(self, workers, jobs):
        """
        Decide where to run jobs.
        
        :param workers: Idle workers
        
        :param jobs: Jobs ready for execution.
        
        :returns: List of `(worker, job, parallel)` tuples. 
          If `parallel` is `True` this job may run in parallel
          with other jobs on the same worker. In that case
          the `worker` can be any worker not just an idle one
          from `workers`. 
        """
        pass
    
    def choose_source_worker(self, valueref, dest):
        """
        Decide from where `dest` should fetch
        the given value.
        
        :param valueref: Value `dest` needs.
        
        :param dest: Worker to which we need to transfer the value.
        
        :returns: source worker.
        """
        pass
    
class MinimalStrategy(SchedulingStrategy):
    """
    Implements the scheduling rules which are required for
    correct execution. Anything beyond that is forwarded to
    another strategy. 
    Wrapping your strategy with this strategy should ensure
    that the execution will work correctly.
    """
        
    def __init__(self, strategy, master_worker=None):
        """
        :param master_worker: Worker that runs tasks with
          side effects. Defaults to the local worker.
        :param strategy: Strategy to forward non-trivial
          decisions.
        """
        self.master_worker = master_worker
        self.strategy = strategy
        
    def choose_source_worker(self, valueref, dest):
        workers = list(valueref.get_workers())
        if len(workers) == 0:
            raise KeyError("%r is not stored on any workers." % valueref.valueid)
        
        if len(workers) == 1:
            # If there is a source we have to return it.
            return workers[0]
        
        # There are at least two workers to pick from.
        worker = self.strategy.choose_source_worker(valueref, dest)
        return worker
    
    def assign_jobs_to_workers(self, workers, jobs):
        
        assignments = []
        
        # jobs we don't want to delegate to the strategy
        jobs_to_remove = []
        
        for job in jobs:
            props = job.g.get_task_properties(job.tick)
            
            masteronly = props.get("masteronly", False)
            syncpoint = props.get("syncpoint", False)
            quick = props.get("quick", False)
            
            syncpoint |= masteronly
            
            # Excecution must happen on `only_worker`.
            only_worker = None
            
            if syncpoint:
                if self.master_worker is None:
                    self.master_worker = anycall.RPCSystem.default.local_remoteworker #@UndefinedVariable
                only_worker = self.master_worker
                
            for port, valueref in job.inputs.iteritems():
                if not valueref.pickle_support:
                    source = next(iter(valueref.get_workers()))
                    if only_worker is not None and only_worker != source:
                        raise ValueError("Job %r has no-send input port %r from %r but needs to run on %r." %
                                         (job, port, source, only_worker))
                    only_worker = source
                
            if only_worker is not None:
                if quick or only_worker in workers or syncpoint:
                    if only_worker in workers:
                        parallel = quick
                        workers.remove(only_worker) # worker removed here, job is removed later.
                    else:
                        parallel = True
                    assignments.append((only_worker, job, parallel))
                    
                jobs_to_remove.append(job)
            
        for job in jobs_to_remove:
            jobs.remove(job)
        
        more_assignments = list(self.strategy.assign_jobs_to_workers(workers, jobs))
        return assignments + more_assignments
        

class TrivialSchedulingStrategy(SchedulingStrategy):
    
    def __init__(self, pool):
        self._pool = pool
        
    def assign_jobs_to_workers(self, workers, jobs):
        while workers and jobs:
            job = jobs.pop()
            worker = workers.pop()
            
            props = job.g.get_task_properties(job.tick)
            quick = props.get("quick", False)
            
            yield worker, job, quick
        
    def choose_source_worker(self, valueref, dest):
        workers = valueref.get_workers()
        return next(iter(workers))
