# Copyright (C) 2015 Stefan C. Mueller

import json
import os.path
from remoot import pythonstarter, smartstarter
import anycall
from pydron.backend import worker
from pydron.interpreter import scheduler, strategies
from twisted.internet import defer

def load_config(configfile=None):
    
    if not configfile:
        candidates = []
        if "PYDRON_CONF" in os.environ:
            candidates.append(os.environ["PYDRON_CONF"])
        candidates.append(os.path.abspath("pydron.conf"))
        candidates.append(os.path.expanduser("~/pydron.conf"))
        candidates.append("/etc/pydron.conf")
        for candidate in candidates:
            if os.path.exists(candidate):
                configfile = candidate
                break
        else:
            raise ValueError("Config file could not be found. Looked for %s" % repr(candidates))
        
    with open(configfile, 'r') as f:
        cfg = json.load(f)
        
    def convert(obj):
        if isinstance(obj, dict):
            return {k:convert(v) for k,v in obj.iteritems()}
        elif isinstance(obj, list):
            return [convert(v) for v in obj]
        elif isinstance(obj, unicode):
            return str(obj)
        else:
            return obj
        
    cfg = convert(cfg)
    return cfg

def create_scheduler(config, pool):
    if "scheduler" not in config:
        strategy_name = "trivial"
    else:
        strategy_name = config["scheduler"]
    
    if strategy_name == "trivial":
        strategy = strategies.TrivialSchedulingStrategy(pool)
        strategy = strategies.MinimalStrategy(strategy)
    else:
        raise ValueError("Unsupported scheduler: %s" % strategy_name)
    
    return scheduler.Scheduler(pool, strategy)
    

def create_pool(config, rpcsystem, error_handler):
    """
    starts workers and returns a pool of them.
    
    Returns two callbacks:
    
    * The first callbacks with the pool as 
    soon as there is one worker. Errbacks if all starters
    failed to create a worker.
    
    * The second calls back once all workers have been
    started. This one can be cancelled.
    
    The given `error_handler` is invoked for every failed start.
    """
    
    starters = []
    
    for starter_conf in config["workers"]:
        starter_type = starter_conf["type"]

        if starter_type == "multicore":
            starters.extend(_multicore_starter(starter_conf, rpcsystem))
        elif starter_type == "ssh":
            starters.extend(_ssh_starter(starter_conf, rpcsystem))
        else:
            raise ValueError("Not supported worker type %s" % repr(starter_type))
        
    pool = worker.Pool()
    
    ds = []
    
    for i, starter in enumerate(starters):
        d = starter.start()
        
        def success(worker, i, starter):
            worker.nicename = "#%s" % i
            pool.add_worker(worker)
        def fail(failure):
            error_handler(failure)
            return failure

        d.addCallback(success, i, starter)
        ds.append(d)
        
    d = defer.DeferredList(ds, fireOnOneErrback=True, consumeErrors=True)
    
    def on_success(result):
        return pool
    def on_fail(firsterror):
        return firsterror.value.subFailure
    d.addCallbacks(on_success, on_fail)
    return d


def _multicore_starter(conf, rpcsystem):
    starters = []
    for _ in range(conf["cores"]):
        starter = pythonstarter.LocalStarter()
        smart = smartstarter.SmartStarter(starter, rpcsystem, anycall.create_tcp_rpc_system, [])
        starters.append(worker.WorkerStarter(smart))
    return starters
    

def _ssh_starter(conf, rpcsystem):
    starters = []
    for _ in range(getattr(conf, "cores", 1)):
        starter = pythonstarter.SSHStarter(conf["hostname"], 
                                           username=conf["username"], 
                                           password=conf.get("password", None), 
                                           private_key_files=conf.get("private_key_files", []),
                                           private_keys=conf.get("private_keys", []), 
                                           tmp_dir=conf.get("tmp_dir", "/tmp"))
        smart = smartstarter.SmartStarter(starter, rpcsystem, anycall.create_tcp_rpc_system, [])
        starters.append(worker.WorkerStarter(smart))
    return starters
