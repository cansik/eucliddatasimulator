'''
Created on Apr 27, 2015

@author: martin.melchior
'''
import json
from euclidwf.server.server_model import JOB_PENDING, JOB_QUEUED, JOB_EXECUTING,\
    JOB_HELD, JOB_ABORTED, JOB_SUSPENDED, JOB_UNKNOWN, JOB_ERROR, JOB_COMPLETED


# CONFIGURE
#----------------------------------------------

def create_configure_cmd(cmdname, config):
    '''
    :param cmdname: name of the configure command implemented by the DRM. 
    :param config: dictionary with the config items for the DRM.
        Supported config items:
        * work_dir_host: absolute path of the root of the workspace at submission host level
        * work_dir_node: absolute path of the root of the workspace at compute node level
        * drm: LOCAL | SGE | PBS
        * log_level: WARN | INFO | DEBUG
    '''
    cmdArray = cmdname.split()
    cmdArray.append("-j")
    cmdArray.append(json.dumps(config))
    return cmdArray

STDOUT='stdout'
STATUS='status'
STDERR='stderr'
EXITCODE='exitcode'
class ConfigResponse():
    def __init__(self, dictionary):
        self.stdout=dictionary[STDOUT] if STDOUT in dictionary else None
        self.status=dictionary[STATUS] if STATUS in dictionary else None
        self.stderr=dictionary[STDERR] if STDERR in dictionary else None
        self.exitcode=dictionary[EXITCODE] if EXITCODE in dictionary else None

def read_configure_response(data):
    stdout=data[0].replace("'","\"")
    stdoutdict=json.loads(stdout)
    return ConfigResponse(stdoutdict)


# SUBMIT
#---------------------------------

def create_submit_command(cmdname, taskname, inputs, outputs, workdir, logdir, resources):
    '''
    :param cmdname: name of the submit command implemented by the DRM. 
    :param taskname: name of the executable to call for execution.
    :param workdir: path to the workdir - path relative to the workspace configured for the DRM. 
    See configure command.
    :param logdir: path to the logdir - path relative to the workdir.
    :param inputs: dictionary containing for each input product the portname as key and the path relative to the workdir as value.
    :param outputs: dictionary containing for each output product the portname as key and the path relative to the workdir as value.
    :param resources: computing resources to be required by the job to be submitted. 
    '''
    jsondict={}
    jsondict['task']=taskname
    jsondict['workdir']=workdir
    jsondict['logdir']=logdir
    jsondict['inputs']=inputs
    jsondict['outputs']=outputs
    if resources:
        jsondict['cores']=str(resources.cores)
        jsondict['ram']=resources.ram
        jsondict['walltime']=walltime_tostring(resources.walltime)
    cmdArray = cmdname.split()
    cmdArray.append("-j")
    cmdArray.append(json.dumps(jsondict))
    return cmdArray

JOBID='job_id'
class SubmitResponse():
    def __init__(self, dictionary):
        self.stdout=dictionary[STDOUT] if STDOUT in dictionary else None
        self.jobid=dictionary[JOBID] if JOBID in dictionary else None
        self.stderr=dictionary[STDERR] if STDERR in dictionary else None
        self.exitcode=dictionary[EXITCODE] if EXITCODE in dictionary else None

def read_submit_response(data):
    stdout=data[0].replace("'","\"")
    stdoutdict=json.loads(stdout)
    return SubmitResponse(stdoutdict)

# CHECK STATUS
#----------------------------------------------

def create_checkstatus_command(cmdname, jobids):
    '''
    :param cmdname: name of the check status command implemented by the DRM. 
    :param jobids: array of ids of the jobs for which to check status.  
    '''
    cmdArray = cmdname.split()
    cmdArray.append("-j")
    cmdArray.append(json.dumps(jobids))
    return cmdArray

JOB_EXITCODE='job_exitcode'
class CheckStatusResponse():
    def __init__(self, dictionary):
        self.stdout=dictionary[STDOUT] if STDOUT in dictionary else None
        self.jobid=dictionary[JOBID] if JOBID in dictionary else None
        self.stderr=dictionary[STDERR] if STDERR in dictionary else None
        self.exitcode=dictionary[EXITCODE] if EXITCODE in dictionary else None
        self.status=dictionary[STATUS] if STATUS in dictionary else None
        self.job_exitcode=dictionary[JOB_EXITCODE] if JOB_EXITCODE in dictionary else None

def read_checkstatus_response(data):
    stdout=data[0].replace("'","\"")
    stdoutlist=json.loads(stdout)
    response=[]
    for jobstatus in stdoutlist:
        response.append(CheckStatusResponse(jobstatus))
    return response


# CLEANUP
#----------------------------------------------

def create_cleanup_cmd(cmdname, workdirs):
    '''
    :param cmdname: name of the cleanup command implemented by the DRM. 
    :param workdirs: array of paths pointing to the workdirs (relative within workspace) that should be cleaned up (deleted).  
    '''
    cmdArray = cmdname.split()
    cmdArray.append("-j")
    cmdArray.append(json.dumps(workdirs))
    return cmdArray

def read_cleanup_response(response):
    pass
    

# DELETE
#----------------------------------------------

def create_delete_cmd(cmdname, jobids):
    '''
    :param cmdname: name of the delete command implemented by the DRM. 
    :param jobids: array of ids of the jobs to be deleted.  
    '''
    cmdArray = cmdname.split()
    cmdArray.append("-j")
    cmdArray.append(json.dumps(jobids))
    return cmdArray

def read_delete_response(response):
    pass


def walltime_tostring(wt):
    m, s = divmod(wt*3600, 60)
    h, m = divmod(m, 60)
    return "%d:%02d:%02d" % (h, m, s)

def ram_tostring(ram):
    return "{0:.1f}g".format(ram)


waiting_state=(JOB_PENDING, JOB_QUEUED, JOB_EXECUTING, JOB_HELD)
error_state=(JOB_ERROR, JOB_UNKNOWN, JOB_SUSPENDED, JOB_ABORTED)
success_state=(JOB_COMPLETED,)

#EXIT CODES
E_SUCCESS = 0
E_INPUTERROR = 1
E_INVALIDCONFIG = 2
E_IOERROR = 3
E_GENERICERROR = -1

def wait_for_job(state):
    return state in waiting_state

def job_completed(state):
    return state in success_state

def job_failed(state):
    return state in error_state
