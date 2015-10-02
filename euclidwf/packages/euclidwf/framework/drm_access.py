'''
Created on Apr 27, 2015

@author: martin.melchior
'''
from euclidwf.server.server_model import JOB_PENDING, JOB_QUEUED, JOB_EXECUTING,\
    JOB_HELD, JOB_ABORTED, JOB_SUSPENDED, JOB_UNKNOWN, JOB_ERROR, JOB_COMPLETED
from euclidwf.utilities.error_handling import ConfigurationError


def create_configure_cmd(cmdname, config):
    pass


# SUBMIT
#---------------------------------
DRM_TASK="--task=%s"
DRM_WORKDIR="--workdir=%s"
DRM_LOGDIR="--logdir=%s"
DRM_INPUTS="--inputs=%s"
DRM_OUTPUTS="--outputs=%s"
DRM_JOBID="--job_id=%s"
def create_submit_command(cmdname, taskname, inputs, outputs, workdir, logdir, resources=None):
    cmdArray = cmdname.split()
    cmdArray.append(DRM_TASK%taskname)
    cmdArray.append(DRM_WORKDIR%workdir)
    cmdArray.append(DRM_INPUTS%str(inputs))
    cmdArray.append(DRM_OUTPUTS%str(outputs))
    cmdArray.append(DRM_LOGDIR%logdir)       
    return cmdArray

STDOUT='stdout'
JOBID='job_id'
STDERR='stderr'
EXITCODE='exitcode'
class SubmitResponse():
    def __init__(self, dictionary):
        self.stdout=dictionary[STDOUT] if STDOUT in dictionary else None
        self.jobid=dictionary[JOBID] if JOBID in dictionary else None
        self.stderr=dictionary[STDERR] if STDERR in dictionary else None
        self.exitcode=dictionary[EXITCODE] if EXITCODE in dictionary else None

def read_submit_response(data):
    return SubmitResponse(read_stdout_dict(data))


# CHECK STATUS
#----------------------------------------------

def create_checkstatus_command(cmdname, jobids):
    if len(jobids) != 1:
        raise ConfigurationError("Check status command called with %i jobids. Only one single is accepted.")
    jobid=jobids[0]
    cmdArray = cmdname.split()    
    cmdArray.append(DRM_JOBID%jobid)
    return cmdArray

STATUS='status'
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
    return [CheckStatusResponse(read_stdout_dict(data))]

def read_stdout_dict(data):
    '''
    :param data: an array containing as first element the stdout and as second element the stderr 
    received on calling a drm method (configure, submit, checkstatus,...)
    '''
    stdout_dict={}
    stdout=data[0]
    for elm in stdout.splitlines():
        if "=" in elm:
            elm_key,elm_val=elm.split("=")
            stdout_dict[elm_key]=elm_val
    return stdout_dict
    

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
