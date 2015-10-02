'''
Created on Jun 2, 2015

@author: martin.melchior, cyril.halmo
'''
from euclidwf.framework.configuration import LOCALCACHE
from euclidwf.utilities.error_handling import RunConfigurationError, ConfigurationError
from flask.json import JSONEncoder


def convert(obj):
    if isinstance(obj, dict):
        return {k:convert(v) for k,v in obj.iteritems()}
    elif isinstance(obj, list):
        return [convert(v) for v in obj]
    elif isinstance(obj, unicode):
        return str(obj)
    else:
        return obj
        


class PipelineRunRegistry():        
    
    def __init__(self):
        self.registry = {}

    def has_run(self, runid):
        runid=self.as_string(runid)
        return runid in self.registry.keys()
    
    def add_run(self, runid, pipelinerun):
        runid=self.as_string(runid)
        if runid not in self.registry.keys():
            self.registry[runid]=pipelinerun
        else:
            raise RuntimeError("Runid %s already exists."%runid)

    def get_all_runids(self):
        return [runid for runid in self.registry.keys()]
    
    def get_run(self, runid):
        runid=self.as_string(runid)
        if runid in self.registry.keys():
            return self.registry[runid]
        else:
            raise RuntimeError("Runid %s does not exist in the registry."%runid)

    def get_run_status(self, runid):
        runid=self.as_string(runid)
        if runid in self.registry.keys():
            return self.registry[runid].status
        else:
            return None

    def delete_run(self, runid): 
        runid=self.as_string(runid)
        if runid in self.registry.keys():
            del self.registry[runid]
        if runid in self.registry.keys():
            raise RuntimeError("Runid %s could not be removed from registry."%runid)

    def as_string(self, s):
        if isinstance(s, str):
            return s
        else:
            return str(s)

class PipelineRunServerHistory():
    
    def __init__(self):
        self.history = []

    def add_entry(self, msg, datetime):
        self.history.append((msg,datetime))


registry = PipelineRunRegistry()
history=PipelineRunServerHistory()
config=None


class PRSJsonEncoder(JSONEncoder):
    
    def default(self, o):
        default = getattr(o, "default", None)
        if callable(default):
            return o.default()
        return super(PRSJsonEncoder, self).default(o)


# These should map to the enum values in ConfigurationResponse.ConfigurationStatus in java.
CONFIG_OK="ok" 
CONFIG_NOT_ACCEPTED="not_accepted"
CONFIG_ERROR="error"
CONFIG_DRM_ERROR="drm_error"

# These should map to the fields in the ConfigurationResponse class in java.       
class ConfigurationResponse():
     
    def __init__(self, status, message):
        self.status=status
        self.message=message
        
    def __repr__(self):
        s="Configuration Response: \n"
        s+="    status: %s\n"
        s+="    msg: %s"
        s=s%(self.status, '\n'.join(self.message.splitlines()))
        return s

# These should map to the fields in the DrmConfiguration class in java.       
DRM_STATUSCHECK_POLLTIME = 'statusCheckPollTime'
DRM_STATUSCHECK_TIMEOUT = 'statusCheckTimeout'
DRM_PROTOCOL = 'protocol'
DRM_HOST = 'host'
DRM_PORT = 'port'
DRM_CONFIGURE_CMD = 'configureCmd'
DRM_SUBMIT_CMD = 'submitCmd'
DRM_CHECKSTATUS_CMD = 'checkStatusCmd'
DRM_CLEANUP_CMD = 'cleanupCmd'
DRM_DELETE_CMD = 'deleteCmd'
IALDRM_CONFIGURE_CMD = 'ialdrm_config'
IALDRM_SUBMIT_CMD = 'ialdrm_submit_job'
IALDRM_CHECKSTATUS_CMD = 'ialdrm_check_job_status'
IALDRM_CLEANUP_CMD = 'ialdrm_workdir_cleanup'
IALDRM_DELETE_CMD = 'ialdrm_delete_job'
class DrmConfiguration():
     
    def __init__(self, data):
        if DRM_STATUSCHECK_POLLTIME not in data:
            raise ConfigurationError("DrmConfig(" + DRM_STATUSCHECK_POLLTIME + ") not set.")
        self.statusCheckPollTime = data[DRM_STATUSCHECK_POLLTIME]
        if DRM_STATUSCHECK_TIMEOUT not in data:
            raise ConfigurationError("DrmConfig(" + DRM_STATUSCHECK_TIMEOUT + ") not set.")
        self.statusCheckTimeout = data[DRM_STATUSCHECK_TIMEOUT]
        if DRM_PROTOCOL not in data:
            raise ConfigurationError("DrmConfig(" + DRM_PROTOCOL + ") not set.")
        self.protocol = data[DRM_PROTOCOL]
        if DRM_HOST not in data:
            raise ConfigurationError("DrmConfig(" + DRM_HOST + ") not set.")
        self.host = data[DRM_HOST]
        if DRM_PORT not in data:
            raise ConfigurationError("DrmConfig(" + DRM_PORT + ") not set.")
        self.port = data[DRM_PORT]
        if DRM_CONFIGURE_CMD not in data:
            self.configureCmd = IALDRM_CONFIGURE_CMD
        else:
            self.configureCmd = data[DRM_CONFIGURE_CMD]
        if DRM_SUBMIT_CMD not in data:
            self.submitCmd = IALDRM_SUBMIT_CMD
        else:
            self.submitCmd = data[DRM_SUBMIT_CMD]
        if DRM_CHECKSTATUS_CMD not in data:
            self.checkStatusCmd = IALDRM_CHECKSTATUS_CMD
        else:
            self.checkStatusCmd = data[DRM_CHECKSTATUS_CMD]
        if DRM_CLEANUP_CMD not in data:
            self.cleanupCmd = IALDRM_CLEANUP_CMD
        else:
            self.cleanupCmd = data[DRM_CLEANUP_CMD]
        if DRM_DELETE_CMD not in data:
            self.deleteCmd = IALDRM_DELETE_CMD
        else:
            self.deleteCmd = data[DRM_DELETE_CMD]
            
    def __eq__(self, other):
        if other == None:
            return False
        return self.statusCheckPollTime == other.statusCheckPollTime \
            and self.statusCheckTimeout == other.statusCheckTimeout \
            and self.protocol == other.protocol \
            and self.host == other.host \
            and self.port == other.port \
            and self.configureCmd == other.configureCmd \
            and self.submitCmd == other.submitCmd \
            and self.checkStatusCmd == other.checkStatusCmd \
            and self.cleanupCmd == other.cleanupCmd \
            and self.deleteCmd == other.deleteCmd
                    
    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        output="%s:%s\n"%(DRM_PROTOCOL,self.protocol)
        output+="%s:%s\n"%(DRM_HOST,self.host)
        output+="%s:%s\n"%(DRM_PORT,self.port)
        output+="%s:%s\n"%(DRM_STATUSCHECK_POLLTIME,self.statusCheckPollTime)
        output+="%s:%s\n"%(DRM_STATUSCHECK_TIMEOUT,self.statusCheckTimeout)
        output+="%s:%s\n"%(DRM_CONFIGURE_CMD,self.configureCmd)
        output+="%s:%s\n"%(DRM_SUBMIT_CMD,self.submitCmd)
        output+="%s:%s\n"%(DRM_CHECKSTATUS_CMD,self.checkStatusCmd)
        output+="%s:%s\n"%(DRM_CLEANUP_CMD,self.cleanupCmd)
        output+="%s:%s\n"%(DRM_DELETE_CMD,self.deleteCmd)
        return output
         
# These should map to the fields in the WsConfiguration class in java.       
WS_PROTOCOL="protocol"
WS_HOST="host"
WS_PORT="port"
WS_ROOT="workspaceRoot"       
class WsConfiguration():
     
    def __init__(self, data):
        if WS_PROTOCOL not in data:
            raise ConfigurationError("WsConfig(" + WS_PROTOCOL + ") not set.")
        self.protocol = data[WS_PROTOCOL]
        if WS_HOST not in data:
            raise ConfigurationError("WsConfig(" + WS_HOST + ") not set.")
        self.host = data[WS_HOST]
        if WS_PORT not in data:
            raise ConfigurationError("WsConfig(" + WS_PORT + ") not set.")
        self.port = data[WS_PORT]
        if WS_ROOT not in data:
            raise ConfigurationError("WsConfig(" + WS_ROOT + ") not set.")
        self.workspaceRoot = data[WS_ROOT]
                
    def __eq__(self, other):
        if other == None:
            return False
        return self.protocol == other.protocol \
            and self.host == other.host \
            and self.port == other.port \
            and self.workspaceRoot == other.workspaceRoot
                        
    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        output="%s:%s\n"%(WS_PROTOCOL,self.protocol)
        output+="%s:%s\n"%(WS_HOST,self.host)
        output+="%s:%s\n"%(WS_PORT,self.port)
        output+="%s:%s\n"%(WS_ROOT,self.workspaceRoot)
        return output


CONFIG_LOCALCACHE = 'localcache'
CONFIG_PROXYFCTS_DIR = 'proxyFctsDir'
CONFIG_DRM = 'drmConfig'
CONFIG_WS = 'wsConfig'
class RunServerConfiguration():
     
    def __init__(self, data):
        if CONFIG_LOCALCACHE not in data:
            raise ConfigurationError("RunServerConfiguration(" + CONFIG_LOCALCACHE + ") not set.")
        self.localcache = data[LOCALCACHE]
        if CONFIG_DRM not in data:
            raise ConfigurationError("RunServerConfiguration(" + CONFIG_DRM + ") not set.")
        self.drmConfig = DrmConfiguration(data[CONFIG_DRM])
        if CONFIG_WS not in data:
            raise ConfigurationError("RunServerConfiguration(" + CONFIG_WS + ") not set.")
        self.wsConfig = WsConfiguration(data[CONFIG_WS])
        if CREDENTIALS not in data:
            raise RunConfigurationError("RunConfiguration(" + CREDENTIALS + ") not set.")
        self.credentials = HpcAccessCredentials(data[CREDENTIALS])
        
    def __eq__(self, other):
        if other == None:
            return False
        return self.localcache == other.localcache \
            and self.drmConfig == other.drmConfig \
            and self.wsConfig == other.wsConfig    
            
    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        output="%s:%s\n"%(CONFIG_LOCALCACHE,self.localcache)
        output+=CONFIG_DRM+":\n"
        output+=repr(self.drmConfig)
        output+=CONFIG_WS+":\n"
        output+=repr(self.wsConfig)
        return output

         
# These should map to the enum values in SubmissionResponse.SubmissionStatus in java.
SUBM_SCHEDULED="scheduled"
SUBM_EXECUTING="executing"
SUBM_FAILED="failed"
SUBM_NOT_ACCEPTED="not_accepted" 
SUBM_ALREADY_SUBMITTED="already_submitted"
SUBM_ERROR="error"

# These should map to the fields in the SubmissionResponse class in java.
class SubmissionResponse():
    
    def __init__(self, runid, status, message, stacktrace=None):
        self.runid = runid
        self.status = status
        self.message = message
        self.stacktrace=stacktrace

    def __repr__(self):
        s="Configuration Response: \n"
        s+="    runid: %s\n"
        s+="    status: %s\n"
        s+="    msg: %s"
        s=s%(self.runid, self.status, '\n'.join(self.message.splitlines()))
        return s

# These should map to the fields in the RunConfiguration class in java
RUNID = 'runid'
WORKDIR = 'workdir'
LOGDIR = 'logdir'
PIPELINE_SCRIPT = 'pipelineScript'
PIPELINE_DIR = 'pipelineDir'
PKG_REPOSITORY = 'pkgRepository'
INPUTDATA_PATHS = 'inputDataPaths' # A MAP OF <string,string>
CREDENTIALS = 'credentials' # see HPcAccessCredentials

class RunConfiguration():
     
    def __init__(self, data):
        if RUNID not in data:
            raise RunConfigurationError("RunConfiguration(" + RUNID + ") not set.")
        self.runid = str(data[RUNID])
        if WORKDIR not in data:
            raise RunConfigurationError("RunConfiguration(" + WORKDIR + ") not set.")
        self.workdir = str(data[WORKDIR])
        if LOGDIR not in data:
            raise RunConfigurationError("RunConfiguration(" + LOGDIR + ") not set.")
        self.logdir = str(data[LOGDIR])
        if PIPELINE_SCRIPT not in data:
            raise RunConfigurationError("RunConfiguration(" + PIPELINE_SCRIPT + ") not set.")
        self.pipelineScript = str(data[PIPELINE_SCRIPT])
        if PIPELINE_DIR not in data:
            raise RunConfigurationError("RunConfiguration(" + PIPELINE_DIR + ") not set.")
        self.pipelineDir = str(data[PIPELINE_DIR])
        if PKG_REPOSITORY not in data:
            raise RunConfigurationError("RunConfiguration(" + PKG_REPOSITORY + ") not set.")
        self.pkgRepository = str(data[PKG_REPOSITORY])
        if INPUTDATA_PATHS not in data:
            raise RunConfigurationError("RunConfiguration(" + INPUTDATA_PATHS + ") not set.")
        self.inputDataPaths = data[INPUTDATA_PATHS]
        if CREDENTIALS not in data:
            raise RunConfigurationError("RunConfiguration(" + CREDENTIALS + ") not set.")
        if isinstance(data[CREDENTIALS], HpcAccessCredentials):
            self.credentials=data[CREDENTIALS]
        else:
            self.credentials = HpcAccessCredentials(data[CREDENTIALS])
           
    def __eq__(self, other):
        if other == None:
            return False
        return self.runid == other.runid \
            and self.workdir == other.workdir \
            and self.logdir == other.logdir \
            and self.pipelineScript == other.pipelineScript \
            and self.pipelineDir == other.pipelineDir \
            and self.pkgRepository == other.pkgRepository \
            and self.inputDataPaths == other.inputDataPaths \
            and self.credentials == other.credentials
                   
    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        output="%s:%s\n"%(RUNID,self.runid)
        output+="%s:%s\n"%(WORKDIR,self.workdir)
        output+="%s:%s\n"%(LOGDIR,self.logdir)
        output+="%s:%s\n"%(PIPELINE_SCRIPT,self.pipelineScript)
        output+="%s:%s\n"%(PIPELINE_DIR,self.pipelineDir)
        output+="%s:%s\n"%(PKG_REPOSITORY,self.pkgRepository)
        output+=INPUTDATA_PATHS+":\n"
        for k,v in self.inputDataPaths.iteritems():
            output+="    %s:%s\n"%(k,v)
        output+=CREDENTIALS+":\n"
        output+=repr(self.credentials)
        return output
         
         
# These should map to the fields in the HpcAccessCredentials class in java
DRM_PASSWORD = 'drmPassword'
DRM_USERNAME = 'drmUsername'
WS_PASSWORD = 'wsPassword'
WS_USERNAME = 'wsUsername'
         
class HpcAccessCredentials():
     
    def __init__(self, credentials):
        if DRM_USERNAME not in credentials:
            raise TypeError("DrmConfig(" + DRM_USERNAME + ") not set.")
        self.drmUsername = str(credentials[DRM_USERNAME])
        if DRM_PASSWORD not in credentials:
            raise TypeError("DrmConfig(" + DRM_PASSWORD + ") not set.")
        self.drmPassword = str(credentials[DRM_PASSWORD])
        if WS_USERNAME not in credentials:
            raise TypeError("DrmConfig(" + WS_USERNAME + ") not set.")
        self.wsUsername = str(credentials[WS_USERNAME])
        if WS_PASSWORD not in credentials:
            raise TypeError("DrmConfig(" + WS_PASSWORD + ") not set.")
        self.wsPassword = str(credentials[WS_PASSWORD])
            
    def __eq__(self, other):
        if other == None:
            return False
        return self.drmUsername == other.drmUsername \
            and self.drmPassword == other.drmPassword \
            and self.wsUsername == other.wsUsername \
            and self.wsPassword == other.wsPassword
    
    def __ne__(self, other):
        return not self.__eq__(other)
    
    def __repr__(self):
        output="%s:%s\n"%(DRM_USERNAME,self.drmUsername)
        output+="%s:%s\n"%(DRM_PASSWORD,self.drmPassword)
        output+="%s:%s\n"%(WS_USERNAME,self.wsUsername)
        output+="%s:%s\n"%(WS_PASSWORD,self.wsPassword)
        return output


# These should map to the enum values in RunStatus.ResponseStatus in java.
STATUS_RESPONSE_OK="ok"
STATUS_RESPONSE_UNKNOWN_RUNID="unknown_runid"
STATUS_RESPONSE_ERROR="error"

# These should map to the fields in the RunStatus class in java.
class RunStatus():
    def __init__(self, runid, responseStatus, executionStatus):
        self.runid=runid
        self.responseStatus=responseStatus
        self.executionStatus=executionStatus

    def __repr__(self):
        s="Configuration Response: \n"
        s+="    runid: %s\n"
        s+="    response: %s\n"
        s+="    execstatus: %s"
        s=s%(self.runid, self.responseStatus, self.executionStatus)
        return s

# These should map to the fields in the RunDetailedStatus class in java.
class RunDetailedStatus(RunStatus):
    def __init__(self, runid, responseStatus, executionStatus, jobs, message, stacktrace=None):        
        self.runid=runid
        self.responseStatus=responseStatus
        self.executionStatus=executionStatus
        self.jobs=jobs  # Contains a list JobStatus objects
        self.message=message
        self.stacktrace=stacktrace

    def __repr__(self):
        s="Configuration Response: \n"
        s+="    runid: %s\n"
        s+="    response: %s\n"
        s+="    execstatus: %s"
        s+="    message: %s"
        s=s%(self.runid, self.responseStatus, self.executionStatus, '\n'.join(self.message.splitlines()))
        return s
    
    def default(self):
        return {'runid' : self.runid, 
                'responseStatus': self.responseStatus,
                'executionStatus': self.executionStatus,
                'message': self.message,
                'stacktrace': self.stacktrace,
                'jobs': [j.__dict__ for j in self.jobs]}             
        
# These should map to the fields in the JobStatus class in java.
class JobStatus(RunStatus):
    def __init__(self, tick, status):        
        self.tick=tick
        self.status=status

    def __repr__(self):
        return "JobStatus(%s, %s)"%(self.tick, self.status)

    def default(self):
        return self.__dict__

        
# These should map to the fields in the RunOutput class in java.        
class RunOutput():    
    def __init__(self, portName, productType, filePath):        
        self.portName=portName
        self.productType=productType
        self.filePath=filePath

    def default(self):
        return self.__dict__


# These should map to the enum values in PipelineTaskRunStatus in java.
JOB_PENDING = "PENDING"
JOB_QUEUED = "QUEUED"
JOB_EXECUTING = "EXECUTING"
JOB_COMPLETED = "COMPLETED"
JOB_ERROR = "ERROR"
JOB_UNKNOWN = "UNKNOWN"
JOB_HELD = "HELD"
JOB_SUSPENDED = "SUSPENDED"
JOB_ABORTED = "ABORTED"

# These should map to the fields in the PipelineTaskRun class in java.
class PipelineTaskRun():
    def __init__(self, executableName, dataflowTick, dataflowPath, processId, processingState, 
                       outputPath, processStdout, processStderr, packageRepositoryId):
        self.executableName=executableName
        self.packageRepositoryId=packageRepositoryId
        self.dataflowPath=dataflowPath
        self.dataflowTick=dataflowTick
        self.outputPath=outputPath
        self.processId=processId
        self.processStdout=processStdout
        self.processStderr=processStderr
        self.processingState=processingState
        
    def default(self):
        return self.__dict__
        

# These should map to the fields in the RunReport class in java.
class RunReport():    
    def __init__(self, runid, responseStatus, message, status, tasks, outputs):
        self.runid=runid
        self.status=status
        self.tasks=tasks
        self.outputs=outputs
        self.responseStatus=responseStatus
        self.message=message

    def default(self):
        return {'runid' : self.runid, 
                'status': self.status,
                'responseStatus': self.responseStatus,
                'message': self.message,
                'outputs': [o.__dict__ for o in self.outputs], 
                'tasks': [t.__dict__ for t in self.tasks]}            
        

    @classmethod
    def createFromPipelineExecution(cls, run):
        return RunReport(   run.runid, STATUS_RESPONSE_OK, "Run report successfully created", 
                            run.status, run.get_task_runs(), run.get_outputs())