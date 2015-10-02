'''
Created on Apr 30, 2015

@author: martin.melchior (at) fhnw.ch
'''
import os
from euclidwf.utilities import file_transporter

CONTEXT='context'
WSROOT='wsroot'
WORKDIR='workdir'
LOGDIR='logdir'
LOCALWORKDIR='local_workdir'
TRANSPORTER='transporter'
CHECKSTATUS_TIME="check_status.polltime"
CHECKSTATUS_TIMEOUT="check_status.timeout"

def create_context(run):
    context={}
    wsConfig=run.config.wsConfig
    drmConfig=run.config.drmConfig
    context[WSROOT]=wsConfig.workspaceRoot
    context[WORKDIR]=run.workdir
    context[LOGDIR]=run.logdir
    context[LOCALWORKDIR]=os.path.join(run.config.localcache,run.workdir)
    context[TRANSPORTER]=file_transporter.create(wsConfig, run.credentials.wsUsername, run.credentials.wsPassword)
    context[CHECKSTATUS_TIME]=float(drmConfig.statusCheckPollTime)
    context[CHECKSTATUS_TIMEOUT]=float(drmConfig.statusCheckTimeout)
    return context


def serializable(context):
    return {k:v for k,v in context.iteritems() if k!=TRANSPORTER}
