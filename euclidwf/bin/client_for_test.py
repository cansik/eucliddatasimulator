#!/usr/bin/env python
'''
Created on May 27, 2015

@author: martin.melchior
'''
import argparse
import httplib
import json
from euclidwf.utilities import data_loader
from euclidwf.server.server_model import CONFIG_DRM, CONFIG_LOCALCACHE,\
    DRM_CONFIGURE_CMD, DRM_CHECKSTATUS_CMD, DRM_SUBMIT_CMD, DRM_CLEANUP_CMD,\
    DRM_DELETE_CMD, DRM_PROTOCOL, DRM_HOST, DRM_PORT, DRM_STATUSCHECK_POLLTIME,\
    DRM_STATUSCHECK_TIMEOUT, WS_PROTOCOL, WS_ROOT, WS_HOST, WS_PORT, RUNID,\
    PIPELINE_SCRIPT, DRM_USERNAME, WORKDIR, LOGDIR, PIPELINE_DIR, PKG_REPOSITORY,\
    INPUTDATA_PATHS, CREDENTIALS, DRM_PASSWORD, WS_USERNAME, WS_PASSWORD,\
    CONFIG_WS, ConfigurationResponse, SubmissionResponse, RunDetailedStatus


def load_run_request_spe(runid):
    datafile="/Users/martinm/Projects/euclid/pipeline_framework/prototype/wfm/trunk/euclidwf_examples/resources/spe_test.data"
    pipeline="spe_test_pipeline.py"
    return load_run_request(datafile, pipeline)
 
def load_run_request_vis(runid):
    datafile="/Users/martinm/Projects/euclid/pipeline_framework/prototype/wfm/trunk/euclidwf_examples/resources/vis_pipeline.data"
    pipeline="vis_pipeline.py"
    return load_run_request(datafile, pipeline)

def load_run_request_test(runid):
    datafile="/Users/martinm/Projects/euclid/pipeline_framework/prototype/wfm/trunk/euclidwf_examples/resources/vis_pipeline.data"
    pipeline="single_step.py"
    return load_run_request(datafile, pipeline)
   
def load_run_request(datafile, pipeline):
    inputs={}
    data_loader.load_data(datafile, inputs)
    data = {}
    data[RUNID]=str(runid)
    data[WORKDIR]=str(inputs[WORKDIR])
    data[LOGDIR]=str(inputs[LOGDIR])
    data[PIPELINE_SCRIPT]=str(pipeline)
    data[PIPELINE_DIR]='/Users/martinm/Projects/euclid/pipeline_framework/prototype/wfm/trunk/euclidwf_examples/examples'
    data[PKG_REPOSITORY]='/Users/martinm/Projects/euclid/pipeline_framework/prototype/wfm/trunk/euclidwf_examples/packages/pkgdefs'
    data[INPUTDATA_PATHS]={str(k):str(v) for k,v in inputs.iteritems() if k not in (WORKDIR,LOGDIR)}
    data[CREDENTIALS]={DRM_USERNAME:'sgsst',DRM_PASSWORD:'euclid',WS_USERNAME:'sgsst',WS_PASSWORD:'euclid'}
    return json.dumps(data)

def submit(runid, pipeline):
    if pipeline=='vis':
        jsondata = load_run_request_vis(runid)
    elif pipeline=='spe':
        jsondata = load_run_request_spe(runid)
    elif pipeline=='test':
        jsondata = load_run_request_test(runid)
    else:
        return
    connection = httplib.HTTPConnection(host='localhost', port=5000)
    headers = {'Content-type': 'application/json'}
    connection.request('POST', '/runs', jsondata, headers)
    return connection.getresponse()

def status(runid):
    connection = httplib.HTTPConnection(host='localhost', port=5000)
    headers = {'Content-type': 'application/json'}
    jsondata=json.dumps({RUNID:runid})
    connection.request('GET', '/runs/%s'%runid, jsondata, headers)
    return connection.getresponse()
    
def configure_data():
    data={}
    data[CONFIG_LOCALCACHE]="/Users/martinm/Projects/euclid/IAL_tests/wfm_prototype/testrun_x/cache"   
    drmConfig={}
    drmConfig[DRM_CONFIGURE_CMD]="ialdrm_config"
    drmConfig[DRM_SUBMIT_CMD]="ialdrm_submit_job"
    drmConfig[DRM_CHECKSTATUS_CMD]="ialdrm_check_job_status"
    drmConfig[DRM_CLEANUP_CMD]="ialdrm_cleanup_job"
    drmConfig[DRM_DELETE_CMD]="ialdrm_cancel_job"    
    drmConfig[DRM_PROTOCOL]="ssh"
    drmConfig[DRM_HOST]="virtualhost2"
    drmConfig[DRM_PORT]=""
    drmConfig[DRM_STATUSCHECK_POLLTIME]=5.0
    drmConfig[DRM_STATUSCHECK_TIMEOUT]=7200
    data[CONFIG_DRM]=drmConfig
    wsConfig={}
    wsConfig[WS_PROTOCOL]="sftp"
    wsConfig[WS_HOST]="virtualhost2"
    wsConfig[WS_PORT]=""
    wsConfig[WS_ROOT]="/home/sgsst/workspace"
    data[CONFIG_WS]=wsConfig
    data[CREDENTIALS]={DRM_USERNAME:'sgsst',DRM_PASSWORD:'euclid',WS_USERNAME:'sgsst',WS_PASSWORD:'euclid'}
    return data


def configure():
    connection = httplib.HTTPConnection(host='localhost', port=5000)
    headers = {'Content-type': 'application/json'}
    jsondata=json.dumps(config)
    connection.request('POST', '/configuration', jsondata, headers)
    return connection.getresponse()
    

def parse_cmd_args():
    parser = argparse.ArgumentParser(description="Test application for submitting and status checks for the pipeline server.")
    parser.add_argument("--cmd", help="configure | submit |checkstatus")
    parser.add_argument("--runid", help="RunId.")
    parser.add_argument("--runids", nargs='+', help="RunIds.")    
    parser.add_argument("--pipeline", help="Pipeline.")
    
    return parser.parse_args()    


config=None

if __name__ == '__main__':
    args = parse_cmd_args()
    config=configure_data()
    if args.cmd=='configure':
        response=configure()
        response_dict=json.loads(response.read())
        print repr(ConfigurationResponse(str(response_dict["status"]),str(response_dict["message"])))
    elif args.cmd=='submit':
        for runid in args.runids:
            response=submit(runid, args.pipeline)
            response_dict=json.loads(response.read())
            print repr(SubmissionResponse(str(response_dict["runid"]), str(response_dict["status"]),str(response_dict["message"])))
    elif args.cmd=='checkstatus':
        response=status(args.runid)
        response_dict=json.loads(response.read())
        print repr(RunDetailedStatus(str(response_dict["runid"]), str(response_dict["responseStatus"]), str(response_dict["executionStatus"]),str(response_dict["message"])))
        print '\n'.join(response.read().splitlines())
    else:
        print "Command %s not found!"%args.cmd
        
