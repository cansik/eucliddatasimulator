'''
Defines the views provided by the pipeline run server.

Created on May 29, 2015

@author: martin.melchior
'''
import datetime
import json
import os
import traceback
from flask import Flask, request, Response, logging
from flask.templating import render_template
from pygments import highlight
from pygments.formatters.html import HtmlFormatter
from pygments.lexers import get_lexer_by_name
from twisted.python.failure import Failure

from euclidwf.framework.runner import PipelineExecution, REPORT, STATUS, SUBMITTED,\
    PIPELINE, EXECSTATUS_ERROR, EXECSTATUS_EXECUTING, EXECSTATUS_ABORTED
from euclidwf.server import server_model, server_config
from euclidwf.utilities import visualizer, cmd_executor
from euclidwf.server.server_model import SUBM_NOT_ACCEPTED,\
    SubmissionResponse, SUBM_ALREADY_SUBMITTED, SUBM_FAILED,\
    CONFIG_OK, CONFIG_NOT_ACCEPTED, CONFIG_ERROR,\
    RunServerConfiguration, ConfigurationResponse, RUNID, RunConfiguration,\
    SUBM_EXECUTING, RunStatus, STATUS_RESPONSE_UNKNOWN_RUNID, STATUS_RESPONSE_OK,\
    RunDetailedStatus, RunReport, SUBM_ERROR, PRSJsonEncoder
from euclidwf.utilities.error_handling import ConfigurationError

from euclidwf.framework import drm_access, drm_access2
from twisted.internet.threads import blockingCallFromThread
from twisted.internet.defer import Deferred

DRM=drm_access

app = Flask(__name__)

logger = logging.getLogger(__name__)

reactor=None
registry = server_model.registry
history = server_model.history

@app.context_processor
def now():
    def formated_date():        
        return datetime.datetime.now().strftime("%A, %d. %B %Y %I:%M%p")
    return dict(now=formated_date)


@app.route('/configuration', methods=['POST'])
def configure():
    logger.debug("Configuring pipeline run server...")
    try:
        data = json.loads(request.data)
        rsc = RunServerConfiguration(data)
        status = None
        if server_model.config == None:
            server_model.config = rsc
            msg = "New configuration set for the pipeline run server."
            status = CONFIG_OK
        elif server_model.config != rsc:
            msg = "Configuration not accepted for the pipeline run server since another configuration has already been set."
            status = CONFIG_NOT_ACCEPTED
        else:
            msg = "The same configuration has already been set for the pipeline run server. Nothing to do."
            status = CONFIG_OK
        
        if status==CONFIG_OK: 
            drm_status, drm_msg = blockingCallFromThread(reactor, _configure_drm, rsc)
            msg += "\n%s"%drm_msg
            if drm_status != CONFIG_OK:
                status=CONFIG_ERROR

        logger.info(msg)
        response = ConfigurationResponse(status, msg)
        return Response(json.dumps(response.__dict__), mimetype="application/json")

    except:
        msg = "An error occured while configuring the run server. Reason: \n%s" % traceback.format_exc()
        logger.error(msg)
        response = ConfigurationResponse(CONFIG_ERROR, msg)
        return Response(json.dumps(response.__dict__), mimetype="application/json")


@app.route('/runs', methods=['GET'])
def runs():
    context={}
    context['table']={'cols':RUNS_COLS, 'rows':_runs()}
    return render_template("runids.html", **context)
    #regRuns = RegisteredRuns(_runs())
    #return Response(json.dumps(regRuns.__dict__), mimetype="application/json")


@app.route('/runs', methods=['POST'])
def submit():
    data = json.loads(request.data)
    if not server_model.config:
        runid=data[RUNID] if RUNID in data else None
        msg="Server is not configured - hence it cannot accept any submits."
        response=SubmissionResponse(runid, SUBM_NOT_ACCEPTED, msg)
        return Response(json.dumps(response.__dict__), mimetype="application/json")

    runConfig = RunConfiguration(data)
    response = _submit(runConfig)
    return Response(json.dumps(response.__dict__), mimetype="application/json")


@app.route('/status/<runid>/status', methods=['GET'])
def status(runid):
    #if request.headers['Content-Type'] == 'application/json':
    response = _status(runid)
    return Response(json.dumps(response.__dict__, cls=PRSJsonEncoder), mimetype="application/json")


@app.route('/runs/<runid>/detailed_status', methods=['GET'])
def detailed_status(runid):
    #if request.headers['Content-Type'] == 'application/json':
    response = _detailed_status(runid)
    return Response(json.dumps(response.__dict__, cls=PRSJsonEncoder), mimetype="application/json")
        

@app.route('/runs/<runid>/report', methods=['GET'])
def report(runid):    
    #if request.headers['Content-Type'] == 'application/json':
    if not registry.has_run(runid):
        response=RunReport(runid, STATUS_RESPONSE_UNKNOWN_RUNID, "No pipeline run with id %s registered."%runid, None, None, None)
        return Response(json.dumps(response.__dict__, mimetype="application/json"))
    run = registry.get_run(runid)
    response=RunReport.createFromPipelineExecution(run)
    return Response(json.dumps(response.__dict__, cls=PRSJsonEncoder), mimetype="application/json")

@app.route('/runs/<runid>/details', methods=['GET'])
def run_details(runid):    
    # rather experimental for the web pages ...
    run = registry.get_run(runid)
    context = run.todict()
    tasks_cols = ({'name':'tick'}, {'name':'model path'}, {'name':'pid'},
                {'name':'status'}, {'name':'duration'}, {'name':'output dir'})
    tasks_rows = context[REPORT]
    context['tasks'] = {'cols':tasks_cols, 'rows':tasks_rows}
    # create pygraph, the map file is also created, but does not work yet (--> tooltips,etc)
    graph = run.traverser.get_graph()
    pygraph = visualizer.create_pygraph(graph)
    pngname = '%s_%s.png' % (context[PIPELINE]['name'], runid)
    mapname = '%s_%s.html' % (context[PIPELINE]['name'], runid)
    _draw_pygraph(pygraph, pngname, mapname)
    context['graph_png'] = '%s/%s' % (app.config[server_config.IMG_DIR], pngname)
    context['graph_map'] = '%s/%s' % (app.config[server_config.IMG_DIR], mapname)        
    return render_template("run_details.html", **context)


def _configure_drm(serverconfig):
    drmconfig_cmd=serverconfig.drmConfig.configureCmd
    drmconfig={"SYSTEM":{"work_dir_host":serverconfig.wsConfig.workspaceRoot}}
    if DRM==drm_access2:
        cmd=DRM.create_configure_cmd(drmconfig_cmd, drmconfig)
        executor = cmd_executor.create(serverconfig.drmConfig, serverconfig.localcache, serverconfig.credentials.drmUsername, serverconfig.credentials.drmPassword)
        drm_response = _call_drm_configure(cmd, executor, drmconfig) 
               
        def on_success(result):            
            if result==CONFIG_OK:
                return CONFIG_OK, "DRM successfully configured."
            else:
                return CONFIG_ERROR, "Failure occurred while trying to configure DRM."
    
        def on_fail(reason):
            return CONFIG_ERROR, "Failure occurred while trying to configure DRM."

        drm_response.addCallback(on_success)
        drm_response.addErrback(on_fail)    
    else:
        drm_response = Deferred()
        drm_response.callback((CONFIG_OK, "DRM not configured in this version."))
    return drm_response


def _call_drm_configure(cmd, executor, expected):
    logger.info("Configure DRM with the command:\n %s"%' '.join(cmd))
    d=executor.execute(cmd)
    
    data=[]        
    def on_executed(process):
        process.stdout.add_callback(data.append)
        process.stderr.add_callback(data.append)
        return process.exited.next_event()

    def on_finished(reason):
        if reason:
            return Failure(ValueError("Exception occurred while configuring DRM. Reason: %s \ Stdout: %s"%(reason.getTraceback(), ''.join(data))))
        else:
            response=DRM.read_configure_response(data)
            if response["SYSTEM"]["work_dir_host"]==expected["SYSTEM"]["work_dir_host"]:
                return CONFIG_OK
            else:
                return Failure(ValueError("Configuration of DRM not successful. STDERR \n%s"%response.stderr))

    def on_failure(reason):
        failure = Failure(ValueError("Exception at submit with reason: %s \n stdout: %s"%(reason.getTraceback(), ''.join(data))))
        return failure
                
    d.addCallback(on_executed)
    d.addCallback(on_finished)
    d.addErrback(on_failure) 
    return d


def _submit(runConfig):
    if registry.has_run(runConfig.runid):
        msg = "Runid %s does already exist" % runConfig.runid
        return SubmissionResponse(runConfig.runid, SUBM_ALREADY_SUBMITTED, msg)
        
    try:
        pipeline_exec = PipelineExecution(runConfig, server_model.config)
        logger.debug("PipelineExecution object for runid %s created."%runConfig.runid)
        pipeline_exec.initialize()
        logger.debug("PipelineExecution object for runid %s initialized."%runConfig.runid)
        registry.add_run(runConfig.runid, pipeline_exec)
        logger.debug("PipelineExecution object for runid %s registered."%runConfig.runid)
        _ = pipeline_exec.start()
        msg = "Pipeline execution for runid %s started."%runConfig.runid
        logger.info(msg)
    except ConfigurationError as ce:
        msg = 'Submission failed with exception %s.' % str(ce)
        stacktrace= 'Stacktrace: \n%s'%'\n'.join(traceback.format_stack())
        logger.warn(msg)
        return SubmissionResponse(runConfig.runid, SUBM_FAILED, msg, stacktrace)
    except Exception as e:
        msg = 'Submission failed with exception %s.' % str(e)
        stacktrace= 'Stacktrace: \n%s'%'\n'.join(traceback.format_stack())
        logger.error(msg)
        return SubmissionResponse(runConfig.runid, SUBM_ERROR, msg, stacktrace)
    try:
        history.add_entry("Runid %s submitted to the system" % runConfig.runid, datetime.datetime.now())
    except Exception as e:
        logger.warn("Runid %s could not be appended to the history."%runConfig.runid)
        msg=msg+"\n But not entry could be added the server history."

    return SubmissionResponse(runConfig.runid, SUBM_EXECUTING, msg)

def _status(runid):
    if not registry.has_run(runid):
        return RunStatus(runid, STATUS_RESPONSE_UNKNOWN_RUNID, None)
    run = registry.get_run(runid)
    return RunStatus(runid, STATUS_RESPONSE_OK, str(run.get_status()))


def _detailed_status(runid):
    if not registry.has_run(runid):
        return RunDetailedStatus(runid, STATUS_RESPONSE_UNKNOWN_RUNID, None, None, "No pipeline run with Id %s found in the registry."%runid)
    run = registry.get_run(runid)    
    return RunDetailedStatus(runid, STATUS_RESPONSE_OK, str(run.get_status()), run.get_jobs_status(), "")



RUNS_COLS = [{'name':'run (ID)'}, {'name':'status'}, { 'name' : 'pipeline'}, { 'name' : 'version' }, {'name' : 'submitted'}]
def _runs():
    return [{RUNID : runid,
             STATUS : str(registry.get_run(runid).status),
             PIPELINE : registry.get_run(runid).pipeline.func_name,
             'version' : 'n/a',
             SUBMITTED : registry.get_run(runid).created.strftime("%A, %d. %B %Y %I:%M%p")
             } for runid in registry.get_all_runids()]


def _draw_pygraph(pygraph, pngname, mapname):
    pngpath = os.path.join(app.config[server_config.STATIC_FILES], app.config[server_config.IMG_DIR], pngname) 
    if os.path.exists(pngpath):
        os.remove(pngpath)   
    mappath = os.path.join(app.config[server_config.STATIC_FILES], app.config[server_config.IMG_DIR], mapname)
    if os.path.exists(mappath):
        os.remove(mappath)       
    pygraph.draw(path=pngpath, format='png')
    pygraph.draw(path=mappath, format="cmapx")


@app.route('/runs/<runid>/script', methods=['GET'])
def run_script(runid):
    if not registry.has_run(runid):
        return {'success' : False,
                'message' : "Runid %s does not exist" % runid }
    
    run = registry.get_run(runid)
    pipeline = run.todict()['pipeline']


    # load file and convert it into html - with code highlighting:
    with open (pipeline['file'], "r") as srcfile:
        code = srcfile.read()    
    lexer = get_lexer_by_name("python", stripall=True)
    formatter = HtmlFormatter(linenos=True, cssclass="source")
    outfilepath = os.path.join(app.config['CODE_DIR'], pipeline['name'] + ".html")
    with open(outfilepath, 'w') as outfile: 
        _ = highlight(code, lexer, formatter, outfile=outfile)
    context = {RUNID:runid, 'script':'generated/%s.html' % pipeline['name'], 'pipeline':pipeline['name']}
    return render_template("script.html", **context)


@app.route('/runs/<runid>/reset', methods=['PUT'])
def reset(runid):
    response = _reset(runid)
    return json.dumps(response)        

def _reset(runid):
    if not registry.has_run(runid):
        return {'success' : False,
                'message' : "Runid %s does not exist" % runid }

    try:
        pipeline_exec = registry.get_run(runid)
    except Exception as e:
        return {STATUS : EXECSTATUS_ERROR,
                'reason' : '%s\n%s' % (str(e), '\n'.join(traceback.format_stack()))}
                    
    pipeline_exec.reset()
    history.add_entry("Runid %s reset." % runid, datetime.datetime.now())
    return {STATUS : EXECSTATUS_EXECUTING,
            RUNID  : runid }


@app.route('/runs/<runid>/cancel', methods=['DELETE'])
def cancel(runid):
    response = _cancel(runid)
    return json.dumps(response)        

def _cancel(runid):
    if not registry.has_run(runid):
        return {'success' : False,
                'message' : "Runid %s does not exist" % runid }
    try:
        pipeline_exec = registry.get_run(runid)
    except Exception as e: 
        return {STATUS : EXECSTATUS_ERROR,
                'reason' : '%s\n%s' % (str(e), '\n'.join(traceback.format_stack()))}
        
    pipeline_exec.cancel()
    registry.delete_run(runid)
    history.add_entry("Runid %s cancelled." % runid, datetime.datetime.now())
    return {STATUS : EXECSTATUS_ABORTED,
            RUNID  : runid}

if __name__ == "__main__":
    app.run()
