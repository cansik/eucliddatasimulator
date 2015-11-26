import StringIO
import os
import pickle
import zipfile
from io import BytesIO
import time

import errno
import werkzeug
from euclid_stubs_generator.mock_generator import MockGenerator
from euclid_stubs_generator.stubs_generator import StubsGenerator
from euclidwf.framework.graph_builder import build_graph
from euclidwf.framework.graph_tasks import ExecTask
from euclidwf.framework.taskdefs import Executable, ComputingResources
from euclidwf.utilities import exec_loader
from flask import render_template, request, url_for, Flask, \
    send_from_directory, session, make_response, send_file
from flask.ext.cache import Cache
from werkzeug.utils import secure_filename, redirect
import config
from controller.index_controller import IndexController
from controller.stub_info import StubInfo
from main import app
import json

__author__ = 'cansik'

packageDefs = '../euclidwf_examples/packages/pkgdefs'
outputFolder = 'temp/'

# This is the path to the upload directory
app.config['UPLOAD_FOLDER'] = 'uploads/'
# These are the extension that we are accepting to be uploaded
app.config['ALLOWED_EXTENSIONS'] = set(
    ['py', 'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'])

controller = IndexController()


@app.route('/')
def index():
    return render_template('FirstPage.html')


# Route that will process the file upload
@app.route('/upload', methods=['POST'])
def upload():
    # Get the name of the uploaded file
    print('upload method called')
    file = request.files['file']
    # Check if the file is one of the allowed types/extensions
    # if file and allowed_file(file.filename):
    # Make the filename safe, remove unsupported chars
    filename = secure_filename(file.filename)

    path = os.path.join(app.config['UPLOAD_FOLDER'])

    if not os.path.exists(path):
        os.makedirs(path)

    # Move the file form the temporal folder to
    # the upload folder we setup
    file.save(os.path.join(path, filename))
    print(os.path.join(app.config['UPLOAD_FOLDER'], filename))
    # Redirect the user to the uploaded_file route, which
    # will basicaly show on the browser the uploaded file
    return redirect(url_for('uploaded_file', filename=filename))


# This route is expecting a parameter containing the name
# of a file. Then it will locate that file on the upload
# directory and show it on the browser, so if the user uploads
# an image, that image is going to be show after the upload
@app.route('/overview/<filename>')
def uploaded_file(filename):
    print(os.path.join(app.config['UPLOAD_FOLDER'], filename))
    # return send_from_directory(app.config['UPLOAD_FOLDER'], filename)
    executables = exec_loader.get_all_executables(packageDefs)

    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    print(os.path.abspath(file_path))

    pydron_graph = controller.build_graph_from_file(file_path)
    files = controller.get_all_start_inputs_from_graph(pydron_graph)

    # filter relevant executables
    ticks = pydron_graph.get_all_ticks()
    tasks = map(lambda t: pydron_graph.get_task(t), ticks)
    task_names = map(lambda t: t.name if hasattr(t, 'name') else t.command,
                     tasks)

    filtered_execs = dict(
        {(k, v) for k, v in executables.items() if k in task_names})

    # todo: melchior fragen warum nicht alle da?!

    # filter(lambda e: e in task_names, executables.keys())

    content = pickle.dumps(filtered_execs)
    session['execs'] = content
    session['files'] = files

    return render_template("euclid.html", files=files,
                           executables=filtered_execs.items())


@app.route('/generate', methods=['POST'])
def generate():
    filterd_executables = pickle.loads(session['execs'])

    # create output dir
    mkdir_p(outputFolder)

    # Set ComputingResources in the executables
    for key in filterd_executables.keys():
        executable = filterd_executables[key]
        cores = request.form[key + '_cores']
        ram = request.form[key + '_ram']
        walltime = request.form[key + '_walltime']
        if isinstance(executable, Executable):
            executable.resources = ComputingResources(cores, ram,
                                                      controller.parseWallTime(
                                                          walltime))

    dict_ka = {}
    for key in filterd_executables.keys():
        dict_ka.update({key: {}})

    for key in filterd_executables.keys():
        executable = filterd_executables[key]
        for outputfile in executable.outputs:
            dict_ka[key].update({outputfile.name: request.form[
                '%s_%s_size' % (key, outputfile.name)]})

    # Set Pipeline Input Size
    files = session['files']
    """:type files: dict"""
    for (key, value) in files.items():
        files[key] = int(request.form[key])

    on = 'pipelineInputCheckBox' in request.form

    stub_infos = map(lambda (k, v): StubInfo(k, v),
                     filterd_executables.items())
    list_str = []
    for key, value in filterd_executables.items():
        list_str.append(StubInfo(key, value))

    with open(os.path.join(outputFolder, 'resources.txt'), 'w') as outfile:
        json.dump(files, outfile)

    StubsGenerator(outputFolder).generate_stubs(filterd_executables, dict_ka)
    MockGenerator(outputFolder).generate_mocks(files)

    memory_file = BytesIO()
    with zipfile.ZipFile(memory_file, 'w') as zf:
        for dirname, subdirs, files in os.walk(outputFolder):
            for filename in files:
                data = zipfile.ZipInfo(filename)
                data.date_time = time.localtime(time.time())[:6]
                data.compress_type = zipfile.ZIP_DEFLATED
                bytes = open(os.path.join(dirname, filename)).read()
                zf.writestr(data, bytes)
    memory_file.seek(0)
    return send_file(memory_file, attachment_filename='test.zip',
                     as_attachment=True)


# For a given file, return whether it's an allowed type or not
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in app.config['ALLOWED_EXTENSIONS']


# creates a dictionary tree
def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise
