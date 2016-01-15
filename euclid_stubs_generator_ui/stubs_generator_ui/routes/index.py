import os
import pickle
import shutil
import errno

from euclid_stubs_generator.mock_generator import MockGenerator
from euclid_stubs_generator.stubs_generator import StubsGenerator
from euclidwf.utilities import exec_loader
from flask import render_template, request, url_for, session, send_file
from werkzeug.utils import secure_filename, redirect
from stubs_generator_ui.controller.index_controller import IndexController
from main import app

__author__ = 'cansik'

# This is the path to the package definitions
packageDefs = '../euclidwf_examples/packages/pkgdefs'
# This is the path to the outputfolder
outputFolder = 'temp/'

# This is the path to the upload directory
app.config['UPLOAD_FOLDER'] = 'uploads/'
# These are the extension that we are accepting to be uploaded
app.config['ALLOWED_EXTENSIONS'] = set(['py', 'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'])

controller = IndexController()


@app.route('/')
def index():
    return render_template('FirstPage.html')


# Route that will process the file upload
@app.route('/upload', methods=['POST'])
def upload():
    # Get the name of the uploaded file
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

    #Load all Package Definitions from the packageDef folder
    executables = exec_loader.get_all_executables(packageDefs)

    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)

    pydron_graph = controller.build_graph_from_file(file_path)

    # Following uncommented code could plot the pydron graph:
    # from euclidwf.utilities import visualizer
    # visualizer.visualize_graph(pydron_graph)

    files = controller.get_all_start_inputs_from_graph(pydron_graph)

    filtered_execs = controller.filter_executables_with_graph(pydron_graph,
                                                              executables)  # dict({(k, v) for k, v in executables.items() if k in task_names})
    controller.setDefaultComputingResources(executables, filtered_execs)

    # set session variables to use them in the generate method
    session['files'] = files
    session['pipeline_name'] = os.path.splitext(filename)[0]
    session['execs'] = pickle.dumps(filtered_execs)

    return render_template("euclid.html", files=files, executables=filtered_execs)


@app.route('/generate', methods=['POST'])
def generate():

    #data = shelve.open(os.path.join(outputFolder,'shelvedata'))
    #temp = data[session['uid']]
    pipeline_name = session['pipeline_name']

    #filterd_executables = pickle.loads(temp)

    execs = pickle.loads(session['execs'])

    # create output dir
    pipeline_output = os.path.join(outputFolder, pipeline_name) + '/'
    mkdir_p(pipeline_output)

    for stubinfo in execs:
        stubinfo.cores = int(float(request.form[stubinfo.command+'_cores']))
        stubinfo.ram = int(float(request.form[stubinfo.command+'_ram']))
        stubinfo.walltime = controller.parseWallTime(request.form[stubinfo.command+'_walltimedisplay'])    #Parsing the walltime to ensure right format
        if stubinfo.isParallelSplit:
            stubinfo.split_parts = int(float(request.form[stubinfo.command+'_splits']))

        tempTupleList = list()
        for outputfile in stubinfo.outputfiles:
            tempTupleList.append((outputfile[0], int(float(request.form['%s_%s_size' % (stubinfo.command, outputfile[0])]))))
        stubinfo.outputfiles = tempTupleList

    # Set Pipeline Input Size
    files = session['files']
    """:type files: dict"""
    for (key,value) in files.items():
        files[key] = int(request.form[key])

    on =  'pipelineInputCheckBox' in request.form

    StubsGenerator(os.path.join(pipeline_output, "bin")).generate_stubs(execs)
    MockGenerator(pipeline_output).generate_script(files)

    if on:
        MockGenerator(pipeline_output).generate_mocks(files)

    controller.writeComputingResources(execs, pipeline_output)
    controller.writePortMapping(files, pipeline_output)

    memory_file = controller.createZip(pipeline_output)

    # remove output folder
    shutil.rmtree(pipeline_output)

    #Send the zipped folder to the user
    return send_file(memory_file, attachment_filename='%s.zip' % pipeline_name, as_attachment=True)


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
