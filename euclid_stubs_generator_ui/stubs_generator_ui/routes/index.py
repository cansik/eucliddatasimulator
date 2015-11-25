import os

from euclidwf.framework.graph_builder import build_graph
from euclidwf.utilities import exec_loader
from flask import render_template, request, url_for, Flask, send_from_directory
from werkzeug.utils import secure_filename, redirect

import config
from main import app
import json

__author__ = 'cansik'

packageDefs = '../euclidwf_examples/packages/pkgdefs'

# This is the path to the upload directory
app.config['UPLOAD_FOLDER'] = 'uploads/'
# These are the extension that we are accepting to be uploaded
app.config['ALLOWED_EXTENSIONS'] = set(['py','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'])

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
    #if file and allowed_file(file.filename):
    # Make the filename safe, remove unsupported chars
    filename = secure_filename(file.filename)

    path = os.path.join(app.config['UPLOAD_FOLDER'])

    if not os.path.exists(path):
        os.makedirs(path)

    # Move the file form the temporal folder to
    # the upload folder we setup
    file.save(os.path.join(path,filename))
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
    #return send_from_directory(app.config['UPLOAD_FOLDER'], filename)
    executables = exec_loader.get_all_executables(packageDefs)

    file_path = os.path.join(app.config['UPLOAD_FOLDER'],filename)
    print(os.path.abspath(file_path))

    # load
    pipeline_globals = {}
    pipeline_locals = {}
    execfile(file_path)

    pipeline2 = None
    pipeline_found = False

    keys = locals().keys()
    for k in keys:
        var = locals().get(k)
        globals().update({k:var})
        if hasattr(var, '__call__') and hasattr(var,'ispipeline') and not pipeline_found:
            pipeline_found = True
            pipeline2 = var

    assert getattr(pipeline2,'ispipeline')
    assert pipeline2 is not None

    pydron_graph = build_graph(pipeline2)

    return render_template("euclid.html", executables=executables.items())

@app.route('/generate')
def generate():
    pass

# For a given file, return whether it's an allowed type or not
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in app.config['ALLOWED_EXTENSIONS']
