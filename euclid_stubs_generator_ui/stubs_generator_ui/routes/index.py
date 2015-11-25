import os

from euclid_stubs_generator.stubs_generator import StubsGenerator
from euclidwf.framework.graph_builder import build_graph
from euclidwf.utilities import exec_loader
from flask import render_template, request, url_for, Flask, send_from_directory
from werkzeug.utils import secure_filename, redirect

import config
from controller.index_controller import IndexController
from main import app
import json

__author__ = 'cansik'

packageDefs = '../euclidwf_examples/packages/pkgdefs'

# This is the path to the upload directory
app.config['UPLOAD_FOLDER'] = 'uploads/'
# These are the extension that we are accepting to be uploaded
app.config['ALLOWED_EXTENSIONS'] = set(['py','txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'])

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

    pydron_graph = controller.build_graph_from_file(file_path)
    files = controller.get_all_start_inputs_from_graph(pydron_graph)

    return render_template("euclid.html", files = files, executables=executables.items())

@app.route('/generate')
def generate():
    pass

# For a given file, return whether it's an allowed type or not
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in app.config['ALLOWED_EXTENSIONS']
