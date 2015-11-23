from euclidwf.utilities import exec_loader
from flask import render_template
from flask.ext.socketio import emit
from main import app
import json

__author__ = 'cansik'

packageDefs = '/Users/cansik/git/fhnw/ip5/eucliddatasimulator/euclidwf_examples/packages/pkgdefs'

@app.route('/')
def index():
    executables = exec_loader.get_all_executables(packageDefs)
    return render_template('euclid.html', executables=executables.items())


def uploadFile(filename):
    pass
