#!/usr/bin/env python
# Martin.Melchior (at) fhnw.ch

import os.path
from setuptools import setup, find_packages

if os.path.exists('README.rst'):
    with open('README.rst') as f:
        long_description = f.read()
else:
    long_description = None
    
install_requires = [
    'flask',
    'pygments',
    'pillow',
    'pydron',
    'remoot'
]

setup(
    name = 'wfm',
    version = '0.5.0',
    description='Euclid pipeline framework',
    long_description=long_description,
    author='Martin Melchior',
    author_email='martin.melchior@fhnw.ch',
    url=None,
	install_requires = install_requires,
    scripts=['bin/client_for_test.py','bin/pipeline_runner.py','bin/pipeline_server_flask.py'],
    package_dir = {'': 'packages'},
    packages = find_packages("packages")
)
