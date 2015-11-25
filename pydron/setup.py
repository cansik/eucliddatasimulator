# Copyright (C) 2014 Stefan C. Mueller

import os.path
from setuptools import setup, find_packages

if os.path.exists('README.rst'):
    with open('README.rst') as f:
        long_description = f.read()
else:
    long_description = None
    
setup(
    name = 'pydron',
    version = '0.1.0',
    description='Semi-automatic Python parallelization.',
    long_description=long_description,
    author='Stefan C. Mueller',
    author_email='stefan.mueller@fhnw.ch',
    url='https://github.com/smurn/pydron',
    packages = find_packages(),
    install_requires = ['astor', 'enum34', 'frozendict', 'sortedcontainers', 'anycall', 'remoot'],
)