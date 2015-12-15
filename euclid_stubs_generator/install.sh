#!/usr/bin/env bash
sudo pip uninstall euclid_stubs_generator -y

cwd=$(pwd)
cd /Library/Python/2.7/site-packages/
sudo rm -r euclid_stubs_generator

cd $cwd
sudo python setup.py install