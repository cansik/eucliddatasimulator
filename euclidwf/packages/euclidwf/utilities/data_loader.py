'''
Created on Apr 22, 2015

@author: martin.melchior
'''
import os
from euclidwf.utilities.error_handling import ConfigurationError

def load_data(datafile, datadict):
    with open(datafile,'r') as f:
        for line in f:
            line=line.strip()
            if line and not line.startswith("#"):
                itemname, itemvalue = line.split("=")
                itemname=itemname.strip()
                itemvalue=itemvalue.strip()
                if itemvalue in datadict.keys():
                    raise ConfigurationError("Data item with name %s already loaded.")
                else:
                    datadict[itemname]=os.path.expandvars(itemvalue)

