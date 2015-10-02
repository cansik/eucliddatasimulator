'''
Created on Apr 22, 2015

@author: martin.melchior
'''
import os
from euclidwf.utilities.error_handling import ConfigurationError

def load_config(configfile, configdict={}):
    with open(configfile,'r') as f:
        for line in f:
            line=line.strip()
            if line and not line.startswith("#"):
                itemname, itemvalue = line.split("=")
                itemname=itemname.strip()
                itemvalue=itemvalue.strip()
                itemvalue=os.path.expandvars(itemvalue)
                nameelms=itemname.split(".")
                addtodict(nameelms, itemvalue, configdict)


def addtodict(nameelms, itemvalue, _dict):
    propname=nameelms[0]
    if len(nameelms)==1:
        _dict[propname]=itemvalue
    else:
        if propname not in _dict:
            _dict[propname]={}
        nameelms.pop(0)
        addtodict(nameelms, itemvalue, _dict[propname])


def load_from_config(config, itemname, fail=True):
    if not itemname in config.keys():
        if fail:
            raise ConfigurationError("Missing config item: %s"%itemname)
        else:
            return None
    else:
        return config[itemname]
