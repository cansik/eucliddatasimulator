'''
Created on April 21, 2015

@author: martin.melchior
'''
from remoot.starter import LocalStarter, SSHStarter
from euclidwf.utilities.error_handling import ConfigurationError


def _resolve_status(data):
    pass

PROTOCOL_SSH="ssh"
PROTOCOL_LOCAL="local"

def create(config, cachedir, username=None, password=None):
    protocol=config.protocol
    if protocol==PROTOCOL_SSH:
        return SSHCmdExecutor(config, cachedir, username, password)
    elif protocol==PROTOCOL_LOCAL:
        return LocalCmdExecutor(cachedir)
    else:
        raise ConfigurationError("Protocol %s for accessing DRM not supported."%protocol)
    

class AbstractCmdExecutor(object):

    def execute(self, command):
        raise NotImplementedError()


class LocalCmdExecutor(AbstractCmdExecutor):

    def __init__(self, cachedir):
        cachedir=cachedir
        self.starter=LocalStarter(cachedir)

    def execute(self, command):
        return self.starter.start(command)
    


class SSHCmdExecutor(AbstractCmdExecutor):
    
    def __init__(self, config, cachedir, username, password):
        self.hostname=config.host
        self.username=username
        self.password=password
        self.starter=SSHStarter(hostname=self.hostname, username=self.username, password=self.password, tmp_dir=cachedir)

    def execute(self, command):
        return self.starter.start(command)
