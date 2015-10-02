'''
Created on April 21, 2015

@author: martin.melchior
'''
import os
import shutil

from twisted.python.failure import Failure
from twisted.internet import defer

from remoot import ssh
from remoot.ssh import sftp

from euclidwf.utilities.error_handling import ConfigurationError


def create(config, username=None, password=None):
    protocol=config.protocol
    if protocol=="sftp":
        return SFTPFileTransporter(config.host, username, password)
    elif protocol=="file":
        return LocalFileTransporter()
    else:
        raise ConfigurationError("Protocol %s for accessing the workspace not supported."%protocol)
    

class AbstractFileTransporter(object):

    def fetch_file(self, remotepath, localpath):
        raise NotImplementedError()


class SFTPFileTransporter(AbstractFileTransporter):

    def __init__(self, hostname, username, password):
        self.hostname=hostname
        self.username=username
        self.password=password
        self.sftp=sftp(self.hostname, self.username, self.password) 

    def fetch_file(self, remotepath, localpath):

        def on_connected(connection):
                       
            def transmit_file(sftp):
                d = sftp.read_file(remotepath)
                                
                def close(data):
                    d_close=sftp.close()
                    d_close.addCallback(lambda _: data)
                    return d_close
                    
                d.addCallback(close)
                return d
            
            def error_while_connection_open(failure):
                d = connection.close()
                d.addBoth(lambda _:failure)
                return d
 
            def write_file(data):
                parentdir=os.path.dirname(localpath)
                if not os.path.exists(parentdir):
                    os.makedirs(parentdir)
                with open(localpath,'w') as f:
                    f.write(data)
                return localpath
                    
            d = connection.open_sftp()
            d.addCallback(lambda sftp: transmit_file(sftp))
            d.addCallback(write_file)
            d.addErrback(error_while_connection_open)
            return d
            
        d = ssh.connect(self.hostname, self.username, self.password)
        d.addCallback(on_connected)
        
        return d


    def upload_file(self, localpath, remotepath):

        def on_connected(connection):
                       
            def transmit_file(sftp):
                with open(localpath,'r') as f:
                    content = f.read()

                parentdir = os.path.dirname(remotepath)
                
                d = self.file_exists(parentdir)
                
                def create_dir(exists):
                    if not exists:   
                        createdir_d = sftp.create_directory(parentdir)

                        def dir_not_created(reason):
                            return Failure(ValueError("Target directory %s could not be created. \n Reason:%s."%(parentdir, reason.getTraceback())))
        
                        createdir_d.addErrback(dir_not_created)
                        return createdir_d

                    else:
                        return defer.succeed(None) 

                d.addCallback(create_dir)

                d.addCallback(lambda _: sftp.write_file(remotepath, content))
                                
                def close(_):
                    d_close=sftp.close()
                    d_close.addCallback(lambda _: remotepath)
                    return d_close

                def file_not_copied(reason):
                    return Failure(ValueError("Exception while copying file to remote path %s. \n Reason:%s."%(remotepath, reason.getTraceback())))
                    
                d.addCallback(close)
                d.addErrback(file_not_copied)
                return d
            
            def error_while_connection_open(failure):
                d = connection.close()
                d.addBoth(lambda _:failure)
                return d
                     
            d = connection.open_sftp()
            d.addCallback(lambda sftp: transmit_file(sftp))
            d.addErrback(error_while_connection_open)
            return d
            
        d = ssh.connect(self.hostname, self.username, self.password)
        d.addCallback(on_connected)
        
        return d
    
    
    def file_exists(self, path):

        d = ssh.connect(self.hostname, self.username, self.password)                
 
        def on_connected(connection):
                        
            def check(sftp):
                d = paths_exist(sftp,path)
                def close(result):
                    d_close=sftp.close()
                    d_close.addCallback(lambda _: result)                    
                    return d_close 
                d.addCallback(close)
                return d

            def error_while_connection_open(failure):
                d = connection.close()
                d.addBoth(lambda _:failure)
                return d
    
            d = connection.open_sftp()
            d.addCallback(check)
            d.addErrback(error_while_connection_open)
            return d
            
        d.addCallback(on_connected)        
        return d
        

def paths_exist(sftp, path):
    elms_to_check=pathelms_to_check(path, "/")
    deferreds=[]
    for elm in elms_to_check:
        d=sftp.list_directory(elm[0])
        def existing(contents, elm):
            _list=[_e[0] for _e in contents]
            return (elm[1], elm[1] in _list)
        def failed(reason):
            return reason
        d.addCallback(existing, elm)
        d.addErrback(failed)
        deferreds.append(d)
                    
    def check_nonexisting(results):
        notexisting=[elm[0] for (_,elm) in results if not elm[1]]
        return len(notexisting)==0
        
    d=defer.DeferredList(deferreds, fireOnOneErrback=True)
    d.addCallback(check_nonexisting)
    return d


class LocalFileTransporter(AbstractFileTransporter):

    def __init__(self):
        pass
        
    def copy_file(self, src, dest):
        d = defer.Deferred()
        if src==dest:
            return
        else:
            destdir=os.path.dirname(dest)
            if not os.path.exists(destdir):
                os.makedirs(destdir)
            try:
                shutil.copyfile(src, dest)
            except:
                d.errback()
        
        d.callback(None)
        return d

    def fetch_file(self, remotepath, localpath):
        return self.copy_file(remotepath, localpath)

    def upload_file(self, localpath, remotepath):
        return self.copy_file(localpath, remotepath)
    
    def file_exists(self, path, rootpath="/"):
        return defer.succeed(os.path.exists(path))
    

def pathelms_to_check(path, rootpath=""):
    if rootpath.endswith("/"):
        rootpath=rootpath[:-1]
    if path.endswith("/"):
        path=path[:-1]
    if not path.startswith(rootpath):
        return None
    else:
        elms=_add_parents(path, rootpath, [])
        if not elms[0][0]:
            elms[0]=("/",elms[0][1])
        return elms
        

def _add_parents(path, rootpath="", dirlist=[]):    
    if path==rootpath:
        return dirlist
    else:
        head,tail=os.path.split(path)
        if head.endswith("/"):
            head=head[:-1]
        dirlist.insert(0,(head,tail))
        return _add_parents(head, rootpath, dirlist)


if __name__ == '__main__':
    paths=pathelms_to_check("/Users/martinm/Projects/euclid/IAL", "/Users/martinm/")
    print '\n'.join([("%s:%s"%(k,v)) for (k,v) in paths])
    paths=pathelms_to_check("/Users/martinm","/")
    print '\n'.join([("%s:%s"%(k,v)) for (k,v) in paths])
