#!/usr/bin/env python
'''
Created on May 29, 2015

@author: martin.melchior
'''
import optparse

from euclidwf.server import server_views_flask
from euclidwf.server.server_model import RunServerConfiguration
from euclidwf.utilities import config_loader


STATIC_FILES='STATIC_FILES'

def start_server(args):
    port=args.port
    cfgfile=args.baseconfig
    print 'Twisted on port {port}...'.format(port=port)
    from twisted.internet import reactor
    from twisted.web.server import Site
    from twisted.web.wsgi import WSGIResource

    app = server_views_flask.app
    server_views_flask.reactor=reactor
    configure(app, cfgfile)
    if args.appconfig:
        configure_app(args.appconfig)    
    resource = WSGIResource(reactor, reactor.getThreadPool(), app)
    site = Site(resource)
    reactor.listenTCP(port, site, interface="0.0.0.0")
    reactor.run()


def configure(app, cfgfile):
    app.config.from_pyfile(cfgfile)
    app.static_folder=app.config[STATIC_FILES]
    

def configure_app(configfile):
    print "Application configured at start up - you probably want to run the server stand-alone..."
    configdata={}
    config_loader.load_config(configfile, configdata)
    rsc = RunServerConfiguration(configdata)
    server_views_flask.server_model.config = rsc

    
def parse_args():
    parser = optparse.OptionParser(usage="%prog [options]  or type %prog -h (--help)")
    #parser.add_option('--port', help='Twisted event-driven web server', action="callback", callback=start_server, type="int");
    parser.add_option('-p', '--port', dest='port', default=701, help='Port to run the server at.', type="int")
    parser.add_option('-b', '--baseconfig', dest='baseconfig', help='Basic config file for setting up the server.')
    parser.add_option('-c', '--appconfig', dest='appconfig', help='Config file used by the application.')
    return parser.parse_args()


def main():
    args,_=parse_args()
    start_server(args)

if __name__ == "__main__":
    main()
