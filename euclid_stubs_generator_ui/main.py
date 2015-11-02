from stubs_generator_server import app, socketio

from stubs_generator_ui import config

__author__ = 'cansik'

if __name__ == '__main__':
    socketio.run(app=app, host='0.0.0.0', port=config.ESG_SERVER_PORT)
