from stubs_generator_ui.stubs_generator_server import app
from stubs_generator_ui import config

__author__ = 'cansik'

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=config.ESG_SERVER_DEBUG_MODE, port=config.ESG_SERVER_PORT)
