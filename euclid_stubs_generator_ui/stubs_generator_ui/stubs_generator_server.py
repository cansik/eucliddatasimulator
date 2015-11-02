from flask import Flask
from flask.ext.cors import CORS
from flask.ext.socketio import SocketIO
import config

app = Flask(__name__,
            static_folder=config.ESG_SERVER_STATIC_FOLDER,
            static_url_path=config.ESG_SERVER_STATIC_PATH,
            template_folder=config.ESG_SERVER_TEMPLATE_FOLDER)

app.config['SECRET_KEY'] = 'secret!'
CORS(app)
socketio = SocketIO(app)

# import routes
import routes.index