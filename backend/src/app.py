from pathlib import Path
import sys

from flask import Flask
from controller.pipeline import pipeline
from flask_cors import CORS


proj_folder = Path(__file__).parent
sys.path.insert(0, proj_folder/'node_mapper/')

app = Flask(__name__)
CORS(app)

app.register_blueprint(pipeline)
