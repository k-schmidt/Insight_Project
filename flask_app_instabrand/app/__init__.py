"""
Insight Data Engineering
Kyle Schmidt

Global variables for the Flask application
"""
import os

from cassandra.cluster import Cluster  # pylint: disable=no-name-in-module
from flask import Flask

from app.config_secure import CASSANDRA_CLUSTER

app = Flask(__name__)   # pylint: disable=invalid-name
session = Cluster(CASSANDRA_CLUSTER).connect("instabrand")  # pylint: disable=invalid-name
UPLOAD_FOLDER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "instance")
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

from app import views  # pylint: disable=unused-import, wrong-import-position
