import os

from cassandra.cluster import Cluster
from flask import Flask

from app.config_secure import CASSANDRA_CLUSTER

app = Flask(__name__)   # create our flask app
session = Cluster(CASSANDRA_CLUSTER).connect("instabrand")
UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "instance")
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

from app import views
