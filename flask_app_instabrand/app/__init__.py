from cassandra.cluster import Cluster
from flask import Flask

from app.config_secure import CASSANDRA_CLUSTER

app = Flask(__name__)   # create our flask app
session = Cluster(CASSANDRA_CLUSTER).connect("instabrand")

from app import views
