import json
import os

from cassandra.cluster import Cluster  # pylint: disable=no-name-in-module
from flask import render_template, Flask
from kafka import KafkaConsumer
import pymysql  # pylint: disable=unused-import
import simplejson as sj
from sqlalchemy import create_engine

import helper_methods
from config_secure import SERVERS, MYSQL_CONF, CASSANDRA_CLUSTER


app = Flask(__name__)   # pylint: disable=invalid-name
session = Cluster(CASSANDRA_CLUSTER).connect("instabrand")  # pylint: disable=invalid-name
UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "instance")
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
engine = create_engine(MYSQL_CONF)  # pylint: disable=invalid-name
print("initiating kafka")
consumer = KafkaConsumer("photo-upload",  # pylint: disable=invalid-name
                         group_id="consumer-group",
                         bootstrap_servers=SERVERS,
                         value_deserializer=lambda m: json.loads(m.decode('ascii')))
print("kafka initiated")


@app.route("/")
def dashboard():
    brand_result = helper_methods.get_top_brands(engine)
    influencer_result = helper_methods.get_top_influencers(engine)
    brand_metrics_result = helper_methods.get_brand_metrics(engine)

    return render_template("main_dashboard.html",
                           tags=brand_result,
                           people=influencer_result,
                           reach=brand_metrics_result)


@app.route("/map")
def render_map():
    return render_template("map.html")


@app.route('/<username>')
def user_photos(username):
    timeline = list(helper_methods.get_user_timeline(username, session))

    print(timeline)
    templateData = {
        'media' : timeline,
        "username": username
    }

    return render_template('newsfeed.html', **templateData)


@app.errorhandler(404)
def page_not_found(error):
    return render_template('404.html'), 404


@app.route("/brand-metrics", methods=["GET"])
def brand_metrics():
    column_names = ["Tag", "Reach", "Comment Frequency",
                    "Like Frequency", "Comment Rate", "Like Rate",
                    "Engagement Rate"]
    result = [dict(zip(column_names, row)) for row in helper_methods.get_brand_metrics(engine)]
    return sj.dumps(result, use_decimal=True)


@app.route("/consume-photos", methods=["GET"])
def consume_photos():
    for message in consumer:
        consumer.commit()
        return json.dumps(message.value)
