"""
Insight Data Engineering
Kyle Schmidt

Flask routes to serve data and render web pages
"""
import json
import os
from typing import Tuple

from cassandra.cluster import Cluster  # pylint: disable=no-name-in-module
from flask import render_template, Flask  # type: ignore
from kafka import KafkaConsumer  # type: ignore
import pymysql  # pylint: disable=unused-import
import simplejson as sj  # type: ignore
from sqlalchemy import create_engine  # type: ignore

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
def dashboard() -> str:
    """
    Main endpoint to serve the home page.

    Web page loads the top brands, top influencers, and metrics about each brand's engagement

    Returns:
        Rendered html template
    """
    brand_result = helper_methods.get_top_brands(engine)
    influencer_result = helper_methods.get_top_influencers(engine)
    brand_metrics_result = helper_methods.get_brand_metrics(engine)

    return render_template("main_dashboard.html",
                           tags=brand_result,
                           people=influencer_result,
                           reach=brand_metrics_result)


@app.route("/map")
def render_map() -> str:
    """
    Serves a google map that updates in real-time using JQuery.

    Returns:
        Rendered html template
    """
    return render_template("map.html")


@app.route('/<username>')
def user_photos(username: str) -> str:
    """
    Serves a user's timeline, populating it with their and their follower's recent posts

    Arguments:
        username: Username to retrieve timeline for

    Returns:
        Rendered html template
    """
    timeline = list(helper_methods.get_user_timeline(username, session))
    template_data = {
        'media' : timeline,
        "username": username
    }

    return render_template('newsfeed.html', **template_data)


@app.errorhandler(404)
def page_not_found(error) -> Tuple[str, int]:  # pylint: disable=unused-argument
    """
    Endpoint that is rendered when an endpoint is invalid

    Returns:
        Rendered html template
    """
    return render_template('404.html'), 404


@app.route("/brand-metrics", methods=["GET"])
def brand_metrics() -> str:
    """
    Endpoint to provide an API to each brand's high-level metrics

    Returns:
        JSON data of all brands' metrics
    """
    column_names = ["Tag", "Reach", "Comment Frequency",
                    "Like Frequency", "Comment Rate", "Like Rate",
                    "Engagement Rate"]
    result = [dict(zip(column_names, row)) for row in helper_methods.get_brand_metrics(engine)]
    return sj.dumps(result, use_decimal=True)


@app.route("/consume-photos", methods=["GET"])
def consume_photos() -> str:
    """
    API listener to the Kafka photo-upload topic.

    Endpoint is used to append markers to the /map endpoint on Google Maps

    Returns:
        JSON of a photo-upload event
    """
    message = next(consumer)
    consumer.commit()
    return json.dumps(message.value)
