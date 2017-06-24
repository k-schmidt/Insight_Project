from datetime import datetime
import json
import os

import boto3
from flask import jsonify, render_template, url_for, request, redirect
from kafka import KafkaProducer
from kafka.client import SimpleClient
from kafka.errors import KafkaError
from kafka.producer import KeyedProducer
from kafka import KafkaConsumer
import pymysql
import simplejson as sj
from sqlalchemy import create_engine

import app
from app import helper_methods
from app.config_secure import SERVERS, MYSQL_CONF


app = Flask(__name__)   # pylint: disable=invalid-name
session = Cluster(CASSANDRA_CLUSTER).connect("instabrand")  # pylint: disable=invalid-name
UPLOAD_FOLDER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "instance")
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
engine = create_engine(MYSQL_CONF)


@app.app.route("/")
def dashboard():
    brand_result = helper_methods.get_top_brands(engine)
    influencer_result = helper_methods.get_top_influencers(engine)
    brand_metrics_result = helper_methods.get_brand_metrics(engine)

    return render_template("main_dashboard.html",
                           tags=brand_result,
                           people=influencer_result,
                           reach=brand_metrics_result)


@app.app.route('/newsfeed/<username>')
def user_photos(username):
    timeline = list(helper_methods.get_user_timeline(username, app.session))

    templateData = {
        'media' : timeline,
        "username": username
    }

    return render_template('newsfeed.html', **templateData)


@app.app.errorhandler(404)
def page_not_found(error):
    return render_template('404.html'), 404


@app.app.route("/handle-post/<username>", methods=["POST"])
def user_upload(username):
    username = request.form["username"]
    tags = request.form["tags"].split(",")
    upload_file = request.files["filename"]
    date_today = datetime.now().strftime("%Y-%m-%d %H%M%S.%f")

    print(username, tags, upload_file)

    local_file_path = os.path.join(app.app.config["UPLOAD_FOLDER"], upload_file.filename)
    upload_file.save(local_file_path)

    s3_bucket = "instabrand-assets"
    s3_name = f"{username}/{date_today}"

    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(local_file_path,
                               s3_bucket,
                               s3_name)
    url = boto3.client('s3').generate_presigned_url(
        ClientMethod='get_object',
        Params={
            "Bucket": s3_bucket,
            "Key": s3_name
        }
    )
    object_acl = s3.ObjectAcl(s3_bucket, s3_name)
    response = object_acl.put(ACL='public-read')

    kafka_message = {
        "username": username,
        "tags": tags,
        "photo_link": url,
        "created_time": date_today,
        "latitude": "40.7128",
        "longitude": "74.0059",
        "event": "photo-upload"
    }

    producer = KafkaProducer(bootstrap_servers=SERVERS,
                             value_serializer=lambda m: json.dumps(m).encode('utf-8'))
    producer.send("photo-upload",
                  key=bytes(username, "utf-8"),
                  value=kafka_message)
    return redirect(url_for('user_photos', username=username))


@app.app.route("/brand-metrics", methods=["GET"])
def brand_metrics():
    column_names = ["Tag", "Reach", "Comment Frequency",
                    "Like Frequency", "Comment Rate", "Like Rate",
                    "Engagement Rate"]
    result = [dict(zip(column_names, row)) for row in helper_methods.get_brand_metrics(engine)]
    return sj.dumps(result, use_decimal=True)


@app.app.route("/consume-photos", methods=["GET"])
def consume_photos():
    consumer = KafkaConsumer("photo-upload",
                             group_id="consumer-group",
                             bootstrap_servers=SERVERS,
                             value_deserializer=lambda m: json.loads(m.decode('ascii')))
    for message in consumer:
        consumer.commit()
        return json.dumps(message.value)
