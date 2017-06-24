from datetime import datetime
import json
import os

import boto3
from flask import render_template, url_for, request, redirect
from kafka import KafkaProducer
from kafka.client import SimpleClient
from kafka.errors import KafkaError
from kafka.producer import KeyedProducer
import pymysql
from sqlalchemy import create_engine

import app
from app import helper_methods
from app.config_secure import SERVERS, MYSQL_CONF


engine = create_engine(MYSQL_CONF)


@app.app.route("/")
def dashboard():
    sql_string = "SELECT tags, cnt from top_brands where tags is not NULL order by cnt desc, tags limit 10"
    brand_result = engine.execute(sql_string).fetchall()

    influencer_string = "SELECT username, frequency from recent_influencers order by frequency desc, username limit 10"
    influencer_result = engine.execute(influencer_string).fetchall()

    return render_template("main_dashboard.html", tags=brand_result, people=influencer_result)


@app.app.route('/newsfeed/<username>')
def user_photos(username):
    timeline = list(helper_methods.get_user_timeline(username, app.session))

    templateData = {
        'size' : "big",  # request.args.get('size','thumb'),
        'media' : timeline,  # recent_media
        "username": username
    }
    print(timeline)

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


# This is a jinja custom filter
@app.app.template_filter('strftime')
def _jinja2_filter_datetime(date, fmt=None):
    pyDate = time.strptime(date,'%a %b %d %H:%M:%S +0000 %Y') # convert instagram date string into python date/time
    return time.strftime('%Y-%m-%d %h:%M:%S', pyDate) # return the formatted date.
