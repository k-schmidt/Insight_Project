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

import app
from app import helper_methods
from app.config_secure import SERVERS, MYSQL_CONF


@app.app.route('/<username>')
def user_photos(username):
    timeline = list(helper_methods.get_user_timeline(username, app.session))

    templateData = {
        'size' : "big",  # request.args.get('size','thumb'),
        'media' : timeline,  # recent_media
        "username": username
    }

    return render_template('base.html', **templateData)


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


@app.app.route("/top-brands")
def top_brands():
    sql_string = "SELECT tags from top_brands order by count(1), tags"
    mysql_session = pymysql.connect(MYSQL_CONF)
    with mysql_session.cursor() as cursor:
        cursor.execute(sql_string)
        result = cursor.fetchall()
    print(result)

# This is a jinja custom filter
@app.app.template_filter('strftime')
def _jinja2_filter_datetime(date, fmt=None):
    pyDate = time.strptime(date,'%a %b %d %H:%M:%S +0000 %Y') # convert instagram date string into python date/time
    return time.strftime('%Y-%m-%d %h:%M:%S', pyDate) # return the formatted date.
