from datetime import datetime

import boto3
from flask import render_template, url_for, request
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer

import app
from app import helper_methods


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
    upload_file = request.form["filename"]
    date_today = datetime.today().strftime("%Y-%m-%d %H%M%S")

    s3_bucket = "instabrand-assets"
    s3_name = f"{username}/{date_today}"

    boto3.resource('s3').meta.client.upload_file(upload_file,
                                                 s3_bucket,
                                                 s3_name)
    url = boto3.client('s3').generate_presigned_url(
        ClientMethod='get_object',
        Params={
            "Bucket": s3_bucket,
            "Key": s3_name
        }
    )
    print(url)

    kafka_message = {
        "username": username,
        "tags": tags,
        "photo_link": url,
        "created_time": date_today,
        "latitude": "40.7128",
        "longitude": "74.0059",
        "event": "photo-upload"
    }

    simple_client = SimpleClient(servers)
    producer = KeyedProducer(simple_client)
    producer.send_messages("photo-upload",
                           bytes(username, "utf-8"),
                           json.dumps(kafka_message).encode("utf-8"))
    return redirect(url_for('user_photos', username=username))


# This is a jinja custom filter
@app.app.template_filter('strftime')
def _jinja2_filter_datetime(date, fmt=None):
    pyDate = time.strptime(date,'%a %b %d %H:%M:%S +0000 %Y') # convert instagram date string into python date/time
    return time.strftime('%Y-%m-%d %h:%M:%S', pyDate) # return the formatted date.
