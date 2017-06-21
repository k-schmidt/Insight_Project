"""
Insight Data Engineering
Kyle Schmidt

Comment Kafka Producer
"""
from datetime import datetime
import json
import random
import string

from faker import Factory


def get_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_text():
    fake = Factory.create()
    return fake.sentence()


def query_photos(user, cassandra_session):
    cql_string = "SELECT photo_id from user_status_updates where username = '{}';"\
        .format(user)
    result = list(cassandra_session.execute(cql_string))
    photo = random.choice(result) if result else None
    return photo


def comment_producer(servers, users, photos, tags, locations, producer):
    if len(photos) == 0:
        return
    follower = random.choice(users)[0]
    photo, followee = random.choice(photos)
    text = get_text()
    created_time = get_datetime()

    if not all([photo, follower, followee]): return
    record = {
        "follower_username": follower,
        "followed_username": followee,
        "photo_id": photo,
        "text": text,
        "created_time": created_time,
        "event": "comment"
    }
    producer.send_messages("comment",
                           bytes(follower, 'utf-8'),
                           json.dumps(record).encode('utf-8'))
    return record
