"""
Insight Data Engineering
Kyle Schmidt

Like Kafka Producer
"""
from datetime import datetime
import json
import random
import time

from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
from sqlalchemy.sql.expression import func


def get_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def query_for_user(Session):
    session = Session()
    sql_string = "SELECT * from users order by rand();"
    user = session.execute(sql_string).first()
    session.close()
    return user


def query_follower(user, Session):
    session = Session()
    sql_string = "SELECT * from users inner join followers on users.id = followers.followee where users.id = {} order by rand()".format(user.id)
    user = session.execute(sql_string).first()
    session.close()
    return user


def query_photos(user, Session):
    session = Session()
    sql_string = "SELECT * from photos where user_id = {} order by rand()".format(user.id)
    photo = session.execute(sql_string).first()
    session.close()
    return photo


def like_producer(servers, Session):
    simple_client = SimpleClient(servers)
    producer = KeyedProducer(simple_client)
    user = query_for_user(Session)
    photo = query_photos(user, Session)
    follower = query_follower(user, Session)
    if not follower: return
    record = {
        "user": {
            "id": follower.id,
            "full_name": follower.id,
            "username": follower.username,
            "last_login": follower.last_login,
            "created_time": follower.created_time,
            "updated_time": follower.updated_time
        },
        "photo": {
            "id": photo.id,
            "tags": [{"id": tag.id, "tag": tag.tag} for tag in photo.tags],
            "link": photo.link,
            "created_time": photo.created_time,
            "updated_time": photo.updated_time,
            "location": photo.location
        },
        "created_time": get_datetime(),
        "updated_time": get_datetime()
    }
    if not photo or record: return
    producer.send_messages("comment",
                           bytes(follower.username, 'utf-8'),
                           json.dumps(record).encode('ascii'))
    return record
