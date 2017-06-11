"""
Insight Data Engineering
Kyle Schmidt

Comment Kafka Producer
"""
from datetime import datetime
import json
import random
import string
import time

from faker import Factory
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
from sqlalchemy.sql.expression import func


def get_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_text():
    fake = Factory.create()
    return fake.sentence()


def query_for_user(mysql_session):
    sql_string = "SELECT * from users order by rand() limit 1;"
    user = list(mysql_session.execute(sql_string))
    return user[0] if user else None


def query_follower(user, cassandra_session):
    cql_string = "SELECT * from user_inbound_follows where followed_username = '{}';".format(user.username)
    result = list(cassandra_session.execute(cql_string))
    follower = random.choice(result) if result else None
    return follower


def query_photos(user, cassandra_session):
    cql_string = "SELECT photo_id from user_status_updates where username = '{}';"\
        .format(user.username)
    result = list(cassandra_session.execute(cql_string))
    photo = random.choice(result) if result else None
    return photo
                          

def comment_producer(servers, mysql_session, cassandra_session):
    simple_client = SimpleClient(servers)
    producer = KeyedProducer(simple_client)
    user = query_for_user(mysql_session)
    photo = query_photos(user, cassandra_session)
    commenter = query_follower(user, cassandra_session)
    text = get_text()
    created_time = get_datetime()

    if not all([photo, commenter, user]): return
    record = {
        "follower_username": follower.username,
        "followed_username": user.username,
        "photo_id": photo.photo_id,
        "text": text,
        "created_time": created_time,
    }
    producer.send_messages("comment",
                           bytes(commenter.username, 'utf-8'),
                           json.dumps(record).encode('utf-8'))
    return record
